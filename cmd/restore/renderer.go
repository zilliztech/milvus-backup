package restore

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// renderRestore draws one aggregate bar plus one bar per in-flight collection
// for the restore job registered under taskID, reconciles them against store
// snapshots until the job settles, then answers with the terminal status.
// Every update written to a bar is an absolute value from one snapshot, so a
// dropped or doubled tick cannot skew it. A job that settles before the first
// poll gets no bars at all, and renderRestore answers immediately.
//
// maxCollBars bounds how many collection bars are on screen at once; the
// caller passes the configured collection concurrency, which is what bounds
// the number of collections actually in flight.
func renderRestore(ctx context.Context, p *mpb.Progress, store *jobstate.Store, taskID string, interval time.Duration, maxCollBars int) (jobstate.RestoreStatus, error) {
	var latest atomic.Value // jobstate.RestoreStatus, read by the decorators

	var total *mpb.Bar
	collBars := make(map[collref.Name]*mpb.Bar)
	abortAll := func() {
		if total != nil {
			total.Abort(true)
		}
		for _, bar := range collBars {
			bar.Abort(true)
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		status, err := store.GetRestoreTask(taskID)
		if err != nil {
			abortAll()
			return jobstate.RestoreStatus{}, fmt.Errorf("jobview: read restore task %s: %w", taskID, err)
		}
		latest.Store(status)

		if status.Terminal() && total == nil {
			return status, nil
		}
		if total == nil {
			total = newRestoreTotalBar(p, &latest)
		}
		reconcileRestoreCollBars(p, &latest, collBars, status, maxCollBars)
		reconcileRestoreTotalBar(total, status)
		if status.Terminal() {
			settleRestoreBars(total, collBars, status)
			return status, nil
		}

		select {
		case <-ctx.Done():
			abortAll()
			return jobstate.RestoreStatus{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

// collStage is the renderer's own per-collection stage, reduced from the
// snapshot's halves into the one thing the collection is doing right now. The
// data half wins when both are moving: it is where the wall-clock time goes.
type collStage int

const (
	collStageDormant collStage = iota
	collStageSchema
	collStageData
	collStageSettled
	collStageFailed
)

// restoreCollStageOf reduces one collection's snapshot to its stage. A
// collection is settled when its schema half succeeded (or never existed —
// the snapshot plan runs no DDL) and every one of its job halves completed.
func restoreCollStageOf(coll jobstate.RestoreCollStatus) collStage {
	for _, job := range coll.Jobs {
		if job.State == jobstate.RestoreJobFailed {
			return collStageFailed
		}
	}
	if coll.DDL != nil && coll.DDL.State == jobstate.RestoreDDLFail {
		return collStageFailed
	}

	ddlOK := coll.DDL == nil || coll.DDL.State == jobstate.RestoreDDLSuccess
	jobsSettled := true
	dataRunning := false
	for _, job := range coll.Jobs {
		if job.State != jobstate.RestoreJobCompleted {
			jobsSettled = false
		}
		if job.State == jobstate.RestoreJobRunning {
			dataRunning = true
		}
	}

	switch {
	case ddlOK && jobsSettled:
		return collStageSettled
	case dataRunning:
		return collStageData
	case coll.DDL != nil && coll.DDL.State == jobstate.RestoreDDLExecuting:
		return collStageSchema
	default:
		// Schema succeeded and the jobs are queued behind the import
		// concurrency limit, or nothing has started yet.
		return collStageDormant
	}
}

// restoreCollActive reports whether a collection stage is one a bar is drawn
// for. Dormant collections are queued behind a semaphore and the settled ones
// are handled by removal, so a bar on screen always means "this collection is
// being worked on right now".
func restoreCollActive(stage collStage) bool {
	switch stage {
	case collStageSchema, collStageData:
		return true
	default:
		return false
	}
}

// newRestoreTotalBar builds the aggregate bar. The stage label and the
// collection counters are dynamic decorators reading the renderer's latest
// snapshot.
func newRestoreTotalBar(p *mpb.Progress, latest *atomic.Value) *mpb.Bar {
	snapshot := func() jobstate.RestoreStatus {
		return latest.Load().(jobstate.RestoreStatus)
	}

	return lo.Must(p.Add(0,
		restoreTotalFiller(snapshot),
		mpb.PrependDecorators(
			decor.Any(func(decor.Statistics) string { return restoreStage(snapshot()) }),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string { return restoreTotalCounters(snapshot()) }, decor.WC{C: decor.DextraSpace}),
		),
		mpb.BarRemoveOnComplete(),
	))
}

// restoreTotalFiller draws no bar graphic before the plan registers its
// collections: there is no denominator during the prepare phase, so the stage
// label is plain text until collections register, git-gc style.
func restoreTotalFiller(latest func() jobstate.RestoreStatus) mpb.BarFiller {
	bar := mpb.BarStyle().Build()

	return mpb.BarFillerFunc(func(w io.Writer, stat decor.Statistics) error {
		if len(latest().Colls) == 0 {
			return nil
		}

		return bar.Fill(w, stat)
	})
}

// reconcileRestoreTotalBar aligns the aggregate bar with one snapshot: the
// total is the collections the job covers, the current how many have settled.
func reconcileRestoreTotalBar(bar *mpb.Bar, status jobstate.RestoreStatus) {
	var done int64
	for _, coll := range status.Colls {
		if restoreCollStageOf(coll) == collStageSettled {
			done++
		}
	}

	bar.SetTotal(int64(len(status.Colls)), false)
	bar.SetCurrent(done)
}

// reconcileRestoreCollBars aligns the per-collection bars with one snapshot:
// bars exist exactly for the active collections, capped at maxBars, in sorted
// name order so the shown set is stable across ticks. A bar whose collection
// went dormant completes away; a failed one aborts.
func reconcileRestoreCollBars(p *mpb.Progress, latest *atomic.Value, bars map[collref.Name]*mpb.Bar, status jobstate.RestoreStatus, maxBars int) {
	active := make([]collref.Name, 0, len(status.Colls))
	for name, coll := range status.Colls {
		if restoreCollActive(restoreCollStageOf(coll)) {
			active = append(active, name)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].String() < active[j].String() })
	if len(active) > maxBars {
		active = active[:maxBars]
	}

	shown := make(map[collref.Name]struct{}, len(active))
	for _, name := range active {
		shown[name] = struct{}{}
		coll := status.Colls[name]
		bar, ok := bars[name]
		if !ok {
			bar = newRestoreCollBar(p, name, latest)
			bars[name] = bar
		}
		if restoreCollStageOf(coll) == collStageData && coll.TotalSize > 0 {
			bar.SetTotal(100, false)
			bar.SetCurrent(int64(restoreCollPercent(coll)))
		}
	}

	for name, bar := range bars {
		coll, ok := status.Colls[name]
		switch {
		case !ok:
			bar.Abort(true)
			delete(bars, name)
		case restoreCollStageOf(coll) == collStageFailed:
			bar.Abort(true)
			delete(bars, name)
		default:
			if _, isShown := shown[name]; isShown {
				continue
			}
			// Dormant or settled: complete the bar and let
			// BarRemoveOnComplete take it off the screen.
			bar.SetTotal(-1, true)
			delete(bars, name)
		}
	}
}

// settleRestoreBars lands every bar on the outcome: success completes the
// remaining bars at 100%, failure drops them so the error line that follows
// is what the user reads.
func settleRestoreBars(total *mpb.Bar, bars map[collref.Name]*mpb.Bar, status jobstate.RestoreStatus) {
	if status.State == jobstate.RestoreStateFail {
		total.Abort(true)
		for _, bar := range bars {
			bar.Abort(true)
		}
		return
	}

	for _, bar := range bars {
		bar.SetTotal(-1, true)
	}
	total.SetTotal(-1, true)
}

// newRestoreCollBar builds one collection's bar. The stage label and the
// progress stats are dynamic decorators reading the renderer's latest
// snapshot, so the line flips from schema to data on the frame after the
// first job starts.
func newRestoreCollBar(p *mpb.Progress, name collref.Name, latest *atomic.Value) *mpb.Bar {
	snapshot := func() jobstate.RestoreStatus {
		return latest.Load().(jobstate.RestoreStatus)
	}

	return lo.Must(p.Add(0,
		restoreCollFiller(name, snapshot),
		mpb.PrependDecorators(
			decor.Name(fitName(name.String(), 20)),
			decor.Any(func(decor.Statistics) string {
				return restoreCollStageLabel(restoreCollOf(snapshot(), name))
			}),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string {
				return restoreCollStatsLine(restoreCollOf(snapshot(), name))
			}, decor.WC{C: decor.DextraSpace}),
		),
		mpb.BarRemoveOnComplete(),
	))
}

// restoreCollFiller draws no bar graphic until the collection's data phase
// opens: before that there is no denominator, same rule as the aggregate bar.
func restoreCollFiller(name collref.Name, latest func() jobstate.RestoreStatus) mpb.BarFiller {
	bar := mpb.BarStyle().Build()

	return mpb.BarFillerFunc(func(w io.Writer, stat decor.Statistics) error {
		coll := restoreCollOf(latest(), name)
		if restoreCollStageOf(coll) != collStageData || coll.TotalSize <= 0 {
			return nil
		}

		return bar.Fill(w, stat)
	})
}

// restoreCollOf reads one collection out of a snapshot; a missing collection
// (already pruned from a bar's snapshot) reads as dormant.
func restoreCollOf(status jobstate.RestoreStatus, name collref.Name) jobstate.RestoreCollStatus {
	if coll, ok := status.Colls[name]; ok {
		return coll
	}

	return jobstate.RestoreCollStatus{}
}

// restoreStage is the aggregate bar's stage label, padded to a fixed width so
// the bar does not jump when the stage flips.
func restoreStage(status jobstate.RestoreStatus) string {
	switch status.State {
	case jobstate.RestoreStateInitial:
		return pad("starting", 14)
	case jobstate.RestoreStateExecuting:
		return pad(restoreCollPhase(status), 14)
	case jobstate.RestoreStateSuccess:
		return pad("done", 14)
	case jobstate.RestoreStateFail:
		return pad("failed", 14)
	default:
		return pad("", 14)
	}
}

// restoreCollPhase summarizes the executing job as the furthest stage any
// collection is on. Once every collection has settled, what is left is the
// plan's own tail, which the tracker does not model — the renderer calls it
// finalizing. "importing" names the binlog plans' data half, "restoring" the
// snapshot plan's single server-side job.
func restoreCollPhase(status jobstate.RestoreStatus) string {
	if len(status.Colls) == 0 {
		return "preparing"
	}

	binlog := false
	settled := 0
	rank := collStageDormant
	for _, coll := range status.Colls {
		if coll.DDL != nil {
			binlog = true
		}
		stage := restoreCollStageOf(coll)
		if stage == collStageSettled || stage == collStageFailed {
			settled++
			continue
		}
		rank = max(rank, stage)
	}
	if settled == len(status.Colls) {
		return "finalizing"
	}

	switch rank {
	case collStageData:
		if binlog {
			return "importing"
		}
		return "restoring"
	case collStageSchema:
		return "schema"
	default:
		return "preparing"
	}
}

// restoreTotalCounters renders the aggregate bar's collection counts. Before
// collections register there is nothing to count.
func restoreTotalCounters(status jobstate.RestoreStatus) string {
	if len(status.Colls) == 0 {
		return ""
	}

	var done int
	for _, coll := range status.Colls {
		if restoreCollStageOf(coll) == collStageSettled {
			done++
		}
	}

	return fmt.Sprintf("%d / %d", done, len(status.Colls))
}

// restoreCollStageLabel is the collection bar's stage label, padded to a
// fixed width like the aggregate one. A collection with no schema half comes
// from the snapshot plan, whose data half runs entirely inside Milvus.
func restoreCollStageLabel(coll jobstate.RestoreCollStatus) string {
	var label string
	switch restoreCollStageOf(coll) {
	case collStageSchema:
		label = "schema"
	case collStageData:
		if coll.DDL == nil {
			label = "restoring"
		} else {
			label = "importing"
		}
	}

	return pad(label, 9)
}

// restoreCollStatsLine renders the right side of a collection bar: the
// size-weighted percent its job halves have reached, and how many of those
// halves are done. Outside the data phase there is no progress to show.
func restoreCollStatsLine(coll jobstate.RestoreCollStatus) string {
	if restoreCollStageOf(coll) != collStageData || coll.TotalSize <= 0 {
		return ""
	}

	parts := []string{fmt.Sprintf("%d%%", restoreCollPercent(coll))}
	if len(coll.Jobs) > 1 {
		parts = append(parts, fmt.Sprintf("%d / %d jobs", restoreCollSettledJobs(coll), len(coll.Jobs)))
	}

	return strings.Join(parts, " ")
}

// restoreCollPercent renders the collection's percent from its job halves,
// weighted by size: a completed half counts fully, a running one by its
// reported percent, a queued one as zero. The schema half is a handful of
// RPCs with no meaningful size, so it never weighs in. The result is clamped
// so a server reporting past 100 cannot overfill the bar.
func restoreCollPercent(coll jobstate.RestoreCollStatus) int32 {
	if coll.TotalSize <= 0 {
		return 0
	}

	var restoredSize int64
	for _, job := range coll.Jobs {
		progress := int64(job.Progress)
		if job.State == jobstate.RestoreJobCompleted {
			progress = 100
		}
		progress = min(max(progress, 0), 100)
		restoredSize += job.Size * progress / 100
	}

	return int32(min(restoredSize*100/coll.TotalSize, 100))
}

// restoreCollSettledJobs counts the collection's job halves that are done.
func restoreCollSettledJobs(coll jobstate.RestoreCollStatus) int {
	var done int
	for _, job := range coll.Jobs {
		if job.State == jobstate.RestoreJobCompleted {
			done++
		}
	}

	return done
}

// fitName renders a collection name in exactly width runes, truncating the
// tail with an ellipsis, so the stage labels behind it stay column-aligned.
func fitName(name string, width int) string {
	r := []rune(name)
	if len(r) > width {
		return string(r[:width-1]) + "…"
	}

	return pad(name, width)
}

// pad right-pads s with spaces to width runes.
func pad(s string, width int) string {
	if n := width - len([]rune(s)); n > 0 {
		return s + strings.Repeat(" ", n)
	}

	return s
}
