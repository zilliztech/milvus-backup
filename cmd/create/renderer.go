package create

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// renderBackup draws one aggregate bar plus one bar per in-flight collection
// for the backup job registered under taskID, reconciles them against store
// snapshots until the job settles, then answers with the terminal status.
// Every update written to a bar is an absolute value from one snapshot, so a
// dropped or doubled tick cannot skew it. A job that settles before the first
// poll gets no bars at all, and renderBackup answers immediately.
//
// maxCollBars bounds how many collection bars are on screen at once; the
// caller passes the configured collection concurrency, which is what bounds
// the number of collections actually in flight.
func renderBackup(ctx context.Context, p *mpb.Progress, store *jobstate.Store, taskID string, interval time.Duration, maxCollBars int) (jobstate.BackupStatus, error) {
	var latest atomic.Value // jobstate.BackupStatus, read by the decorators

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
		status, err := store.GetBackupTask(taskID)
		if err != nil {
			abortAll()
			return jobstate.BackupStatus{}, fmt.Errorf("jobview: read backup task %s: %w", taskID, err)
		}
		latest.Store(status)

		if status.Terminal() && total == nil {
			return status, nil
		}
		if total == nil {
			total = newBackupTotalBar(p, &latest)
		}
		reconcileBackupCollBars(p, &latest, collBars, status, maxCollBars)
		reconcileBackupTotalBar(total, status)
		if status.Terminal() {
			settleBackupBars(total, collBars, status)
			return status, nil
		}

		select {
		case <-ctx.Done():
			abortAll()
			return jobstate.BackupStatus{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

// collStage is the renderer's own per-collection stage, reduced from the
// snapshot's two independent halves (schema and data race each other in some
// plans) into the one thing the collection is doing right now. The data half
// wins when both are moving: it is where the wall-clock time goes.
type collStage int

const (
	collStageDormant collStage = iota
	collStageSchema
	collStageFlushing
	collStageListing
	collStageCopying
	collStageSettled
	collStageFailed
)

// backupCollStageOf reduces one collection's snapshot to its stage. A
// collection is settled when its schema half succeeded and its data half
// either succeeded or never existed — meta_only jobs register no data half.
func backupCollStageOf(coll jobstate.BackupCollStatus) collStage {
	ddlOK := coll.DDL != nil && coll.DDL.State == jobstate.BackupCollDDLStateSuccess
	dmlOK := coll.DML == nil || coll.DML.State == jobstate.BackupCollDMLStateSuccess

	failed := (coll.DDL != nil && coll.DDL.State == jobstate.BackupCollDDLStateFail) ||
		(coll.DML != nil && coll.DML.State == jobstate.BackupCollDMLStateFail)

	switch {
	case failed:
		return collStageFailed
	case ddlOK && dmlOK:
		return collStageSettled
	case coll.DML != nil && coll.DML.State == jobstate.BackupCollDMLStateExecuting:
		return collStageCopying
	case coll.DML != nil && coll.DML.State == jobstate.BackupCollDMLStatePrepare:
		return collStageListing
	case coll.DML != nil && coll.DML.State == jobstate.BackupCollDMLStateFlushing:
		return collStageFlushing
	case coll.DDL != nil && coll.DDL.State == jobstate.BackupCollDDLStateExecuting:
		return collStageSchema
	default:
		return collStageDormant
	}
}

// backupCollActive reports whether a collection stage is one a bar is drawn
// for. Dormant collections are queued behind a semaphore and the settled
// ones are handled by removal, so a bar on screen always means "this
// collection is being worked on right now".
func backupCollActive(stage collStage) bool {
	switch stage {
	case collStageSchema, collStageFlushing, collStageListing, collStageCopying:
		return true
	default:
		return false
	}
}

// newBackupTotalBar builds the aggregate bar. The stage label and the
// collection counters are dynamic decorators reading the renderer's latest
// snapshot.
func newBackupTotalBar(p *mpb.Progress, latest *atomic.Value) *mpb.Bar {
	snapshot := func() jobstate.BackupStatus {
		return latest.Load().(jobstate.BackupStatus)
	}

	return p.MustAdd(0,
		backupTotalFiller(snapshot),
		mpb.PrependDecorators(
			decor.Any(func(decor.Statistics) string { return backupStage(snapshot()) }),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string { return backupTotalCounters(snapshot()) }, decor.WC{C: decor.DextraSpace}),
		),
		mpb.BarRemoveOnComplete(),
	)
}

// backupTotalFiller draws no bar graphic before the collection phase: there
// is no denominator during the database meta phase, so the stage label is
// plain text until collections register, git-gc style.
func backupTotalFiller(latest func() jobstate.BackupStatus) mpb.BarFiller {
	bar := mpb.BarStyle().Build()

	return mpb.BarFillerFunc(func(w io.Writer, stat decor.Statistics) error {
		if latest().State != jobstate.BackupStateCollectionExecuting {
			return nil
		}

		return bar.Fill(w, stat)
	})
}

// reconcileBackupTotalBar aligns the aggregate bar with one snapshot: the
// total is the collections the job covers, the current how many have
// settled.
func reconcileBackupTotalBar(bar *mpb.Bar, status jobstate.BackupStatus) {
	var done int64
	for _, coll := range status.Colls {
		if backupCollStageOf(coll) == collStageSettled {
			done++
		}
	}

	bar.SetTotal(int64(len(status.Colls)), false)
	bar.SetCurrent(done)
}

// reconcileBackupCollBars aligns the per-collection bars with one snapshot:
// bars exist exactly for the active collections, capped at maxBars, in
// sorted name order so the shown set is stable across ticks. A bar whose
// collection went dormant completes away; a failed one aborts.
func reconcileBackupCollBars(p *mpb.Progress, latest *atomic.Value, bars map[collref.Name]*mpb.Bar, status jobstate.BackupStatus, maxBars int) {
	active := make([]collref.Name, 0, len(status.Colls))
	for name, coll := range status.Colls {
		if backupCollActive(backupCollStageOf(coll)) {
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
			bar = newBackupCollBar(p, name, latest)
			bars[name] = bar
		}
		if coll.DML != nil {
			bar.SetTotal(coll.DML.TotalSize, false)
			bar.SetCurrent(min(coll.DML.CopiedSize, coll.DML.TotalSize))
		}
	}

	for name, bar := range bars {
		coll, ok := status.Colls[name]
		switch {
		case !ok:
			bar.Abort(true)
			delete(bars, name)
		case backupCollStageOf(coll) == collStageFailed:
			bar.Abort(true)
			delete(bars, name)
		default:
			if _, isShown := shown[name]; isShown {
				continue
			}
			// Dormant or settled: complete the bar at 100% and let
			// BarRemoveOnComplete take it off the screen.
			bar.SetTotal(-1, true)
			delete(bars, name)
		}
	}
}

// settleBackupBars lands every bar on the outcome: success completes the
// remaining bars at 100%, failure drops them so the error line that follows
// is what the user reads.
func settleBackupBars(total *mpb.Bar, bars map[collref.Name]*mpb.Bar, status jobstate.BackupStatus) {
	if status.State == jobstate.BackupStateFail {
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

// newBackupCollBar builds one collection's bar. The stage label and the copy
// stats are dynamic decorators reading the renderer's latest snapshot, so
// the line flips from listing to copying on the frame after the total lands.
func newBackupCollBar(p *mpb.Progress, name collref.Name, latest *atomic.Value) *mpb.Bar {
	snapshot := func() jobstate.BackupStatus {
		return latest.Load().(jobstate.BackupStatus)
	}

	return p.MustAdd(0,
		backupCollFiller(name, snapshot),
		mpb.PrependDecorators(
			decor.Name(fitName(name.String(), 20)),
			decor.Any(func(decor.Statistics) string {
				return backupCollStageLabel(backupCollStageOf(backupCollOf(snapshot(), name)))
			}),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string {
				return backupCollStatsLine(snapshot(), name, time.Now())
			}, decor.WC{C: decor.DextraSpace}),
		),
		mpb.BarRemoveOnComplete(),
	)
}

// backupCollFiller draws no bar graphic until the collection's copy phase
// opens: before that there is no denominator, same rule as the aggregate
// bar.
func backupCollFiller(name collref.Name, latest func() jobstate.BackupStatus) mpb.BarFiller {
	bar := mpb.BarStyle().Build()

	return mpb.BarFillerFunc(func(w io.Writer, stat decor.Statistics) error {
		if backupCollStageOf(backupCollOf(latest(), name)) != collStageCopying {
			return nil
		}

		return bar.Fill(w, stat)
	})
}

// backupCollOf reads one collection out of a snapshot; a missing collection
// (already pruned from a bar's snapshot) reads as dormant.
func backupCollOf(status jobstate.BackupStatus, name collref.Name) jobstate.BackupCollStatus {
	if coll, ok := status.Colls[name]; ok {
		return coll
	}

	return jobstate.BackupCollStatus{}
}

// backupStage is the aggregate bar's stage label, padded to a fixed width so
// the bar does not jump when the stage flips.
func backupStage(status jobstate.BackupStatus) string {
	switch status.State {
	case jobstate.BackupStateInitial:
		return pad("starting", 14)
	case jobstate.BackupStateDatabaseExecuting:
		return pad("database meta", 14)
	case jobstate.BackupStateCollectionExecuting:
		return pad(backupCollPhase(status), 14)
	case jobstate.BackupStateSuccess:
		return pad("done", 14)
	case jobstate.BackupStateFail:
		return pad("failed", 14)
	default:
		return pad("", 14)
	}
}

// backupCollPhase summarizes the collection phase as the furthest stage any
// collection is on: one copying collection makes the job "copying" even
// while others still flush. Once every collection has settled, what is left
// is the meta write, which the tracker does not model — the renderer calls
// it finalizing.
func backupCollPhase(status jobstate.BackupStatus) string {
	if len(status.Colls) == 0 {
		return "collections"
	}

	rank := 0
	settled := 0
	for _, coll := range status.Colls {
		stage := backupCollStageOf(coll)
		if stage == collStageSettled || stage == collStageFailed {
			settled++
			continue
		}
		rank = max(rank, int(stage))
	}
	if settled == len(status.Colls) {
		return "finalizing"
	}

	switch collStage(rank) {
	case collStageCopying:
		return "copying"
	case collStageListing:
		return "preparing"
	case collStageFlushing:
		return "flushing"
	case collStageSchema:
		return "schema"
	default:
		return "collections"
	}
}

// backupTotalCounters renders the aggregate bar's collection counts. Before
// collections register there is nothing to count.
func backupTotalCounters(status jobstate.BackupStatus) string {
	if len(status.Colls) == 0 {
		return ""
	}

	var done int
	for _, coll := range status.Colls {
		if backupCollStageOf(coll) == collStageSettled {
			done++
		}
	}

	return fmt.Sprintf("%d / %d", done, len(status.Colls))
}

// backupCollStageLabel is the collection bar's stage label, padded to a
// fixed width like the aggregate one. The labels stay short on purpose: at
// the container's 80 columns the name, stage, and copy stats share the line
// with the bar graphic, and a longer word trims the ETA off the end.
func backupCollStageLabel(stage collStage) string {
	var label string
	switch stage {
	case collStageSchema:
		label = "schema"
	case collStageFlushing:
		label = "flushing"
	case collStageListing:
		label = "listing"
	case collStageCopying:
		label = "copying"
	}

	return pad(label, 9)
}

// backupCollStatsLine renders the right side of a collection bar as one
// decorator: separate Any decorators with DextraSpace do not reliably space
// apart (the extra space is a width hint, not a literal pad), so the line is
// joined here instead. Outside the copy phase there are no stats to show.
func backupCollStatsLine(status jobstate.BackupStatus, name collref.Name, now time.Time) string {
	coll := backupCollOf(status, name)
	if coll.DML == nil || coll.DML.State != jobstate.BackupCollDMLStateExecuting {
		return ""
	}

	dml := coll.DML
	parts := []string{backupCollCounters(status.Unit, dml)}
	if v := backupCollSpeed(status.Unit, dml, now); v != "" {
		parts = append(parts, v)
	}
	if v := backupCollETA(dml, now); v != "" {
		parts = append(parts, v)
	}

	return strings.Join(parts, " ")
}

// backupCollCounters renders the copy counts in the job's unit. The snapshot
// format's total grows as the export job discovers files, so file counts
// stay marked approximate for the whole copy; the binlog total locks in
// before the copy starts and reads exact.
func backupCollCounters(unit jobstate.BackupUnit, dml *jobstate.BackupCollDMLStatus) string {
	copied := min(dml.CopiedSize, dml.TotalSize)
	if unit == jobstate.BackupUnitFiles {
		return fmt.Sprintf("%d / ~%d files", copied, dml.TotalSize)
	}

	return fmt.Sprintf("%s / %s", sizeB1024(copied), sizeB1024(dml.TotalSize))
}

// backupCollSpeed renders the average copy rate over the phase's lifetime.
// It stays blank for the first second, where the average is all noise.
func backupCollSpeed(unit jobstate.BackupUnit, dml *jobstate.BackupCollDMLStatus, now time.Time) string {
	rate := backupCollRate(dml, now)
	if rate < 1 {
		return ""
	}

	if unit == jobstate.BackupUnitFiles {
		return fmt.Sprintf("%.0f files/s", rate)
	}

	return sizeB1024(int64(rate)) + "/s"
}

// backupCollETA renders the time remaining from the average rate. Once the
// copied count passes the total there is no estimate left to give.
func backupCollETA(dml *jobstate.BackupCollDMLStatus, now time.Time) string {
	remaining := dml.TotalSize - dml.CopiedSize
	rate := backupCollRate(dml, now)
	if remaining <= 0 || rate < 1 {
		return ""
	}

	eta := time.Duration(float64(remaining) / rate * float64(time.Second)).Truncate(time.Second)

	return "ETA " + eta.String()
}

// backupCollRate is the average copies per second since the collection's
// copy phase started, from one snapshot and one clock read.
func backupCollRate(dml *jobstate.BackupCollDMLStatus, now time.Time) float64 {
	elapsed := now.Sub(dml.StartTime)
	if dml.CopiedSize <= 0 || elapsed <= 0 {
		return 0
	}

	return float64(dml.CopiedSize) / elapsed.Seconds()
}

// sizeB1024 renders a byte count with a binary unit, e.g. "1.50 GiB".
func sizeB1024(n int64) string {
	return fmt.Sprintf("% .2f", decor.SizeB1024(max(n, 0)))
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
