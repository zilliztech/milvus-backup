package del

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// renderDelete draws one progress bar for the delete job registered under
// taskID and reconciles it against store snapshots until the job settles, then
// answers with the terminal status. Every update written to the bar is an
// absolute value from one snapshot, so a dropped or doubled tick cannot skew
// it. A job that settles before the first poll gets no bar at all, and
// renderDelete answers immediately.
func renderDelete(ctx context.Context, p *mpb.Progress, store *jobstate.Store, taskID string, interval time.Duration) (jobstate.DeleteStatus, error) {
	var latest atomic.Value // jobstate.DeleteStatus, read by the decorators

	var bar *mpb.Bar
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		status, err := store.GetDeleteTask(taskID)
		if err != nil {
			if bar != nil {
				bar.Abort(true)
			}
			return jobstate.DeleteStatus{}, fmt.Errorf("jobview: read delete task %s: %w", taskID, err)
		}
		latest.Store(status)

		if status.Terminal() && bar == nil {
			return status, nil
		}
		if bar == nil {
			bar = newDeleteBar(p, status.Name, &latest)
		}
		if status.Terminal() {
			settleDeleteBar(bar, status)
			bar.Wait()
			return status, nil
		}

		reconcileDeleteBar(bar, status)

		select {
		case <-ctx.Done():
			bar.Abort(true)
			return jobstate.DeleteStatus{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

// newDeleteBar builds the single delete bar. The stage label and the counters
// are dynamic decorators reading the renderer's latest snapshot, so the line
// flips from listing to deleting on the frame after ListingDone lands.
func newDeleteBar(p *mpb.Progress, name string, latest *atomic.Value) *mpb.Bar {
	snapshot := func() jobstate.DeleteStatus {
		return latest.Load().(jobstate.DeleteStatus)
	}

	return p.MustAdd(0,
		deleteFiller(snapshot),
		mpb.PrependDecorators(
			decor.Name(name+"  "),
			decor.Any(func(decor.Statistics) string { return deletePhase(snapshot()) }),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string { return deleteCounters(snapshot()) }, decor.WC{C: decor.DextraSpace}),
			decor.Any(func(decor.Statistics) string { return deleteSpeed(snapshot()) }, decor.WC{C: decor.DextraSpace}),
			decor.Any(func(decor.Statistics) string { return deleteETA(snapshot()) }),
		),
		mpb.BarRemoveOnComplete(),
	)
}

// deleteFiller draws no bar graphic while the lister is still running: the
// total is a lower bound then, and a graphic fed by it would sit near full
// and mislead, git-gc style the stage is plain text until the denominator
// locks in. The graphic appears on the frame after ListingDone.
func deleteFiller(latest func() jobstate.DeleteStatus) mpb.BarFiller {
	bar := mpb.BarStyle().Build()

	return mpb.BarFillerFunc(func(w io.Writer, stat decor.Statistics) error {
		if !latest().ListingDone {
			return nil
		}

		return bar.Fill(w, stat)
	})
}

// reconcileDeleteBar aligns the bar with one snapshot. Deleted can briefly
// exceed Discovered — the lister and the deleter each run their own listing of
// the prefix — so the current is clamped to keep the bar below full until the
// job settles.
func reconcileDeleteBar(bar *mpb.Bar, status jobstate.DeleteStatus) {
	bar.SetTotal(status.Discovered, false)
	bar.SetCurrent(min(status.Deleted, status.Discovered))
}

// settleDeleteBar lands the bar on the outcome: success completes it at 100%
// regardless of the final counter skew, failure drops it so the error line
// that follows is what the user reads.
func settleDeleteBar(bar *mpb.Bar, status jobstate.DeleteStatus) {
	if status.State == jobstate.DeleteStateFail {
		bar.Abort(true)
		return
	}

	bar.SetTotal(-1, true)
}

// deletePhase is the git-gc-style stage label. The two strings are padded to
// the same width so the bar does not jump when the stage flips.
func deletePhase(status jobstate.DeleteStatus) string {
	if !status.ListingDone {
		return "listing objects  "
	}

	return "deleting objects "
}

// deleteCounters renders the object counts. While the lister is still running
// the total is a lower bound and is marked as approximate; once listing is
// done the denominator locks in. The deleted count is clamped to the
// discovered count: the two counters come from independent listings and the
// deleter can be briefly ahead, which reads as nonsense on a progress line.
func deleteCounters(status jobstate.DeleteStatus) string {
	deleted := min(status.Deleted, status.Discovered)
	if !status.ListingDone {
		return fmt.Sprintf("~%s found · %s deleted", commas(status.Discovered), commas(deleted))
	}

	return fmt.Sprintf("%s / %s", commas(deleted), commas(status.Discovered))
}

// deleteSpeed renders the average deletion rate over the job's lifetime. It
// stays blank for the first second, where the average is all noise.
func deleteSpeed(status jobstate.DeleteStatus) string {
	rate := deleteRate(status, time.Now())
	if rate < 1 {
		return ""
	}

	return fmt.Sprintf("%.0f obj/s", rate)
}

// deleteETA renders the time remaining once the total is known. Before that
// there is no denominator, so there is no estimate.
func deleteETA(status jobstate.DeleteStatus) string {
	if !status.ListingDone {
		return ""
	}

	remaining := status.Discovered - status.Deleted
	rate := deleteRate(status, time.Now())
	if remaining <= 0 || rate < 1 {
		return ""
	}

	eta := time.Duration(float64(remaining) / rate * float64(time.Second)).Truncate(time.Second)

	return "ETA " + eta.String()
}

// deleteRate is the average deletions per second since the job started, from
// one snapshot and one clock read.
func deleteRate(status jobstate.DeleteStatus, now time.Time) float64 {
	elapsed := now.Sub(status.StartTime)
	if status.Deleted <= 0 || elapsed <= 0 {
		return 0
	}

	return float64(status.Deleted) / elapsed.Seconds()
}

// deleteSummaryLine is the one-line outcome the CLI prints after the bar
// settles: how much was deleted and how long it took. Sub-second runs report
// milliseconds, so a fast delete does not read as "0s".
func deleteSummaryLine(status jobstate.DeleteStatus) string {
	cost := status.EndTime.Sub(status.StartTime)
	if cost >= time.Second {
		cost = cost.Truncate(time.Second)
	} else {
		cost = cost.Round(time.Millisecond)
	}

	return fmt.Sprintf("delete backup %q done: %s objects in %s", status.Name, commas(status.Deleted), cost)
}

// commas renders n with thousands separators, so wide object counts stay
// readable: 8902 -> "8,902".
func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + (len(s)-1)/3)
	lead := len(s) % 3
	if lead > 0 {
		b.WriteString(s[:lead])
	}
	for i := lead; i < len(s); i += 3 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}

	return b.String()
}
