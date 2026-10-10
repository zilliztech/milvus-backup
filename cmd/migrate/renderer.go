package migrate

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// renderMigrate draws the upload progress bar for the migrate job registered
// under taskID, reconciling it against store snapshots until done delivers
// the Execute result, then settles the bar and answers with that result.
// Every update written to the bar is an absolute value from one snapshot, so
// a dropped or doubled tick cannot skew it. A job that settles before the
// first poll shows CopyStarted gets no bar at all.
func renderMigrate(ctx context.Context, p *mpb.Progress, store *jobstate.Store, taskID string, interval time.Duration, done <-chan error) error {
	var latest atomic.Value // jobstate.MigrateStatus, read by the decorators

	var bar *mpb.Bar
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		status, err := store.GetMigrateTask(taskID)
		if err != nil {
			if bar != nil {
				bar.Abort(true)
			}
			return fmt.Errorf("jobview: read migrate task %s: %w", taskID, err)
		}
		latest.Store(status)

		if status.CopyStarted {
			if bar == nil {
				bar = newMigrateBar(p, &latest)
			}
			reconcileMigrateBar(bar, status)
		}

		select {
		case err := <-done:
			if bar == nil {
				return err
			}
			settleMigrateBar(bar, err)
			bar.Wait()
			return err
		case <-ctx.Done():
			if bar != nil {
				bar.Abort(true)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// newMigrateBar builds the single upload bar. The counters, speed, and ETA
// are dynamic decorators reading the renderer's latest snapshot.
func newMigrateBar(p *mpb.Progress, latest *atomic.Value) *mpb.Bar {
	snapshot := func() jobstate.MigrateStatus {
		return latest.Load().(jobstate.MigrateStatus)
	}

	return p.MustAdd(0,
		mpb.BarStyle().Rbound("|").Build(),
		mpb.PrependDecorators(
			decor.Name("Uploading  "),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string { return migrateStatsLine(snapshot()) }, decor.WC{C: decor.DextraSpace}),
		),
		mpb.BarRemoveOnComplete(),
	)
}

// migrateStatsLine renders the right side of the bar as one decorator:
// separate Any decorators with DextraSpace do not reliably space apart (the
// extra space is a width hint, not a literal pad), so the line is joined here
// instead.
func migrateStatsLine(status jobstate.MigrateStatus) string {
	parts := []string{migrateCounters(status)}
	if v := migrateSpeed(status); v != "" {
		parts = append(parts, v)
	}
	if v := migrateETA(status); v != "" {
		parts = append(parts, v)
	}

	return strings.Join(parts, " ")
}

// reconcileMigrateBar aligns the bar with one snapshot. TotalSize excludes
// partition L0 segments and meta, so CopiedSize can briefly pass it; the
// current is clamped to keep the bar below full until the copy settles.
func reconcileMigrateBar(bar *mpb.Bar, status jobstate.MigrateStatus) {
	bar.SetTotal(status.TotalSize, false)
	bar.SetCurrent(min(status.CopiedSize, status.TotalSize))
}

// settleMigrateBar lands the bar on the outcome: success completes it at 100%
// regardless of the approximate denominator, failure drops it so the error
// line that follows is what the user reads.
func settleMigrateBar(bar *mpb.Bar, err error) {
	if err != nil {
		bar.Abort(true)
		return
	}

	bar.SetTotal(-1, true)
}

// migrateCounters renders the byte counts. The total comes from backupinfo
// and excludes partition L0 and meta, so it stays marked approximate for the
// whole upload.
func migrateCounters(status jobstate.MigrateStatus) string {
	return fmt.Sprintf("%s / ~%s", sizeB1024(status.CopiedSize), sizeB1024(status.TotalSize))
}

// migrateSpeed renders the average upload rate over the copy's lifetime. It
// stays blank for the first second, where the average is all noise.
func migrateSpeed(status jobstate.MigrateStatus) string {
	rate := migrateRate(status, time.Now())
	if rate < 1 {
		return ""
	}

	return sizeB1024(int64(rate)) + "/s"
}

// migrateETA renders the time remaining from the average rate. Once
// CopiedSize passes the approximate total there is no estimate left to give.
func migrateETA(status jobstate.MigrateStatus) string {
	remaining := status.TotalSize - status.CopiedSize
	rate := migrateRate(status, time.Now())
	if remaining <= 0 || rate < 1 {
		return ""
	}

	eta := time.Duration(float64(remaining) / rate * float64(time.Second)).Truncate(time.Second)

	return "ETA " + eta.String()
}

// migrateRate is the average bytes per second since the copy started, from
// one snapshot and one clock read.
func migrateRate(status jobstate.MigrateStatus, now time.Time) float64 {
	elapsed := now.Sub(status.StartTime)
	if status.CopiedSize <= 0 || elapsed <= 0 {
		return 0
	}

	return float64(status.CopiedSize) / elapsed.Seconds()
}

// sizeB1024 renders a byte count with a binary unit, e.g. "1.50 GiB".
func sizeB1024(n int64) string {
	return fmt.Sprintf("% .2f", decor.SizeB1024(max(n, 0)))
}
