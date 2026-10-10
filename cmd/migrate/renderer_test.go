package migrate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vbauerster/mpb/v8"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestMigrateCounters(t *testing.T) {
	t.Run("MarksTotalApproximate", func(t *testing.T) {
		s := jobstate.MigrateStatus{CopiedSize: 512 << 20, TotalSize: 1 << 30}
		assert.Equal(t, "512.00 MiB / ~1.00 GiB", migrateCounters(s))
	})
	t.Run("SubKiB", func(t *testing.T) {
		s := jobstate.MigrateStatus{CopiedSize: 512, TotalSize: 1024}
		assert.Equal(t, "512.00 b / ~1.00 KiB", migrateCounters(s))
	})
}

func TestMigrateRate(t *testing.T) {
	now := time.Now()

	t.Run("NothingCopied", func(t *testing.T) {
		s := jobstate.MigrateStatus{StartTime: now.Add(-time.Minute)}
		assert.Equal(t, float64(0), migrateRate(s, now))
	})
	t.Run("NoTimeElapsed", func(t *testing.T) {
		s := jobstate.MigrateStatus{StartTime: now, CopiedSize: 100}
		assert.Equal(t, float64(0), migrateRate(s, now))
	})
	t.Run("AverageOverLifetime", func(t *testing.T) {
		s := jobstate.MigrateStatus{StartTime: now.Add(-10 * time.Second), CopiedSize: 3520}
		assert.InDelta(t, 352, migrateRate(s, now), 0.001)
	})
}

func TestMigrateSpeed(t *testing.T) {
	t.Run("BlankBelowOneBytePerSecond", func(t *testing.T) {
		s := jobstate.MigrateStatus{StartTime: time.Now()}
		assert.Empty(t, migrateSpeed(s))
	})
	t.Run("RendersWithUnit", func(t *testing.T) {
		// 15 MiB in 10s -> 1.50 MiB/s; the millisecond-scale drift between
		// the test clock and the render clock cannot move the second decimal.
		s := jobstate.MigrateStatus{StartTime: time.Now().Add(-10 * time.Second), CopiedSize: 15 << 20}
		assert.Equal(t, "1.50 MiB/s", migrateSpeed(s))
	})
}

func TestMigrateETA(t *testing.T) {
	t.Run("BlankWhenNothingCopied", func(t *testing.T) {
		s := jobstate.MigrateStatus{StartTime: time.Now().Add(-10 * time.Second), TotalSize: 1500}
		assert.Empty(t, migrateETA(s))
	})
	t.Run("BlankWhenTotalPassed", func(t *testing.T) {
		// The total excludes partition L0 and meta, so the copy can run past
		// it near the end; there is no estimate left to give then.
		s := jobstate.MigrateStatus{StartTime: time.Now().Add(-10 * time.Second), TotalSize: 1000, CopiedSize: 1001}
		assert.Empty(t, migrateETA(s))
	})
	t.Run("CountsDownFromRate", func(t *testing.T) {
		// 100 B/s, 500 to go -> 5s.
		s := jobstate.MigrateStatus{StartTime: time.Now().Add(-10 * time.Second), TotalSize: 1500, CopiedSize: 1000}
		assert.Equal(t, "ETA 5s", migrateETA(s))
	})
}

func TestMigrateStatsLine(t *testing.T) {
	t.Run("BlankSpeedAndETAWhenNothingCopied", func(t *testing.T) {
		s := jobstate.MigrateStatus{CopiedSize: 100, TotalSize: 1024}
		assert.Equal(t, "100.00 b / ~1.00 KiB", migrateStatsLine(s))
	})
	t.Run("JoinsCountersSpeedETA", func(t *testing.T) {
		s := jobstate.MigrateStatus{StartTime: time.Now().Add(-10 * time.Second), CopiedSize: 15 << 20, TotalSize: 30 << 20}
		// speed and ETA drift with the clock; assert the joins, not the digits.
		line := migrateStatsLine(s)
		assert.Contains(t, line, " / ~30.00 MiB ")
		assert.Contains(t, line, "MiB/s ETA ")
	})
}

func TestRenderMigrate(t *testing.T) {
	newProgress := func(t *testing.T) *mpb.Progress {
		p := mpb.New(mpb.WithOutput(io.Discard))
		t.Cleanup(p.Shutdown)
		return p
	}

	t.Run("SuccessSettlesFromSnapshots", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddMigrateTask("t1", 1<<20)
		tracker.SetRunning()
		tracker.SetCopyStart()

		p := newProgress(t)
		type outcome struct {
			status jobstate.MigrateStatus
			err    error
		}
		done := make(chan outcome, 1)
		go func() {
			status, err := renderMigrate(context.Background(), p, store, "t1", time.Millisecond)
			done <- outcome{status, err}
		}()

		// Drive the job through the copy to success; the renderer polls at
		// 1ms, so it observes whichever of these states it lands on and must
		// still converge on the terminal snapshot.
		tracker.IncCopied(1 << 20)
		tracker.SetCopyDone()
		tracker.SetJobID("job-42")
		tracker.SetSuccess()

		select {
		case got := <-done:
			require.NoError(t, got.err)
			assert.Equal(t, jobstate.MigrateStateSuccess, got.status.State)
			assert.Equal(t, int64(1<<20), got.status.CopiedSize)
			assert.Equal(t, "job-42", got.status.MigrateJobID)
		case <-time.After(10 * time.Second):
			t.Fatal("renderMigrate did not settle")
		}
	})

	t.Run("FailureCarriesMessage", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddMigrateTask("t2", 1<<20)
		tracker.SetRunning()
		tracker.SetFail(errors.New("boom"))

		status, err := renderMigrate(context.Background(), newProgress(t), store, "t2", time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, jobstate.MigrateStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
	})

	t.Run("NoBarBeforeCopyStart", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddMigrateTask("t3", 1<<20)
		tracker.SetRunning()
		tracker.SetFail(errors.New("boom"))

		// The job failed before the copy started, so no bar is ever created;
		// the renderer just answers with the terminal status.
		status, err := renderMigrate(context.Background(), newProgress(t), store, "t3", time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, jobstate.MigrateStateFail, status.State)
	})

	t.Run("UnknownTaskID", func(t *testing.T) {
		store := jobstate.NewStore()

		_, err := renderMigrate(context.Background(), newProgress(t), store, "nope", time.Millisecond)
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})

	t.Run("ContextCancelAborts", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddMigrateTask("t4", 1<<20)
		tracker.SetRunning()
		tracker.SetCopyStart()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := renderMigrate(ctx, newProgress(t), store, "t4", time.Millisecond)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// mpb only renders frames when auto-refresh is on (a terminal output implies
// it); with a plain buffer output and no auto-refresh nothing is ever
// written, which is exactly how a 0x0 pty swallowed every frame in the wild.
func TestRenderMigrateEmitsFrames(t *testing.T) {
	var buf bytes.Buffer
	p := mpb.New(mpb.WithOutput(&buf), mpb.WithAutoRefresh())

	store := jobstate.NewStore()
	tracker := store.AddMigrateTask("frames", 104857600)
	tracker.SetRunning()
	tracker.SetCopyStart()

	result := make(chan error, 1)
	go func() {
		_, err := renderMigrate(context.Background(), p, store, "frames", 20*time.Millisecond)
		result <- err
	}()

	for i := 0; i < 20; i++ {
		tracker.IncCopied(5 << 20)
		time.Sleep(50 * time.Millisecond)
	}
	tracker.SetCopyDone()
	tracker.SetSuccess()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("renderMigrate did not settle")
	}

	// The render goroutine keeps writing to buf until the container shuts
	// down, so Shutdown (not defer) must precede any read of buf.
	p.Shutdown()

	assert.Contains(t, buf.String(), "Uploading")
	assert.Contains(t, buf.String(), "MiB/s ETA")
}
