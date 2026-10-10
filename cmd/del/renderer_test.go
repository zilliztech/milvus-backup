package del

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestCommas(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{12, "12"},
		{123, "123"},
		{1234, "1,234"},
		{1234567, "1,234,567"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, commas(c.in))
	}
}

func TestDeletePhase(t *testing.T) {
	t.Run("Listing", func(t *testing.T) {
		assert.Equal(t, "listing objects  ", deletePhase(jobstate.DeleteStatus{ListingDone: false}))
	})
	t.Run("ListingDone", func(t *testing.T) {
		assert.Equal(t, "deleting objects ", deletePhase(jobstate.DeleteStatus{ListingDone: true}))
	})
	// The two stages pad to the same width, so the bar does not jump when the
	// stage flips mid-line.
	assert.Equal(t, len("listing objects  "), len("deleting objects "))
}

func TestDeleteFiller(t *testing.T) {
	stat := decor.Statistics{Total: 100, Current: 50, AvailableWidth: 60, RequestedWidth: 60}

	t.Run("NoGraphicWhileListing", func(t *testing.T) {
		filler := deleteFiller(func() jobstate.DeleteStatus { return jobstate.DeleteStatus{ListingDone: false} })
		var buf strings.Builder
		require.NoError(t, filler.Fill(&buf, stat))
		assert.Empty(t, buf.String())
	})

	t.Run("GraphicOnceListingDone", func(t *testing.T) {
		filler := deleteFiller(func() jobstate.DeleteStatus { return jobstate.DeleteStatus{ListingDone: true} })
		var buf strings.Builder
		require.NoError(t, filler.Fill(&buf, stat))
		assert.Contains(t, buf.String(), "[")
	})
}

func TestDeleteCounters(t *testing.T) {
	t.Run("ListingMarksTotalApproximate", func(t *testing.T) {
		s := jobstate.DeleteStatus{Discovered: 8902, Deleted: 4231}
		assert.Equal(t, "~8,902 found · 4,231 deleted", deleteCounters(s))
	})
	t.Run("ListingDoneLocksDenominator", func(t *testing.T) {
		s := jobstate.DeleteStatus{ListingDone: true, Discovered: 9344, Deleted: 8120}
		assert.Equal(t, "8,120 / 9,344", deleteCounters(s))
	})
	t.Run("DeletedBeyondDiscoveredIsClamped", func(t *testing.T) {
		s := jobstate.DeleteStatus{ListingDone: true, Discovered: 100, Deleted: 101}
		assert.Equal(t, "100 / 100", deleteCounters(s))
	})
	t.Run("DeletedBeyondDiscoveredIsClampedWhileListing", func(t *testing.T) {
		// The deleter runs its own listing and can be briefly ahead of the
		// lister; the progress line still keeps deleted <= found.
		s := jobstate.DeleteStatus{ListingDone: false, Discovered: 8, Deleted: 19}
		assert.Equal(t, "~8 found · 8 deleted", deleteCounters(s))
	})
}

func TestDeleteRate(t *testing.T) {
	now := time.Now()

	t.Run("NothingDeleted", func(t *testing.T) {
		s := jobstate.DeleteStatus{StartTime: now.Add(-time.Minute)}
		assert.Equal(t, float64(0), deleteRate(s, now))
	})
	t.Run("NoTimeElapsed", func(t *testing.T) {
		s := jobstate.DeleteStatus{StartTime: now, Deleted: 100}
		assert.Equal(t, float64(0), deleteRate(s, now))
	})
	t.Run("AverageOverLifetime", func(t *testing.T) {
		s := jobstate.DeleteStatus{StartTime: now.Add(-10 * time.Second), Deleted: 3520}
		assert.InDelta(t, 352, deleteRate(s, now), 0.001)
	})
}

func TestDeleteSpeed(t *testing.T) {
	t.Run("BlankBelowOnePerSecond", func(t *testing.T) {
		s := jobstate.DeleteStatus{StartTime: time.Now()}
		assert.Empty(t, deleteSpeed(s))
	})
	t.Run("RoundsToWholeObjects", func(t *testing.T) {
		s := jobstate.DeleteStatus{StartTime: time.Now().Add(-10 * time.Second), Deleted: 3524}
		assert.Equal(t, "352 obj/s", deleteSpeed(s))
	})
}

func TestDeleteETA(t *testing.T) {
	t.Run("BlankWhileListing", func(t *testing.T) {
		s := jobstate.DeleteStatus{StartTime: time.Now().Add(-10 * time.Second), Discovered: 9344, Deleted: 8120}
		assert.Empty(t, deleteETA(s))
	})
	t.Run("BlankWhenNothingRemains", func(t *testing.T) {
		s := jobstate.DeleteStatus{ListingDone: true, StartTime: time.Now().Add(-10 * time.Second), Discovered: 9344, Deleted: 9344}
		assert.Empty(t, deleteETA(s))
	})
	t.Run("CountsDownFromRate", func(t *testing.T) {
		// 100 obj/s, 500 to go -> 5s.
		s := jobstate.DeleteStatus{ListingDone: true, StartTime: time.Now().Add(-10 * time.Second), Discovered: 1500, Deleted: 1000}
		assert.Equal(t, "ETA 5s", deleteETA(s))
	})
}

func TestDeleteSummaryLine(t *testing.T) {
	start := time.Now()

	t.Run("SecondsAndUp", func(t *testing.T) {
		s := jobstate.DeleteStatus{
			Name:      "my_backup",
			Deleted:   9344,
			StartTime: start,
			EndTime:   start.Add(27*time.Second + 567*time.Millisecond),
		}
		assert.Equal(t, `delete backup "my_backup" done: 9,344 objects in 27s`, deleteSummaryLine(s))
	})

	t.Run("SubSecondReportsMillis", func(t *testing.T) {
		s := jobstate.DeleteStatus{
			Name:      "my_backup",
			Deleted:   41,
			StartTime: start,
			EndTime:   start.Add(912345 * time.Microsecond),
		}
		assert.Equal(t, `delete backup "my_backup" done: 41 objects in 912ms`, deleteSummaryLine(s))
	})
}

func TestRenderDelete(t *testing.T) {
	newProgress := func(t *testing.T) *mpb.Progress {
		p := mpb.New(mpb.WithOutput(io.Discard))
		t.Cleanup(p.Shutdown)
		return p
	}

	t.Run("SuccessSettlesFromSnapshots", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker, err := store.AddDeleteTask("t1", "b1")
		require.NoError(t, err)
		tracker.SetRunning()
		tracker.AddDiscovered(100)

		p := newProgress(t)
		type outcome struct {
			status jobstate.DeleteStatus
			err    error
		}
		done := make(chan outcome, 1)
		go func() {
			status, err := renderDelete(context.Background(), p, store, "t1", time.Millisecond)
			done <- outcome{status, err}
		}()

		// Drive the job through listing-done to success; the renderer polls at
		// 1ms, so it observes whichever of these states it lands on and must
		// still converge on the terminal snapshot.
		tracker.SetListingDone()
		for i := 0; i < 100; i++ {
			tracker.IncDeleted()
		}
		tracker.SetSuccess()

		select {
		case got := <-done:
			require.NoError(t, got.err)
			assert.Equal(t, jobstate.DeleteStateSuccess, got.status.State)
			assert.Equal(t, int64(100), got.status.Deleted)
			assert.Equal(t, "b1", got.status.Name)
		case <-time.After(10 * time.Second):
			t.Fatal("renderDelete did not settle")
		}
	})

	t.Run("FailureCarriesMessage", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker, err := store.AddDeleteTask("t2", "b2")
		require.NoError(t, err)
		tracker.SetFail(errors.New("boom"))

		status, err := renderDelete(context.Background(), newProgress(t), store, "t2", time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, jobstate.DeleteStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
	})

	t.Run("ContextCancelAborts", func(t *testing.T) {
		store := jobstate.NewStore()
		_, err := store.AddDeleteTask("t3", "b3")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = renderDelete(ctx, newProgress(t), store, "t3", time.Millisecond)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("UnknownTask", func(t *testing.T) {
		store := jobstate.NewStore()
		_, err := renderDelete(context.Background(), newProgress(t), store, "nope", time.Millisecond)
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})
}
