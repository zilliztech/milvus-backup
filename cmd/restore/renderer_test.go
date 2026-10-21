package restore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vbauerster/mpb/v8"

	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// schemaHalf builds a collection snapshot with just a schema half, which is
// how a meta_only collection reads.
func schemaHalf(state jobstate.RestoreDDLState) jobstate.RestoreCollStatus {
	return jobstate.RestoreCollStatus{DDL: &jobstate.RestoreDDLStatus{State: state}}
}

// withJobs builds a collection snapshot with the given schema half and job
// halves; a nil ddl is the snapshot plan, which runs no DDL of its own.
func withJobs(ddl *jobstate.RestoreDDLStatus, jobs ...jobstate.RestoreJobStatus) jobstate.RestoreCollStatus {
	return jobstate.RestoreCollStatus{DDL: ddl, Jobs: jobs}
}

func ddl(state jobstate.RestoreDDLState) *jobstate.RestoreDDLStatus {
	return &jobstate.RestoreDDLStatus{State: state}
}

func TestRestoreCollStageOf(t *testing.T) {
	t.Run("SchemaRunningIsSchema", func(t *testing.T) {
		assert.Equal(t, collStageSchema, restoreCollStageOf(schemaHalf(jobstate.RestoreDDLExecuting)))
	})
	t.Run("MetaOnlySettlesOnSchemaAlone", func(t *testing.T) {
		assert.Equal(t, collStageSettled, restoreCollStageOf(schemaHalf(jobstate.RestoreDDLSuccess)))
	})
	t.Run("DataWinsOverSchema", func(t *testing.T) {
		coll := withJobs(ddl(jobstate.RestoreDDLExecuting),
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning})
		assert.Equal(t, collStageData, restoreCollStageOf(coll))
	})
	t.Run("SchemaDoneJobsQueuedIsDormant", func(t *testing.T) {
		coll := withJobs(ddl(jobstate.RestoreDDLSuccess),
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobPending})
		assert.Equal(t, collStageDormant, restoreCollStageOf(coll))
	})
	t.Run("AllJobsCompletedIsSettled", func(t *testing.T) {
		coll := withJobs(ddl(jobstate.RestoreDDLSuccess),
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobCompleted},
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobCompleted})
		assert.Equal(t, collStageSettled, restoreCollStageOf(coll))
	})
	t.Run("SnapshotPlanHasNoSchemaHalf", func(t *testing.T) {
		assert.Equal(t, collStageData, restoreCollStageOf(withJobs(nil,
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning})))
		assert.Equal(t, collStageSettled, restoreCollStageOf(withJobs(nil,
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobCompleted})))
	})
	t.Run("FailOnEitherHalfIsFailed", func(t *testing.T) {
		assert.Equal(t, collStageFailed, restoreCollStageOf(withJobs(
			ddl(jobstate.RestoreDDLSuccess),
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobFailed},
		)))
		assert.Equal(t, collStageFailed, restoreCollStageOf(withJobs(ddl(jobstate.RestoreDDLFail),
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobPending})))
	})
}

func TestRestoreStage(t *testing.T) {
	t.Run("Initial", func(t *testing.T) {
		assert.Equal(t, "starting      ", restoreStage(jobstate.RestoreStatus{State: jobstate.RestoreStateInitial}))
	})
	t.Run("ExecutingWithoutCollections", func(t *testing.T) {
		assert.Equal(t, "preparing     ", restoreStage(jobstate.RestoreStatus{State: jobstate.RestoreStateExecuting}))
	})
	t.Run("ExecutingImporting", func(t *testing.T) {
		s := jobstate.RestoreStatus{
			State: jobstate.RestoreStateExecuting,
			Colls: map[collref.Name]jobstate.RestoreCollStatus{
				collref.New("db1", "c1"): withJobs(ddl(jobstate.RestoreDDLSuccess),
					jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning}),
			},
		}
		assert.Equal(t, "importing     ", restoreStage(s))
	})
	t.Run("ExecutingRestoring", func(t *testing.T) {
		s := jobstate.RestoreStatus{
			State: jobstate.RestoreStateExecuting,
			Colls: map[collref.Name]jobstate.RestoreCollStatus{
				collref.New("db1", "c1"): withJobs(nil,
					jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning}),
			},
		}
		assert.Equal(t, "restoring     ", restoreStage(s))
	})
	t.Run("SuccessAndFail", func(t *testing.T) {
		assert.Equal(t, "done          ", restoreStage(jobstate.RestoreStatus{State: jobstate.RestoreStateSuccess}))
		assert.Equal(t, "failed        ", restoreStage(jobstate.RestoreStatus{State: jobstate.RestoreStateFail}))
	})
}

func TestRestoreCollPhase(t *testing.T) {
	t.Run("NoCollectionsIsPreparing", func(t *testing.T) {
		assert.Equal(t, "preparing", restoreCollPhase(jobstate.RestoreStatus{}))
	})
	t.Run("DataOutranksSchema", func(t *testing.T) {
		s := jobstate.RestoreStatus{Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "c1"): schemaHalf(jobstate.RestoreDDLExecuting),
			collref.New("db1", "c2"): withJobs(ddl(jobstate.RestoreDDLSuccess),
				jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning}),
		}}
		assert.Equal(t, "importing", restoreCollPhase(s))
	})
	t.Run("AllSettledIsFinalizing", func(t *testing.T) {
		s := jobstate.RestoreStatus{Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "c1"): schemaHalf(jobstate.RestoreDDLSuccess),
			collref.New("db1", "c2"): withJobs(nil,
				jobstate.RestoreJobStatus{State: jobstate.RestoreJobCompleted}),
		}}
		assert.Equal(t, "finalizing", restoreCollPhase(s))
	})
	t.Run("QueuedOnlyIsPreparing", func(t *testing.T) {
		s := jobstate.RestoreStatus{Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "c1"): withJobs(ddl(jobstate.RestoreDDLSuccess),
				jobstate.RestoreJobStatus{State: jobstate.RestoreJobPending}),
		}}
		assert.Equal(t, "preparing", restoreCollPhase(s))
	})
}

func TestRestoreTotalCounters(t *testing.T) {
	t.Run("BlankBeforeCollections", func(t *testing.T) {
		assert.Empty(t, restoreTotalCounters(jobstate.RestoreStatus{}))
	})
	t.Run("CountsSettled", func(t *testing.T) {
		s := jobstate.RestoreStatus{Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "c1"): schemaHalf(jobstate.RestoreDDLSuccess),
			collref.New("db1", "c2"): withJobs(nil,
				jobstate.RestoreJobStatus{State: jobstate.RestoreJobCompleted}),
			collref.New("db1", "c3"): withJobs(ddl(jobstate.RestoreDDLSuccess),
				jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning}),
		}}
		assert.Equal(t, "2 / 3", restoreTotalCounters(s))
	})
}

func TestRestoreCollPercent(t *testing.T) {
	t.Run("WeightedByJobSize", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			TotalSize: 100,
			Jobs: []jobstate.RestoreJobStatus{
				{State: jobstate.RestoreJobRunning, Size: 10, Progress: 100},
				{State: jobstate.RestoreJobRunning, Size: 20, Progress: 50},
				{State: jobstate.RestoreJobRunning, Size: 30, Progress: 0},
			},
		}
		assert.Equal(t, int32(20), restoreCollPercent(coll))
	})
	t.Run("CompletedCountsFull", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			TotalSize: 100,
			Jobs: []jobstate.RestoreJobStatus{
				// the last polled ratio does not matter once the job completed
				{State: jobstate.RestoreJobCompleted, Size: 50, Progress: 30},
				{State: jobstate.RestoreJobPending, Size: 50},
			},
		}
		assert.Equal(t, int32(50), restoreCollPercent(coll))
	})
	t.Run("ClampedToFull", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			TotalSize: 100,
			Jobs:      []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning, Size: 200, Progress: 100}},
		}
		assert.Equal(t, int32(100), restoreCollPercent(coll))
	})
	t.Run("ZeroTotalSize", func(t *testing.T) {
		assert.Equal(t, int32(0), restoreCollPercent(jobstate.RestoreCollStatus{
			Jobs: []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning}},
		}))
	})
}

func TestRestoreCollStageLabel(t *testing.T) {
	t.Run("Schema", func(t *testing.T) {
		assert.Equal(t, "schema   ", restoreCollStageLabel(schemaHalf(jobstate.RestoreDDLExecuting)))
	})
	t.Run("Importing", func(t *testing.T) {
		coll := withJobs(ddl(jobstate.RestoreDDLSuccess),
			jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning})
		assert.Equal(t, "importing", restoreCollStageLabel(coll))
	})
	t.Run("Restoring", func(t *testing.T) {
		coll := withJobs(nil, jobstate.RestoreJobStatus{State: jobstate.RestoreJobRunning})
		assert.Equal(t, "restoring", restoreCollStageLabel(coll))
	})
	t.Run("SettledIsBlank", func(t *testing.T) {
		assert.Equal(t, "         ", restoreCollStageLabel(schemaHalf(jobstate.RestoreDDLSuccess)))
	})
}

func TestRestoreCollStatsLine(t *testing.T) {
	t.Run("BlankOutsideDataPhase", func(t *testing.T) {
		assert.Empty(t, restoreCollStatsLine(schemaHalf(jobstate.RestoreDDLExecuting)))
	})
	t.Run("SingleJobIsPercentOnly", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			TotalSize: 100,
			Jobs:      []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning, Size: 100, Progress: 42}},
		}
		assert.Equal(t, "42%", restoreCollStatsLine(coll))
	})
	t.Run("MultipleJobsAddCounters", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			TotalSize: 100,
			Jobs: []jobstate.RestoreJobStatus{
				{State: jobstate.RestoreJobCompleted, Size: 50},
				{State: jobstate.RestoreJobRunning, Size: 50, Progress: 20},
			},
		}
		assert.Equal(t, "60% 1 / 2 jobs", restoreCollStatsLine(coll))
	})
}

func TestFitName(t *testing.T) {
	t.Run("PadsShortName", func(t *testing.T) {
		assert.Equal(t, "db1.c1      ", fitName("db1.c1", 12))
	})
	t.Run("TruncatesLongName", func(t *testing.T) {
		assert.Equal(t, "db1.verylon…", fitName("db1.verylongcollection", 12))
	})
	t.Run("KeepsExactFit", func(t *testing.T) {
		assert.Equal(t, "db1.coll", fitName("db1.coll", 8))
	})
}

func TestReconcileRestoreCollBars(t *testing.T) {
	newProgress := func(t *testing.T) *mpb.Progress {
		p := mpb.New(mpb.WithOutput(io.Discard))
		t.Cleanup(p.Shutdown)
		return p
	}

	t.Run("CapsShownBars", func(t *testing.T) {
		p := newProgress(t)
		bars := make(map[collref.Name]*mpb.Bar)
		status := jobstate.RestoreStatus{Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "c1"): withJobs(ddl(jobstate.RestoreDDLExecuting)),
			collref.New("db1", "c2"): withJobs(ddl(jobstate.RestoreDDLExecuting)),
			collref.New("db1", "c3"): withJobs(ddl(jobstate.RestoreDDLExecuting)),
		}}
		latest := &atomic.Value{}
		latest.Store(status)

		reconcileRestoreCollBars(p, latest, bars, status, 2)
		assert.Len(t, bars, 2)
		// The cap keeps the sorted head, so the shown set is stable across
		// ticks instead of flickering with map order.
		reconcileRestoreCollBars(p, latest, bars, status, 2)
		assert.Len(t, bars, 2)
		assert.Contains(t, bars, collref.New("db1", "c1"))
		assert.Contains(t, bars, collref.New("db1", "c2"))
	})

	t.Run("DormantAndSettledBarsLeave", func(t *testing.T) {
		p := newProgress(t)
		bars := make(map[collref.Name]*mpb.Bar)
		c1 := collref.New("db1", "c1")
		status := jobstate.RestoreStatus{Colls: map[collref.Name]jobstate.RestoreCollStatus{
			c1: schemaHalf(jobstate.RestoreDDLExecuting),
		}}
		latest := &atomic.Value{}
		latest.Store(status)
		reconcileRestoreCollBars(p, latest, bars, status, 4)
		require.Len(t, bars, 1)

		status.Colls[c1] = schemaHalf(jobstate.RestoreDDLSuccess)
		reconcileRestoreCollBars(p, latest, bars, status, 4)
		assert.Empty(t, bars)
	})
}

func TestRenderRestore(t *testing.T) {
	newProgress := func(t *testing.T) *mpb.Progress {
		p := mpb.New(mpb.WithOutput(io.Discard))
		t.Cleanup(p.Shutdown)
		return p
	}

	t.Run("SuccessSettlesFromSnapshots", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddRestoreTask("t1")
		collRef := collref.New("db1", "coll1")
		coll := tracker.NewCollTracker(collRef, 100)

		p := newProgress(t)
		type outcome struct {
			status jobstate.RestoreStatus
			err    error
		}
		done := make(chan outcome, 1)
		go func() {
			status, err := renderRestore(context.Background(), p, store, "t1", time.Millisecond, 4)
			done <- outcome{status, err}
		}()

		// Drive the job through its phases to success; the renderer polls at
		// 1ms, so it observes whichever of these states it lands on and must
		// still converge on the terminal snapshot.
		tracker.SetExecuting()
		ddl := coll.DDL()
		ddl.SetExecuting()
		job := coll.NewJob(100)
		job.Bind("job-1")
		ddl.SetSuccess()
		job.SetProgress(40)
		job.SetCompleted()
		tracker.SetSuccess()

		select {
		case got := <-done:
			require.NoError(t, got.err)
			assert.Equal(t, jobstate.RestoreStateSuccess, got.status.State)
			assert.Equal(t, collStageSettled, restoreCollStageOf(got.status.Colls[collRef]))
		case <-time.After(10 * time.Second):
			t.Fatal("renderRestore did not settle")
		}
	})

	t.Run("FailureCarriesMessage", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddRestoreTask("t2")
		tracker.SetExecuting()
		tracker.SetFail(errors.New("boom"))

		status, err := renderRestore(context.Background(), newProgress(t), store, "t2", time.Millisecond, 4)
		require.NoError(t, err)
		assert.Equal(t, jobstate.RestoreStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
	})

	t.Run("NoBarsBeforeFirstPoll", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddRestoreTask("t3")
		tracker.SetFail(errors.New("boom"))

		// The job settled before the renderer's first poll, so no bar is
		// ever created; the renderer just answers with the terminal status.
		status, err := renderRestore(context.Background(), newProgress(t), store, "t3", time.Millisecond, 4)
		require.NoError(t, err)
		assert.Equal(t, jobstate.RestoreStateFail, status.State)
	})

	t.Run("UnknownTaskID", func(t *testing.T) {
		store := jobstate.NewStore()

		_, err := renderRestore(context.Background(), newProgress(t), store, "nope", time.Millisecond, 4)
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})

	t.Run("ContextCancelAborts", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker := store.AddRestoreTask("t4")
		tracker.SetExecuting()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := renderRestore(ctx, newProgress(t), store, "t4", time.Millisecond, 4)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// mpb only renders frames when auto-refresh is on (a terminal output implies
// it); with a plain buffer output and no auto-refresh nothing is ever
// written, which is exactly how a 0x0 pty swallowed every frame in the wild.
func TestRenderRestoreEmitsFrames(t *testing.T) {
	var buf bytes.Buffer
	p := mpb.New(mpb.WithOutput(&buf), mpb.WithAutoRefresh())

	store := jobstate.NewStore()
	tracker := store.AddRestoreTask("frames")
	collRef := collref.New("db1", "coll1")
	coll := tracker.NewCollTracker(collRef, 100)

	result := make(chan error, 1)
	go func() {
		_, err := renderRestore(context.Background(), p, store, "frames", 20*time.Millisecond, 4)
		result <- err
	}()

	tracker.SetExecuting()
	ddl := coll.DDL()
	ddl.SetExecuting()
	time.Sleep(50 * time.Millisecond)
	ddl.SetSuccess()
	job := coll.NewJob(100)
	job.Bind("job-1")
	for i := 0; i < 10; i++ {
		job.SetProgress(int32((i + 1) * 10))
		time.Sleep(30 * time.Millisecond)
	}
	job.SetCompleted()
	tracker.SetSuccess()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("renderRestore did not settle")
	}

	// The render goroutine keeps writing to buf until the container shuts
	// down, so Shutdown (not defer) must precede any read of buf.
	p.Shutdown()

	assert.Contains(t, buf.String(), "db1.coll1")
	assert.Contains(t, buf.String(), "importing")
	// The collection bar's right side carries the percent; the exact number
	// depends on which refresh frames landed, so only its presence is pinned.
	assert.Contains(t, buf.String(), "%")
}
