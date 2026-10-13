package create

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

// ddlOnly builds a collection snapshot with just a schema half, which is how
// a meta_only job's collections read.
func ddlOnly(state jobstate.BackupCollDDLState) jobstate.BackupCollStatus {
	return jobstate.BackupCollStatus{DDL: &jobstate.BackupCollDDLStatus{State: state}}
}

// bothHalves builds a collection snapshot with both halves in the given
// states.
func bothHalves(ddl jobstate.BackupCollDDLState, dml jobstate.BackupCollDMLState) jobstate.BackupCollStatus {
	return jobstate.BackupCollStatus{
		DDL: &jobstate.BackupCollDDLStatus{State: ddl},
		DML: &jobstate.BackupCollDMLStatus{State: dml},
	}
}

func TestBackupCollStageOf(t *testing.T) {
	t.Run("MetaOnlySettlesOnSchemaAlone", func(t *testing.T) {
		assert.Equal(t, collStageSettled, backupCollStageOf(ddlOnly(jobstate.BackupCollDDLStateSuccess)))
	})
	t.Run("MetaOnlyInFlightIsSchema", func(t *testing.T) {
		assert.Equal(t, collStageSchema, backupCollStageOf(ddlOnly(jobstate.BackupCollDDLStateExecuting)))
	})
	t.Run("DataStagesWinOverSchema", func(t *testing.T) {
		// The plans race the two halves; while data is moving, that is what
		// the collection is doing.
		coll := bothHalves(jobstate.BackupCollDDLStateExecuting, jobstate.BackupCollDMLStateExecuting)
		assert.Equal(t, collStageCopying, backupCollStageOf(coll))
	})
	t.Run("DataDoneSchemaStillRunningIsSchema", func(t *testing.T) {
		coll := bothHalves(jobstate.BackupCollDDLStateExecuting, jobstate.BackupCollDMLStateSuccess)
		assert.Equal(t, collStageSchema, backupCollStageOf(coll))
	})
	t.Run("BothHalvesDoneIsSettled", func(t *testing.T) {
		coll := bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateSuccess)
		assert.Equal(t, collStageSettled, backupCollStageOf(coll))
	})
	t.Run("SchemaDoneDataQueuedIsDormant", func(t *testing.T) {
		coll := bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStatePending)
		assert.Equal(t, collStageDormant, backupCollStageOf(coll))
	})
	t.Run("FailOnEitherHalfIsFailed", func(t *testing.T) {
		assert.Equal(t, collStageFailed, backupCollStageOf(bothHalves(jobstate.BackupCollDDLStateFail, jobstate.BackupCollDMLStateExecuting)))
		assert.Equal(t, collStageFailed, backupCollStageOf(bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateFail)))
	})
	t.Run("FlushingAndListing", func(t *testing.T) {
		assert.Equal(t, collStageFlushing, backupCollStageOf(bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateFlushing)))
		assert.Equal(t, collStageListing, backupCollStageOf(bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStatePrepare)))
	})
}

func TestBackupStage(t *testing.T) {
	t.Run("DatabasePhase", func(t *testing.T) {
		s := jobstate.BackupStatus{State: jobstate.BackupStateDatabaseExecuting}
		assert.Equal(t, "database meta ", backupStage(s))
	})
	t.Run("CollectionPhase", func(t *testing.T) {
		s := jobstate.BackupStatus{
			State: jobstate.BackupStateCollectionExecuting,
			Colls: map[collref.Name]jobstate.BackupCollStatus{
				collref.New("db1", "c1"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateExecuting),
			},
		}
		assert.Equal(t, "copying       ", backupStage(s))
	})
}

func TestBackupCollPhase(t *testing.T) {
	t.Run("EmptyPhase", func(t *testing.T) {
		s := jobstate.BackupStatus{State: jobstate.BackupStateCollectionExecuting}
		assert.Equal(t, "collections", backupCollPhase(s))
	})
	t.Run("CopyingOutranksFlushing", func(t *testing.T) {
		s := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			collref.New("db1", "c1"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateFlushing),
			collref.New("db1", "c2"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateExecuting),
		}}
		assert.Equal(t, "copying", backupCollPhase(s))
	})
	t.Run("AllSettledIsFinalizing", func(t *testing.T) {
		s := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			collref.New("db1", "c1"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateSuccess),
			collref.New("db1", "c2"): ddlOnly(jobstate.BackupCollDDLStateSuccess),
		}}
		assert.Equal(t, "finalizing", backupCollPhase(s))
	})
	t.Run("PendingOnlyIsCollections", func(t *testing.T) {
		s := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			collref.New("db1", "c1"): bothHalves(jobstate.BackupCollDDLStatePending, jobstate.BackupCollDMLStatePending),
		}}
		assert.Equal(t, "collections", backupCollPhase(s))
	})
}

func TestBackupTotalCounters(t *testing.T) {
	t.Run("BlankBeforeCollections", func(t *testing.T) {
		assert.Empty(t, backupTotalCounters(jobstate.BackupStatus{}))
	})
	t.Run("CountsSettled", func(t *testing.T) {
		s := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			collref.New("db1", "c1"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateSuccess),
			collref.New("db1", "c2"): ddlOnly(jobstate.BackupCollDDLStateSuccess),
			collref.New("db1", "c3"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateExecuting),
		}}
		assert.Equal(t, "2 / 3", backupTotalCounters(s))
	})
}

func TestBackupCollCounters(t *testing.T) {
	t.Run("BytesReadExact", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{CopiedSize: 512 << 20, TotalSize: 1 << 30}
		assert.Equal(t, "512.00 MiB / 1.00 GiB", backupCollCounters(jobstate.BackupUnitBytes, dml))
	})
	t.Run("FilesStayApproximate", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{CopiedSize: 120, TotalSize: 340}
		assert.Equal(t, "120 / ~340 files", backupCollCounters(jobstate.BackupUnitFiles, dml))
	})
	t.Run("CopiedClampedToTotal", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{CopiedSize: 341, TotalSize: 340}
		assert.Equal(t, "340 / ~340 files", backupCollCounters(jobstate.BackupUnitFiles, dml))
	})
}

func TestBackupCollRate(t *testing.T) {
	now := time.Now()

	t.Run("NothingCopied", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{StartTime: now.Add(-time.Minute)}
		assert.Equal(t, float64(0), backupCollRate(dml, now))
	})
	t.Run("NoTimeElapsed", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{StartTime: now, CopiedSize: 100}
		assert.Equal(t, float64(0), backupCollRate(dml, now))
	})
	t.Run("AverageOverLifetime", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{StartTime: now.Add(-10 * time.Second), CopiedSize: 3520}
		assert.InDelta(t, 352, backupCollRate(dml, now), 0.001)
	})
}

func TestBackupCollSpeed(t *testing.T) {
	t.Run("BlankBelowOnePerSecond", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{StartTime: time.Now()}
		assert.Empty(t, backupCollSpeed(jobstate.BackupUnitBytes, dml, time.Now()))
	})
	t.Run("BytesRenderWithUnit", func(t *testing.T) {
		// 15 MiB in 10s -> 1.50 MiB/s; the millisecond-scale drift between
		// the test clock and the render clock cannot move the second decimal.
		dml := &jobstate.BackupCollDMLStatus{StartTime: time.Now().Add(-10 * time.Second), CopiedSize: 15 << 20}
		assert.Equal(t, "1.50 MiB/s", backupCollSpeed(jobstate.BackupUnitBytes, dml, time.Now()))
	})
	t.Run("FilesRenderAsCount", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{StartTime: time.Now().Add(-10 * time.Second), CopiedSize: 120}
		assert.Equal(t, "12 files/s", backupCollSpeed(jobstate.BackupUnitFiles, dml, time.Now()))
	})
}

func TestBackupCollETA(t *testing.T) {
	t.Run("BlankWhenNothingCopied", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{StartTime: time.Now().Add(-10 * time.Second), TotalSize: 1500}
		assert.Empty(t, backupCollETA(dml, time.Now()))
	})
	t.Run("BlankWhenTotalPassed", func(t *testing.T) {
		dml := &jobstate.BackupCollDMLStatus{StartTime: time.Now().Add(-10 * time.Second), TotalSize: 1000, CopiedSize: 1001}
		assert.Empty(t, backupCollETA(dml, time.Now()))
	})
	t.Run("CountsDownFromRate", func(t *testing.T) {
		// 100 B/s, 500 to go -> 5s.
		dml := &jobstate.BackupCollDMLStatus{StartTime: time.Now().Add(-10 * time.Second), TotalSize: 1500, CopiedSize: 1000}
		assert.Equal(t, "ETA 5s", backupCollETA(dml, time.Now()))
	})
}

func TestBackupCollStatsLine(t *testing.T) {
	name := collref.New("db1", "c1")

	t.Run("BlankOutsideCopyPhase", func(t *testing.T) {
		s := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			name: bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStatePrepare),
		}}
		assert.Empty(t, backupCollStatsLine(s, name, time.Now()))
	})
	t.Run("BlankWithoutDataHalf", func(t *testing.T) {
		s := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			name: ddlOnly(jobstate.BackupCollDDLStateExecuting),
		}}
		assert.Empty(t, backupCollStatsLine(s, name, time.Now()))
	})
	t.Run("JoinsCountersSpeedETA", func(t *testing.T) {
		s := jobstate.BackupStatus{
			Unit: jobstate.BackupUnitBytes,
			Colls: map[collref.Name]jobstate.BackupCollStatus{
				name: {DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateExecuting, StartTime: time.Now().Add(-10 * time.Second), CopiedSize: 15 << 20, TotalSize: 30 << 20}},
			},
		}
		// speed and ETA drift with the clock; assert the joins, not the digits.
		line := backupCollStatsLine(s, name, time.Now())
		assert.Contains(t, line, " / 30.00 MiB ")
		assert.Contains(t, line, "MiB/s ETA ")
	})
	t.Run("MissingCollectionReadsBlank", func(t *testing.T) {
		assert.Empty(t, backupCollStatsLine(jobstate.BackupStatus{}, name, time.Now()))
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

func TestReconcileBackupCollBars(t *testing.T) {
	newProgress := func(t *testing.T) *mpb.Progress {
		p := mpb.New(mpb.WithOutput(io.Discard))
		t.Cleanup(p.Shutdown)
		return p
	}

	t.Run("CapsShownBars", func(t *testing.T) {
		p := newProgress(t)
		bars := make(map[collref.Name]*mpb.Bar)
		status := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			collref.New("db1", "c1"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateFlushing),
			collref.New("db1", "c2"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateFlushing),
			collref.New("db1", "c3"): bothHalves(jobstate.BackupCollDDLStateSuccess, jobstate.BackupCollDMLStateFlushing),
		}}
		latest := &atomic.Value{}
		latest.Store(status)

		reconcileBackupCollBars(p, latest, bars, status, 2)
		assert.Len(t, bars, 2)
		// The cap keeps the sorted head, so the shown set is stable across
		// ticks instead of flickering with map order.
		reconcileBackupCollBars(p, latest, bars, status, 2)
		assert.Len(t, bars, 2)
		assert.Contains(t, bars, collref.New("db1", "c1"))
		assert.Contains(t, bars, collref.New("db1", "c2"))
	})

	t.Run("DormantAndSettledBarsLeave", func(t *testing.T) {
		p := newProgress(t)
		bars := make(map[collref.Name]*mpb.Bar)
		c1 := collref.New("db1", "c1")
		status := jobstate.BackupStatus{Colls: map[collref.Name]jobstate.BackupCollStatus{
			c1: ddlOnly(jobstate.BackupCollDDLStateExecuting),
		}}
		latest := &atomic.Value{}
		latest.Store(status)
		reconcileBackupCollBars(p, latest, bars, status, 4)
		require.Len(t, bars, 1)

		status.Colls[c1] = ddlOnly(jobstate.BackupCollDDLStateSuccess)
		reconcileBackupCollBars(p, latest, bars, status, 4)
		assert.Empty(t, bars)
	})
}

func TestRenderBackup(t *testing.T) {
	newProgress := func(t *testing.T) *mpb.Progress {
		p := mpb.New(mpb.WithOutput(io.Discard))
		t.Cleanup(p.Shutdown)
		return p
	}

	t.Run("SuccessSettlesFromSnapshots", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker, err := store.AddBackupTask("t1", "backup1")
		require.NoError(t, err)

		p := newProgress(t)
		type outcome struct {
			status jobstate.BackupStatus
			err    error
		}
		done := make(chan outcome, 1)
		go func() {
			status, err := renderBackup(context.Background(), p, store, "t1", time.Millisecond, 4)
			done <- outcome{status, err}
		}()

		// Drive the job through its phases to success; the renderer polls at
		// 1ms, so it observes whichever of these states it lands on and must
		// still converge on the terminal snapshot.
		tracker.SetDatabaseExecuting()
		tracker.SetCollectionExecuting(jobstate.BackupUnitBytes)
		collRef := collref.New("db1", "coll1")
		ddl := tracker.NewCollDDLTracker(collRef)
		dml := tracker.NewCollDMLTracker(collRef)
		ddl.SetExecuting()
		ddl.SetSuccess()
		dml.SetFlushing()
		dml.SetPrepare()
		dml.SetExecuting(1 << 20)
		dml.AddCopiedSize(1 << 20)
		dml.SetSuccess()
		tracker.SetSuccess()

		select {
		case got := <-done:
			require.NoError(t, got.err)
			assert.Equal(t, jobstate.BackupStateSuccess, got.status.State)
			assert.Equal(t, collStageSettled, backupCollStageOf(got.status.Colls[collRef]))
		case <-time.After(10 * time.Second):
			t.Fatal("renderBackup did not settle")
		}
	})

	t.Run("FailureCarriesMessage", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker, err := store.AddBackupTask("t2", "backup1")
		require.NoError(t, err)
		tracker.SetDatabaseExecuting()
		tracker.SetFail(errors.New("boom"))

		status, err := renderBackup(context.Background(), newProgress(t), store, "t2", time.Millisecond, 4)
		require.NoError(t, err)
		assert.Equal(t, jobstate.BackupStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
	})

	t.Run("NoBarsBeforeFirstPoll", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker, err := store.AddBackupTask("t3", "backup1")
		require.NoError(t, err)
		tracker.SetFail(errors.New("boom"))

		// The job settled before the renderer's first poll, so no bar is
		// ever created; the renderer just answers with the terminal status.
		status, err := renderBackup(context.Background(), newProgress(t), store, "t3", time.Millisecond, 4)
		require.NoError(t, err)
		assert.Equal(t, jobstate.BackupStateFail, status.State)
	})

	t.Run("UnknownTaskID", func(t *testing.T) {
		store := jobstate.NewStore()

		_, err := renderBackup(context.Background(), newProgress(t), store, "nope", time.Millisecond, 4)
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})

	t.Run("ContextCancelAborts", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker, err := store.AddBackupTask("t4", "backup1")
		require.NoError(t, err)
		tracker.SetDatabaseExecuting()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = renderBackup(ctx, newProgress(t), store, "t4", time.Millisecond, 4)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// mpb only renders frames when auto-refresh is on (a terminal output implies
// it); with a plain buffer output and no auto-refresh nothing is ever
// written, which is exactly how a 0x0 pty swallowed every frame in the wild.
func TestRenderBackupEmitsFrames(t *testing.T) {
	var buf bytes.Buffer
	p := mpb.New(mpb.WithOutput(&buf), mpb.WithAutoRefresh())

	store := jobstate.NewStore()
	tracker, err := store.AddBackupTask("frames", "backup1")
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		_, err := renderBackup(context.Background(), p, store, "frames", 20*time.Millisecond, 4)
		result <- err
	}()

	tracker.SetDatabaseExecuting()
	tracker.SetCollectionExecuting(jobstate.BackupUnitBytes)
	collRef := collref.New("db1", "coll1")
	ddl := tracker.NewCollDDLTracker(collRef)
	dml := tracker.NewCollDMLTracker(collRef)
	ddl.SetExecuting()
	dml.SetPrepare()
	time.Sleep(50 * time.Millisecond)
	dml.SetExecuting(100 << 20)
	for i := 0; i < 10; i++ {
		dml.AddCopiedSize(10 << 20)
		time.Sleep(30 * time.Millisecond)
	}
	ddl.SetSuccess()
	dml.SetSuccess()
	tracker.SetSuccess()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("renderBackup did not settle")
	}

	// The render goroutine keeps writing to buf until the container shuts
	// down, so Shutdown (not defer) must precede any read of buf.
	p.Shutdown()

	assert.Contains(t, buf.String(), "db1.coll1")
	assert.Contains(t, buf.String(), "copying")
	assert.Contains(t, buf.String(), "MiB/s ETA")
}
