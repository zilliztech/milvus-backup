package apiv1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestNewRestoreCollTaskResp(t *testing.T) {
	now := time.Now()
	collRef := collref.New("db1", "coll1")
	coll := jobstate.RestoreCollStatus{
		StartTime: now,
		TotalSize: 100,
		DDL:       &jobstate.RestoreDDLStatus{State: jobstate.RestoreDDLSuccess, EndTime: now},
	}

	res := NewRestoreCollTaskResp(collRef, coll)
	assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, res.StateCode)
	assert.Empty(t, res.ErrorMessage)
	assert.Equal(t, now.Unix(), res.StartTime)
	assert.Equal(t, now.Unix(), res.EndTime)
	assert.Equal(t, int32(100), res.Progress)
	assert.Equal(t, "db1", res.TargetDbName)
	assert.Equal(t, "coll1", res.TargetCollectionName)
}

func TestNewRestoreTaskResp(t *testing.T) {
	now := time.Now()

	status := jobstate.RestoreStatus{
		ID:           "id1",
		State:        jobstate.RestoreStateExecuting,
		ErrorMessage: "error message",
		StartTime:    now,
		EndTime:      now,
		Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "coll1"): {StartTime: now},
		},
	}

	res := NewRestoreTaskResp(status)
	assert.Equal(t, "id1", res.Id)
	assert.Equal(t, backuppb.RestoreTaskStateCode_EXECUTING, res.StateCode)
	assert.Equal(t, "error message", res.ErrorMessage)
	assert.Equal(t, now.Unix(), res.StartTime)
	assert.Equal(t, now.Unix(), res.EndTime)
	// the collection has no halves and no size, so the percent floors at 1
	assert.Equal(t, int32(1), res.Progress)

	assert.Len(t, res.CollectionRestoreTasks, 1)
	assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, res.CollectionRestoreTasks[0].StateCode)
}

// The collection's state is derived from its halves: nothing has started is
// initial, a failed half fails it, every present half settled succeeds it,
// and anything else is in flight.
func TestRestoreCollState(t *testing.T) {
	t.Run("NoHalvesIsInitial", func(t *testing.T) {
		state, _, endTime := restoreCollState(jobstate.RestoreCollStatus{})
		assert.Equal(t, jobstate.RestoreStateInitial, state)
		assert.True(t, endTime.IsZero())
	})

	t.Run("DDLOnlySettles", func(t *testing.T) {
		end := time.Unix(1, 0)
		state, _, endTime := restoreCollState(jobstate.RestoreCollStatus{
			DDL: &jobstate.RestoreDDLStatus{State: jobstate.RestoreDDLSuccess, EndTime: end},
		})
		assert.Equal(t, jobstate.RestoreStateSuccess, state)
		assert.Equal(t, end, endTime)
	})

	t.Run("OneJobInFlight", func(t *testing.T) {
		state, _, endTime := restoreCollState(jobstate.RestoreCollStatus{
			Jobs: []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning, Progress: 40}},
		})
		assert.Equal(t, jobstate.RestoreStateExecuting, state)
		assert.True(t, endTime.IsZero())
	})

	t.Run("DDLFailWinsOverJobFailure", func(t *testing.T) {
		state, errMessage, _ := restoreCollState(jobstate.RestoreCollStatus{
			DDL:  &jobstate.RestoreDDLStatus{State: jobstate.RestoreDDLFail, ErrorMessage: "ddl failed"},
			Jobs: []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobFailed, ErrorMessage: "job failed"}},
		})
		assert.Equal(t, jobstate.RestoreStateFail, state)
		assert.Equal(t, "ddl failed", errMessage)
	})

	t.Run("JobFailFailsCollection", func(t *testing.T) {
		end := time.Unix(2, 0)
		state, errMessage, endTime := restoreCollState(jobstate.RestoreCollStatus{
			DDL: &jobstate.RestoreDDLStatus{State: jobstate.RestoreDDLSuccess},
			Jobs: []jobstate.RestoreJobStatus{
				{State: jobstate.RestoreJobCompleted},
				{State: jobstate.RestoreJobFailed, ErrorMessage: assert.AnError.Error(), EndTime: end},
			},
		})
		assert.Equal(t, jobstate.RestoreStateFail, state)
		assert.Equal(t, assert.AnError.Error(), errMessage)
		assert.Equal(t, end, endTime)
	})
}

func TestRestoreCollProgress(t *testing.T) {
	t.Run("WeightedByJobSize", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			TotalSize: 100,
			Jobs: []jobstate.RestoreJobStatus{
				{State: jobstate.RestoreJobRunning, Size: 10, Progress: 100},
				{State: jobstate.RestoreJobRunning, Size: 20, Progress: 50},
				{State: jobstate.RestoreJobRunning, Size: 30, Progress: 0},
			},
		}

		assert.Equal(t, int32(20), restoreCollProgress(coll))
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

		assert.Equal(t, int32(50), restoreCollProgress(coll))
	})

	t.Run("ZeroTotalSize", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			Jobs: []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning}},
		}

		assert.Equal(t, int32(1), restoreCollProgress(coll))
	})

	t.Run("ZeroRestoredSizeStillReportsOne", func(t *testing.T) {
		coll := jobstate.RestoreCollStatus{
			TotalSize: 100,
			Jobs:      []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning, Size: 10}},
		}

		assert.Equal(t, int32(1), restoreCollProgress(coll))
	})
}

func TestRestoreProgress(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		assert.Equal(t, int32(100), restoreProgress(jobstate.RestoreStatus{State: jobstate.RestoreStateSuccess}))
	})

	t.Run("WeightedCollections", func(t *testing.T) {
		status := jobstate.RestoreStatus{
			State: jobstate.RestoreStateExecuting,
			Colls: map[collref.Name]jobstate.RestoreCollStatus{
				collref.New("db1", "coll1"): {
					TotalSize: 10,
					Jobs:      []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning, Size: 10, Progress: 100}},
				},
				collref.New("db1", "coll2"): {
					TotalSize: 20,
					Jobs:      []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning, Size: 20, Progress: 50}},
				},
				collref.New("db1", "coll3"): {
					TotalSize: 30,
					Jobs:      []jobstate.RestoreJobStatus{{State: jobstate.RestoreJobRunning, Size: 30, Progress: 0}},
				},
			},
		}

		// each collection's job is its whole size, so the collection percents
		// are 100, 50 and 1 (the floor), weighting to (10+10+0)/60
		assert.Equal(t, int32(33), restoreProgress(status))
	})

	t.Run("NoCollectionsReportsOne", func(t *testing.T) {
		assert.Equal(t, int32(1), restoreProgress(jobstate.RestoreStatus{State: jobstate.RestoreStateExecuting}))
	})
}

func TestRestoreStateToPB(t *testing.T) {
	assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, restoreStateToPB(jobstate.RestoreStateInitial))
	assert.Equal(t, backuppb.RestoreTaskStateCode_EXECUTING, restoreStateToPB(jobstate.RestoreStateExecuting))
	assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, restoreStateToPB(jobstate.RestoreStateSuccess))
	assert.Equal(t, backuppb.RestoreTaskStateCode_FAIL, restoreStateToPB(jobstate.RestoreStateFail))
}

// The progress mapping is the v1 wire's historical contract; these cases pin
// the numbers the old view interface used to compute.
func TestBackupStateToPB(t *testing.T) {
	assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_INITIAL, backupStateToPB(jobstate.BackupStateInitial))
	assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_EXECUTING, backupStateToPB(jobstate.BackupStateDatabaseExecuting))
	assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_EXECUTING, backupStateToPB(jobstate.BackupStateCollectionExecuting))
	assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_SUCCESS, backupStateToPB(jobstate.BackupStateSuccess))
	assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_FAIL, backupStateToPB(jobstate.BackupStateFail))
}

func TestBackupCollProgress(t *testing.T) {
	t.Run("DDLHalfOnly", func(t *testing.T) {
		// A meta_only collection has no data half: the schema half is the
		// whole story, and its success reads as done.
		assert.Equal(t, int32(1), backupCollProgress(jobstate.BackupCollStatus{
			DDL: &jobstate.BackupCollDDLStatus{State: jobstate.BackupCollDDLStatePending}}))
		assert.Equal(t, int32(5), backupCollProgress(jobstate.BackupCollStatus{
			DDL: &jobstate.BackupCollDDLStatus{State: jobstate.BackupCollDDLStateExecuting}}))
		assert.Equal(t, int32(100), backupCollProgress(jobstate.BackupCollStatus{
			DDL: &jobstate.BackupCollDDLStatus{State: jobstate.BackupCollDDLStateSuccess}}))
		assert.Equal(t, int32(0), backupCollProgress(jobstate.BackupCollStatus{
			DDL: &jobstate.BackupCollDDLStatus{State: jobstate.BackupCollDDLStateFail}}))
	})

	t.Run("DMLHalfDrives", func(t *testing.T) {
		assert.Equal(t, int32(10), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStatePending}}))
		assert.Equal(t, int32(15), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStatePrepare}}))
		assert.Equal(t, int32(100), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateSuccess}}))
		assert.Equal(t, int32(0), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateFail}}))
	})

	t.Run("FailOnEitherHalfZeroes", func(t *testing.T) {
		assert.Equal(t, int32(0), backupCollProgress(jobstate.BackupCollStatus{
			DDL: &jobstate.BackupCollDDLStatus{State: jobstate.BackupCollDDLStateFail},
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStatePending}}))
		assert.Equal(t, int32(0), backupCollProgress(jobstate.BackupCollStatus{
			DDL: &jobstate.BackupCollDDLStatus{State: jobstate.BackupCollDDLStateSuccess},
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateFail}}))
	})

	t.Run("MappedProgress", func(t *testing.T) {
		assert.Equal(t, int32(15), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateExecuting, TotalSize: 100, CopiedSize: 0}}))
		assert.Equal(t, int32(57), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateExecuting, TotalSize: 100, CopiedSize: 50}}))
		assert.Equal(t, int32(100), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateExecuting, TotalSize: 100, CopiedSize: 100}}))
		// total size is 0, so we just return 15
		assert.Equal(t, int32(15), backupCollProgress(jobstate.BackupCollStatus{
			DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateExecuting, TotalSize: 0, CopiedSize: 100}}))
	})
}

func TestBackupProgress(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		assert.Equal(t, int32(100), backupProgress(&jobstate.BackupStatus{State: jobstate.BackupStateSuccess}))
	})

	t.Run("DatabaseExecuting", func(t *testing.T) {
		assert.Equal(t, int32(5), backupProgress(&jobstate.BackupStatus{State: jobstate.BackupStateDatabaseExecuting}))
	})

	t.Run("CollectionExecuting", func(t *testing.T) {
		status := &jobstate.BackupStatus{
			State: jobstate.BackupStateCollectionExecuting,
			Colls: map[collref.Name]jobstate.BackupCollStatus{
				collref.New("db1", "coll1"): {
					DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateExecuting, TotalSize: 100, CopiedSize: 50}},
				collref.New("db1", "coll2"): {
					DML: &jobstate.BackupCollDMLStatus{State: jobstate.BackupCollDMLStateSuccess}},
			},
		}

		assert.Equal(t, int32(79), backupProgress(status))
	})

	t.Run("CollectionExecutingWithoutCollections", func(t *testing.T) {
		status := &jobstate.BackupStatus{State: jobstate.BackupStateCollectionExecuting}
		assert.Equal(t, int32(5), backupProgress(status))
	})
}
