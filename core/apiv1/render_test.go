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
		State:        jobstate.RestoreStateFail,
		ErrorMessage: "error message",
		StartTime:    now,
		EndTime:      now,
		Progress:     42,
	}

	res := NewRestoreCollTaskResp(collRef, coll)
	assert.Equal(t, backuppb.RestoreTaskStateCode_FAIL, res.StateCode)
	assert.Equal(t, "error message", res.ErrorMessage)
	assert.Equal(t, now.Unix(), res.StartTime)
	assert.Equal(t, now.Unix(), res.EndTime)
	assert.Equal(t, int32(42), res.Progress)
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
		Progress:     42,
		Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "coll1"): {State: jobstate.RestoreStateInitial},
		},
	}

	res := NewRestoreTaskResp(status)
	assert.Equal(t, "id1", res.Id)
	assert.Equal(t, backuppb.RestoreTaskStateCode_EXECUTING, res.StateCode)
	assert.Equal(t, "error message", res.ErrorMessage)
	assert.Equal(t, now.Unix(), res.StartTime)
	assert.Equal(t, now.Unix(), res.EndTime)
	assert.Equal(t, int32(42), res.Progress)

	assert.Len(t, res.CollectionRestoreTasks, 1)
	assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, res.CollectionRestoreTasks[0].StateCode)
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
