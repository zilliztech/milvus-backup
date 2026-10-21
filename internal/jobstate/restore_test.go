package jobstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/collref"
)

// ddlHalf answers the collection's DDL half, requiring it to be registered
// before the test reads into it.
func ddlHalf(t *testing.T, coll RestoreCollStatus) RestoreDDLStatus {
	t.Helper()
	require.NotNil(t, coll.DDL)
	return *coll.DDL
}

// onlyJob answers the collection's single job half, requiring the snapshot to
// hold exactly one job before the test reads into it.
func onlyJob(t *testing.T, coll RestoreCollStatus) RestoreJobStatus {
	t.Helper()
	require.Len(t, coll.Jobs, 1)
	return coll.Jobs[0]
}

// The snapshot keeps the halves as they are: a collection's state and percent
// are renderings over them, pinned at the transport edge (core/apiv1).
func TestRestoreCollTask_Snapshot(t *testing.T) {
	newCollTracker := func() *RestoreCollTracker {
		return NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 100)
	}

	t.Run("RegisteredOnlyHasNoHalves", func(t *testing.T) {
		snap := newCollTracker().coll.snapshot()
		assert.Nil(t, snap.DDL)
		assert.Empty(t, snap.Jobs)
		assert.Equal(t, int64(100), snap.TotalSize)
	})

	t.Run("MetaOnlyRegistersTheDDLHalf", func(t *testing.T) {
		coll := newCollTracker()
		ddl := coll.DDL()
		assert.Equal(t, RestoreDDLPending, ddlHalf(t, coll.coll.snapshot()).State)

		ddl.SetExecuting()
		assert.Equal(t, RestoreDDLExecuting, ddlHalf(t, coll.coll.snapshot()).State)

		ddl.SetSuccess()
		snap := coll.coll.snapshot()
		got := ddlHalf(t, snap)
		assert.Equal(t, RestoreDDLSuccess, got.State)
		assert.False(t, got.EndTime.IsZero())
	})

	t.Run("SnapshotShapeIsOneJob", func(t *testing.T) {
		coll := newCollTracker()
		job := coll.NewJob(100)
		assert.Equal(t, RestoreJobPending, onlyJob(t, coll.coll.snapshot()).State)

		job.Bind("job-1")
		job.SetProgress(40)
		got := onlyJob(t, coll.coll.snapshot())
		assert.Equal(t, RestoreJobRunning, got.State)
		assert.Equal(t, "job-1", got.ID)
		assert.Equal(t, int32(40), got.Progress)

		job.SetCompleted()
		assert.Equal(t, RestoreJobCompleted, onlyJob(t, coll.coll.snapshot()).State)
	})

	t.Run("HalvesKeepTheirOwnFailure", func(t *testing.T) {
		coll := newCollTracker()
		coll.DDL().SetFail(assert.AnError)
		assert.Equal(t, assert.AnError.Error(), ddlHalf(t, coll.coll.snapshot()).ErrorMessage)

		other := newCollTracker()
		other.NewJob(100).SetFailed(assert.AnError)
		assert.Equal(t, assert.AnError.Error(), onlyJob(t, other.coll.snapshot()).ErrorMessage)
	})
}

func TestRestoreTracker(t *testing.T) {
	t.Run("JobLifecycle", func(t *testing.T) {
		tracker := NewStore().AddRestoreTask("task-1")

		assert.Equal(t, RestoreStateInitial, tracker.Snapshot().State)

		tracker.SetExecuting()
		assert.Equal(t, RestoreStateExecuting, tracker.Snapshot().State)
		assert.False(t, tracker.Snapshot().Terminal())

		tracker.SetFail(assert.AnError)
		snap := tracker.Snapshot()
		assert.Equal(t, RestoreStateFail, snap.State)
		assert.Equal(t, assert.AnError.Error(), snap.ErrorMessage)
		assert.True(t, snap.Terminal())
		assert.False(t, snap.EndTime.IsZero())
	})

	t.Run("SnapshotIsDeadData", func(t *testing.T) {
		tracker := NewStore().AddRestoreTask("task-1")
		coll := tracker.NewCollTracker(collref.New("db1", "coll1"), 100)
		job := coll.NewJob(100)
		job.Bind("job-1")

		snap := tracker.Snapshot()
		snap.State = RestoreStateFail
		snap.Colls[collref.New("db1", "coll1")] = RestoreCollStatus{}

		fresh := tracker.Snapshot()
		assert.Equal(t, RestoreStateInitial, fresh.State)
		assert.Equal(t, RestoreJobRunning, onlyJob(t, fresh.Colls[collref.New("db1", "coll1")]).State)
	})

	t.Run("DDLHandleMintsOnce", func(t *testing.T) {
		coll := NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 100)
		coll.DDL().SetExecuting()
		// a second handle to the same half writes the same node
		coll.DDL().SetSuccess()

		snap := coll.coll.snapshot()
		assert.Equal(t, RestoreDDLSuccess, ddlHalf(t, snap).State)
	})
}

func TestStore_GetRestoreTask(t *testing.T) {
	store := NewStore()

	_, err := store.GetRestoreTask("unknown")
	assert.ErrorIs(t, err, ErrTaskNotFound)

	tracker := store.AddRestoreTask("task-1")
	tracker.NewCollTracker(collref.New("db1", "coll1"), 100).DDL().SetSuccess()

	snap, err := store.GetRestoreTask("task-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", snap.ID)
	require.Len(t, snap.Colls, 1)
	assert.Equal(t, RestoreDDLSuccess, ddlHalf(t, snap.Colls[collref.New("db1", "coll1")]).State)
}
