package jobstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/collref"
)

// The collection's state is derived from its halves: nothing has started is
// initial, a failed half fails it, every present half settled succeeds it,
// and anything else is in flight.
func TestRestoreCollTask_Derive(t *testing.T) {
	newCollTracker := func() *RestoreCollTracker {
		return NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 100)
	}

	t.Run("RegisteredOnlyIsInitial", func(t *testing.T) {
		coll := newCollTracker()
		assert.Equal(t, RestoreStateInitial, coll.coll.snapshot().State)
	})

	t.Run("MetaOnlyIsDDLOnly", func(t *testing.T) {
		coll := newCollTracker()
		ddl := coll.DDL()
		assert.Equal(t, RestoreStateInitial, coll.coll.snapshot().State)

		ddl.SetExecuting()
		assert.Equal(t, RestoreStateExecuting, coll.coll.snapshot().State)

		ddl.SetSuccess()
		snap := coll.coll.snapshot()
		assert.Equal(t, RestoreStateSuccess, snap.State)
		assert.Equal(t, int32(100), snap.Progress)
		assert.False(t, snap.EndTime.IsZero())
	})

	t.Run("SnapshotShapeIsOneJob", func(t *testing.T) {
		coll := newCollTracker()
		job := coll.NewJob(100)
		assert.Equal(t, RestoreStateInitial, coll.coll.snapshot().State)

		job.Bind("job-1")
		job.SetProgress(40)
		snap := coll.coll.snapshot()
		assert.Equal(t, RestoreStateExecuting, snap.State)
		assert.Equal(t, int32(40), snap.Progress)

		job.SetCompleted()
		snap = coll.coll.snapshot()
		assert.Equal(t, RestoreStateSuccess, snap.State)
		require.Len(t, snap.Jobs, 1)
		assert.Equal(t, "job-1", snap.Jobs[0].ID)
	})

	t.Run("DDLFailFailsCollection", func(t *testing.T) {
		coll := newCollTracker()
		coll.NewJob(100) // the import never runs once the schema half fails
		coll.DDL().SetExecuting()
		coll.DDL().SetFail(assert.AnError)

		snap := coll.coll.snapshot()
		assert.Equal(t, RestoreStateFail, snap.State)
		assert.Equal(t, assert.AnError.Error(), snap.ErrorMessage)
	})

	t.Run("JobFailFailsCollection", func(t *testing.T) {
		coll := newCollTracker()
		coll.DDL().SetSuccess()
		ok := coll.NewJob(50)
		bad := coll.NewJob(50)
		ok.Bind("ok")
		ok.SetCompleted()
		bad.Bind("bad")
		bad.SetFailed(assert.AnError)

		snap := coll.coll.snapshot()
		assert.Equal(t, RestoreStateFail, snap.State)
		assert.Equal(t, assert.AnError.Error(), snap.ErrorMessage)
		assert.False(t, snap.EndTime.IsZero())
	})
}

func TestRestoreCollTask_DeriveProgress(t *testing.T) {
	t.Run("WeightedByJobSize", func(t *testing.T) {
		coll := NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 100)
		for i, p := range []int32{100, 50, 0} {
			job := coll.NewJob([]int64{10, 20, 30}[i])
			job.Bind("job")
			job.SetProgress(p)
		}

		assert.Equal(t, int32(20), coll.coll.snapshot().Progress)
	})

	t.Run("CompletedCountsFull", func(t *testing.T) {
		coll := NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 100)
		done := coll.NewJob(50)
		done.Bind("done")
		done.SetProgress(30)
		done.SetCompleted()
		coll.NewJob(50) // still queued, so the collection is not done

		// the last polled ratio does not matter once the job completed
		assert.Equal(t, int32(50), coll.coll.snapshot().Progress)
	})

	t.Run("ZeroTotalSize", func(t *testing.T) {
		coll := NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 0)
		coll.NewJob(0).Bind("job")

		assert.Equal(t, int32(1), coll.coll.snapshot().Progress)
	})

	t.Run("ZeroRestoredSizeStillReportsOne", func(t *testing.T) {
		coll := NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 100)
		coll.NewJob(10).Bind("job")

		assert.Equal(t, int32(1), coll.coll.snapshot().Progress)
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

	t.Run("ProgressWeightsCollections", func(t *testing.T) {
		tracker := NewStore().AddRestoreTask("task-1")
		names := []string{"coll1", "coll2", "coll3"}
		sizes := []int64{10, 20, 30}
		for i, p := range []int32{100, 50, 0} {
			job := tracker.NewCollTracker(collref.New("db1", names[i]), sizes[i]).NewJob(sizes[i])
			job.Bind("job")
			job.SetProgress(p)
		}

		// each collection's job is its whole size, so the collection percents
		// are 100, 50 and 1 (the floor), weighting to (10+10+0)/60
		assert.Equal(t, int32(33), tracker.Snapshot().Progress)
	})

	t.Run("SuccessIsFullProgress", func(t *testing.T) {
		tracker := NewStore().AddRestoreTask("task-1")
		tracker.SetSuccess()

		assert.Equal(t, int32(100), tracker.Snapshot().Progress)
	})

	t.Run("NoCollectionsReportsOne", func(t *testing.T) {
		tracker := NewStore().AddRestoreTask("task-1")
		tracker.SetExecuting()

		assert.Equal(t, int32(1), tracker.Snapshot().Progress)
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
		assert.Equal(t, RestoreStateExecuting, fresh.Colls[collref.New("db1", "coll1")].State)
	})

	t.Run("DDLHandleMintsOnce", func(t *testing.T) {
		coll := NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db1", "coll1"), 100)
		coll.DDL().SetExecuting()
		// a second handle to the same half writes the same node
		coll.DDL().SetSuccess()

		snap := coll.coll.snapshot()
		require.NotNil(t, snap.DDL)
		assert.Equal(t, RestoreDDLSuccess, snap.DDL.State)
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
	assert.Equal(t, RestoreStateSuccess, snap.Colls[collref.New("db1", "coll1")].State)
}
