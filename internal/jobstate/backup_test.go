package jobstate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/collref"
)

func TestBackupTracker(t *testing.T) {
	t.Run("WritesLandInTheSnapshot", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)

		tracker.SetDatabaseExecuting()
		assert.Equal(t, BackupStateDatabaseExecuting, tracker.Snapshot().State)

		tracker.SetCollectionExecuting()
		collRef := collref.New("db1", "coll1")
		coll := tracker.NewCollDMLTracker(collRef)
		coll.SetExecuting(100)
		coll.AddCopiedSize(40)

		status := tracker.Snapshot()
		assert.Equal(t, BackupStateCollectionExecuting, status.State)
		assert.Equal(t, "task-1", status.ID)
		assert.Equal(t, "backup1", status.Name)
		assert.False(t, status.StartTime.IsZero())
		assert.False(t, status.Terminal())

		collStatus := status.Colls[collRef]
		require.NotNil(t, collStatus.DML)
		assert.Equal(t, BackupCollDMLStateExecuting, collStatus.DML.State)
		assert.Equal(t, int64(100), collStatus.DML.TotalSize)
		assert.Equal(t, int64(40), collStatus.DML.CopiedSize)

		// The store reads back the same state the tracker's snapshot shows.
		stored, err := store.GetBackupTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, status, stored)
	})

	t.Run("SnapshotIsDeadData", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)

		collRef := collref.New("db1", "coll1")
		coll := tracker.NewCollDMLTracker(collRef)

		before := tracker.Snapshot()
		coll.SetExecuting(100)
		coll.AddCopiedSize(40)

		// The snapshot taken before the writes still shows the old state:
		// readers hold a copy, not a view into the task.
		require.NotNil(t, before.Colls[collRef].DML)
		assert.Equal(t, BackupCollDMLStatePending, before.Colls[collRef].DML.State)
		assert.Zero(t, before.Colls[collRef].DML.CopiedSize)
	})

	t.Run("SettleRecordsMessageAndEndTime", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)

		tracker.SetFail(errors.New("boom"))

		status := tracker.Snapshot()
		assert.Equal(t, BackupStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
		assert.False(t, status.EndTime.IsZero())
		assert.True(t, status.Terminal())
	})

	t.Run("FailedJobDoesNotBlockRetry", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)
		tracker.SetFail(errors.New("boom"))

		_, err = store.AddBackupTask("task-2", "backup1")
		assert.NoError(t, err)
	})

	t.Run("SettledCollRecordsEndTime", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)

		collRef := collref.New("db1", "coll1")
		tracker.NewCollDMLTracker(collRef).SetFail(errors.New("copy boom"))

		coll := tracker.Snapshot().Colls[collRef]
		require.NotNil(t, coll.DML)
		assert.Equal(t, BackupCollDMLStateFail, coll.DML.State)
		assert.Equal(t, "copy boom", coll.DML.ErrorMessage)
		assert.False(t, coll.DML.EndTime.IsZero())
	})

	t.Run("HalvesSettleIndependently", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)

		collRef := collref.New("db1", "coll1")
		ddl := tracker.NewCollDDLTracker(collRef)
		dml := tracker.NewCollDMLTracker(collRef)

		// Both halves live on the same collection entry and move on their own:
		// the schema half can settle while the data half has not started.
		ddl.SetExecuting()
		ddl.SetSuccess()

		coll := tracker.Snapshot().Colls[collRef]
		require.NotNil(t, coll.DDL)
		require.NotNil(t, coll.DML)
		assert.Equal(t, BackupCollDDLStateSuccess, coll.DDL.State)
		assert.False(t, coll.DDL.EndTime.IsZero())
		assert.Equal(t, BackupCollDMLStatePending, coll.DML.State)

		dml.SetFail(errors.New("copy boom"))
		coll = tracker.Snapshot().Colls[collRef]
		assert.Equal(t, BackupCollDDLStateSuccess, coll.DDL.State)
		assert.Equal(t, BackupCollDMLStateFail, coll.DML.State)
	})

	t.Run("NewTrackerForSameHalfSharesTheNode", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)

		collRef := collref.New("db1", "coll1")
		first := tracker.NewCollDMLTracker(collRef)
		first.SetExecuting(100)

		// A second handle for the same half writes the same node:
		// nothing is re-registered and earlier writes are still visible.
		second := tracker.NewCollDMLTracker(collRef)
		second.AddCopiedSize(40)

		status := tracker.Snapshot()
		require.Len(t, status.Colls, 1)
		coll := status.Colls[collRef]
		require.NotNil(t, coll.DML)
		assert.Equal(t, BackupCollDMLStateExecuting, coll.DML.State)
		assert.Equal(t, int64(100), coll.DML.TotalSize)
		assert.Equal(t, int64(40), coll.DML.CopiedSize)
	})

	t.Run("UnregisteredHalfStaysAbsent", func(t *testing.T) {
		store := NewStore()
		tracker, err := store.AddBackupTask("task-1", "backup1")
		require.NoError(t, err)

		// A meta_only job creates only schema halves: the data half is absent
		// from the snapshot, not pending.
		collRef := collref.New("db1", "coll1")
		tracker.NewCollDDLTracker(collRef).SetSuccess()

		coll := tracker.Snapshot().Colls[collRef]
		require.NotNil(t, coll.DDL)
		assert.Equal(t, BackupCollDDLStateSuccess, coll.DDL.State)
		assert.Nil(t, coll.DML)
	})
}
