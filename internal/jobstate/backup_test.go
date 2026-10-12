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

		collRef := collref.New("db1", "coll1")
		colls := tracker.StartCollectionPhase([]collref.Name{collRef})
		coll := colls[collRef]
		coll.SetDMLExecuting(100)
		coll.AddCopiedSize(40)

		status := tracker.Snapshot()
		assert.Equal(t, BackupStateCollectionExecuting, status.State)
		assert.Equal(t, "task-1", status.ID)
		assert.Equal(t, "backup1", status.Name)
		assert.False(t, status.StartTime.IsZero())
		assert.False(t, status.Terminal())

		collStatus := status.Colls[collRef]
		assert.Equal(t, BackupCollStateDMLExecuting, collStatus.State)
		assert.Equal(t, int64(100), collStatus.TotalSize)
		assert.Equal(t, int64(40), collStatus.CopiedSize)

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
		colls := tracker.StartCollectionPhase([]collref.Name{collRef})

		before := tracker.Snapshot()
		colls[collRef].SetDMLExecuting(100)
		colls[collRef].AddCopiedSize(40)

		// The snapshot taken before the writes still shows the old state:
		// readers hold a copy, not a view into the task.
		assert.Equal(t, BackupCollStatePending, before.Colls[collRef].State)
		assert.Zero(t, before.Colls[collRef].CopiedSize)
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
		colls := tracker.StartCollectionPhase([]collref.Name{collRef})
		colls[collRef].SetFail(errors.New("copy boom"))

		coll := tracker.Snapshot().Colls[collRef]
		assert.Equal(t, BackupCollStateFail, coll.State)
		assert.Equal(t, "copy boom", coll.ErrorMessage)
		assert.False(t, coll.EndTime.IsZero())
	})
}
