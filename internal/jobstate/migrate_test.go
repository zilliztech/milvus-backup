package jobstate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateTask_Lifecycle(t *testing.T) {
	store := NewStore()
	tracker := store.AddMigrateTask("task-1", 1<<30)

	status, err := store.GetMigrateTask("task-1")
	require.NoError(t, err)
	assert.Equal(t, MigrateStateInitial, status.State)
	assert.False(t, status.Terminal())
	assert.True(t, status.EndTime.IsZero())

	tracker.SetRunning()

	status, err = store.GetMigrateTask("task-1")
	require.NoError(t, err)
	assert.Equal(t, MigrateStateRunning, status.State)
	assert.False(t, status.Terminal())
}

func TestMigrateTask_Settle(t *testing.T) {
	t.Run("FailRecordsMessageAndEndTime", func(t *testing.T) {
		store := NewStore()
		tracker := store.AddMigrateTask("task-1", 1<<30)
		tracker.SetRunning()

		tracker.SetFail(errors.New("boom"))

		status, err := store.GetMigrateTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, MigrateStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
		assert.False(t, status.EndTime.IsZero())
		assert.True(t, status.Terminal())
	})

	t.Run("SuccessRecordsEndTime", func(t *testing.T) {
		store := NewStore()
		tracker := store.AddMigrateTask("task-1", 1<<30)
		tracker.SetRunning()

		tracker.SetSuccess()

		status, err := store.GetMigrateTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, MigrateStateSuccess, status.State)
		assert.Empty(t, status.ErrorMessage)
		assert.False(t, status.EndTime.IsZero())
		assert.True(t, status.Terminal())
	})
}

func TestMigrateTask_TrackerWritesLandInSnapshot(t *testing.T) {
	store := NewStore()

	tracker := store.AddMigrateTask("task-1", 1<<30)

	tracker.SetCopyStart()
	tracker.IncCopied(100)
	tracker.IncCopied(200)

	status, err := store.GetMigrateTask("task-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", status.ID)
	assert.Equal(t, int64(1<<30), status.TotalSize)
	assert.Equal(t, int64(300), status.CopiedSize)
	assert.True(t, status.CopyStarted)
	assert.False(t, status.CopyDone)
	assert.False(t, status.StartTime.IsZero())
	assert.Empty(t, status.MigrateJobID)

	tracker.SetCopyDone()
	tracker.SetJobID("job-42")

	status, err = store.GetMigrateTask("task-1")
	require.NoError(t, err)
	assert.True(t, status.CopyDone)
	assert.Equal(t, "job-42", status.MigrateJobID)
}

func TestMigrateTask_SnapshotIsACopy(t *testing.T) {
	store := NewStore()

	tracker := store.AddMigrateTask("task-1", 1<<30)
	tracker.SetCopyStart()
	tracker.IncCopied(5)

	before, err := store.GetMigrateTask("task-1")
	require.NoError(t, err)

	tracker.IncCopied(5)

	// The earlier snapshot is dead data: writes after the read do not leak
	// into it.
	assert.Equal(t, int64(5), before.CopiedSize)
}

func TestGetMigrateTask_NotFound(t *testing.T) {
	store := NewStore()

	_, err := store.GetMigrateTask("nope")
	assert.ErrorIs(t, err, ErrTaskNotFound)
}
