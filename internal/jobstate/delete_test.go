package jobstate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteTask_TrackerWritesLandInSnapshot(t *testing.T) {
	store := NewStore()

	tracker, err := store.AddDeleteTask("task-1", "backup-1")
	require.NoError(t, err)

	tracker.SetRunning()
	tracker.AddDiscovered(10)
	tracker.AddDeleted(1)
	tracker.AddDeleted(1)

	status, err := store.GetDeleteTask("task-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", status.ID)
	assert.Equal(t, "backup-1", status.Name)
	assert.Equal(t, DeleteStateRunning, status.State)
	assert.Equal(t, int64(10), status.Discovered)
	assert.Equal(t, int64(2), status.Deleted)
	assert.False(t, status.ListingDone)
	assert.False(t, status.Terminal())
	assert.False(t, status.StartTime.IsZero())
	assert.True(t, status.EndTime.IsZero())

	tracker.SetListingDone()
	tracker.SetSuccess()

	status, err = store.GetDeleteTask("task-1")
	require.NoError(t, err)
	assert.Equal(t, DeleteStateSuccess, status.State)
	assert.True(t, status.ListingDone)
	assert.True(t, status.Terminal())
	assert.False(t, status.EndTime.IsZero())
}

func TestDeleteTask_FailRecordsError(t *testing.T) {
	store := NewStore()

	tracker, err := store.AddDeleteTask("task-1", "backup-1")
	require.NoError(t, err)

	tracker.SetFail(errors.New("boom"))

	status, err := store.GetDeleteTask("task-1")
	require.NoError(t, err)
	assert.Equal(t, DeleteStateFail, status.State)
	assert.Equal(t, "boom", status.ErrorMessage)
	assert.True(t, status.Terminal())
}

func TestDeleteTask_SnapshotIsACopy(t *testing.T) {
	store := NewStore()

	tracker, err := store.AddDeleteTask("task-1", "backup-1")
	require.NoError(t, err)
	tracker.AddDiscovered(5)

	before, err := store.GetDeleteTask("task-1")
	require.NoError(t, err)

	tracker.AddDiscovered(5)

	// The earlier snapshot is dead data: writes after the read do not leak
	// into it.
	assert.Equal(t, int64(5), before.Discovered)
}

func TestAddDeleteTask_DuplicateName(t *testing.T) {
	store := NewStore()

	tracker, err := store.AddDeleteTask("task-1", "backup-1")
	require.NoError(t, err)

	// A second delete for the same name is refused while the first runs.
	_, err = store.AddDeleteTask("task-2", "backup-1")
	assert.ErrorContains(t, err, "backup-1")

	// A finished job does not block a retry.
	tracker.SetFail(errors.New("boom"))
	_, err = store.AddDeleteTask("task-2", "backup-1")
	assert.NoError(t, err)
}

func TestGetDeleteTask_NotFound(t *testing.T) {
	store := NewStore()

	_, err := store.GetDeleteTask("nope")
	assert.ErrorIs(t, err, ErrTaskNotFound)
}
