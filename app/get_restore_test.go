package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestGetRestoreExecute(t *testing.T) {
	t.Run("ReturnsSnapshotForKnownID", func(t *testing.T) {
		mgr := jobstate.NewStore()
		mgr.AddRestoreTask("task-1").SetExecuting()

		uc := NewGetRestore(mgr)
		status, err := uc.Execute(context.Background(), "task-1")

		require.NoError(t, err)
		assert.Equal(t, "task-1", status.ID)
		assert.Equal(t, jobstate.RestoreStateExecuting, status.State)
	})

	t.Run("ErrorsForUnknownID", func(t *testing.T) {
		// An empty manager: the process has restarted, every task is gone.
		uc := NewGetRestore(jobstate.NewStore())
		status, err := uc.Execute(context.Background(), "task-1")

		require.Error(t, err)
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
		assert.Contains(t, err.Error(), "task-1")
		assert.Equal(t, jobstate.RestoreStatus{}, status)
	})
}
