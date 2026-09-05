package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func TestGetRestoreExecute(t *testing.T) {
	t.Run("ReturnsTaskViewForKnownID", func(t *testing.T) {
		mgr := taskmgr.NewMgr()
		mgr.AddRestoreTask("task-1")
		mgr.UpdateRestoreTask("task-1", taskmgr.SetRestoreExecuting())

		uc := &GetRestore{taskMgr: mgr}
		view, err := uc.Execute(context.Background(), "task-1")

		require.NoError(t, err)
		assert.Equal(t, "task-1", view.Task.ID())
		assert.Equal(t, backuppb.RestoreTaskStateCode_EXECUTING, view.Task.StateCode())
	})

	t.Run("ErrorsForUnknownID", func(t *testing.T) {
		// An empty manager: the process has restarted, every task is gone.
		uc := &GetRestore{taskMgr: taskmgr.NewMgr()}
		view, err := uc.Execute(context.Background(), "task-1")

		require.Error(t, err)
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
		assert.Contains(t, err.Error(), "task-1")
		assert.Nil(t, view.Task)
	})
}
