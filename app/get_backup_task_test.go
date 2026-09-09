package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func TestGetBackupTaskExecute(t *testing.T) {
	t.Run("ByName", func(t *testing.T) {
		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))

		uc := NewGetBackupTask(mgr)
		task, err := uc.Execute(context.Background(), GetBackupTaskRequest{Name: "backup1"})

		require.NoError(t, err)
		assert.Equal(t, "task-1", task.ID())
		assert.Equal(t, "backup1", task.Name())
	})

	t.Run("ByID", func(t *testing.T) {
		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))

		uc := NewGetBackupTask(mgr)
		task, err := uc.Execute(context.Background(), GetBackupTaskRequest{ID: "task-1"})

		require.NoError(t, err)
		assert.Equal(t, "task-1", task.ID())
	})

	t.Run("IDWinsOverName", func(t *testing.T) {
		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))
		require.NoError(t, mgr.AddBackupTask("task-2", "backup2"))

		uc := NewGetBackupTask(mgr)
		task, err := uc.Execute(context.Background(), GetBackupTaskRequest{ID: "task-1", Name: "backup2"})

		require.NoError(t, err)
		assert.Equal(t, "task-1", task.ID())
		assert.Equal(t, "backup1", task.Name())
	})

	t.Run("UnknownNameIsTaskNotFound", func(t *testing.T) {
		uc := NewGetBackupTask(taskmgr.NewMgr())
		task, err := uc.Execute(context.Background(), GetBackupTaskRequest{Name: "backup1"})

		assert.Nil(t, task)
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	})

	t.Run("UnknownIDIsTaskNotFound", func(t *testing.T) {
		uc := NewGetBackupTask(taskmgr.NewMgr())
		task, err := uc.Execute(context.Background(), GetBackupTaskRequest{ID: "task-1"})

		assert.Nil(t, task)
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	})

	t.Run("RejectsEmptyNameAndID", func(t *testing.T) {
		uc := NewGetBackupTask(taskmgr.NewMgr())
		task, err := uc.Execute(context.Background(), GetBackupTaskRequest{})

		assert.Nil(t, task)
		assert.Error(t, err)
	})
}
