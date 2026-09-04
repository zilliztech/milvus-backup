package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// expectMetaExist teaches the mock client whether the backup dir has a meta
// at all: the exist check lists the backup_meta.json key.
func expectMetaExist(t *testing.T, cli *storage.MockClient, backupDir string, exist bool) {
	t.Helper()

	var iter *storage.MockObjectIterator
	if exist {
		iter = storage.NewMockObjectIterator([]storage.ObjectAttr{
			{Key: backupDir + "/meta/backup_meta.json"},
		})
	} else {
		iter = storage.NewMockObjectIterator(nil)
	}
	cli.EXPECT().ListPrefix(mock.Anything, backupDir+"/meta/backup_meta.json", false).Return(iter, nil)
}

// expectMetaSize teaches the mock client to answer the meta dir size probe.
func expectMetaSize(t *testing.T, cli *storage.MockClient, backupDir string, size int64) {
	t.Helper()

	iter := storage.NewMockObjectIterator([]storage.ObjectAttr{
		{Key: backupDir + "/meta/full_meta.json", Length: size},
	})
	cli.EXPECT().ListPrefix(mock.Anything, backupDir+"/meta/", true).Return(iter, nil)
}

// expectReadableBackup teaches the mock client the whole read of one backup:
// meta exists, meta is readable, meta dir has a size.
func expectReadableBackup(t *testing.T, cli *storage.MockClient, rootPath, name string, size int64) {
	t.Helper()

	backupDir := rootPath + "/" + name
	expectMetaExist(t, cli, backupDir, true)
	expectFullMeta(t, cli, backupDir, &backuppb.BackupInfo{Name: name, Size: size})
	expectMetaSize(t, cli, backupDir, size)
}

func TestGetBackupExecute(t *testing.T) {
	t.Run("ByNameReadsMetaWithoutTask", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectReadableBackup(t, cli, "root", "backup1", 100)

		// An empty manager: the creating process has restarted, no task is
		// known. The artifact must still be answered in full.
		uc := &GetBackup{cli: cli, taskMgr: taskmgr.NewMgr(), rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1"})

		require.NoError(t, err)
		assert.Nil(t, view.Task)
		require.NotNil(t, view.Meta)
		assert.Equal(t, "backup1", view.Meta.GetName())
		assert.Equal(t, int64(100), view.MetaSize)
	})

	t.Run("ByNameOverlaysTaskOnMeta", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectReadableBackup(t, cli, "root", "backup1", 100)

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))
		mgr.UpdateBackupTask("task-1", taskmgr.SetBackupSuccess())

		uc := &GetBackup{cli: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1"})

		require.NoError(t, err)
		require.NotNil(t, view.Task)
		assert.Equal(t, "task-1", view.Task.ID())
		assert.Equal(t, "backup1", view.Task.Name())
		require.NotNil(t, view.Meta)
		assert.Equal(t, int64(100), view.MetaSize)
	})

	t.Run("ByNameErrorsWhenNeitherTaskNorMetaExists", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectMetaExist(t, cli, "root/backup1", false)

		uc := &GetBackup{cli: cli, taskMgr: taskmgr.NewMgr(), rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1"})

		assert.Nil(t, view)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("ByNameInFlightTaskWithoutMetaReturnsTaskOnly", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectMetaExist(t, cli, "root/backup1", false)

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))
		mgr.UpdateBackupTask("task-1", taskmgr.SetBackupDatabaseExecuting())

		uc := &GetBackup{cli: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1"})

		require.NoError(t, err)
		require.NotNil(t, view.Task)
		assert.Nil(t, view.Meta)
		assert.Zero(t, view.MetaSize)
	})

	t.Run("ByNameFailTaskWithoutMetaReturnsTaskOnly", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectMetaExist(t, cli, "root/backup1", false)

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))
		mgr.UpdateBackupTask("task-1", taskmgr.SetBackupFail(errors.New("milvus down")))

		uc := &GetBackup{cli: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1"})

		require.NoError(t, err)
		require.NotNil(t, view.Task)
		assert.Contains(t, view.Task.ErrorMessage(), "milvus down")
		assert.Nil(t, view.Meta)
	})

	t.Run("ByNameSuccessTaskWithoutMetaErrors", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectMetaExist(t, cli, "root/backup1", false)

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))
		mgr.UpdateBackupTask("task-1", taskmgr.SetBackupSuccess())

		uc := &GetBackup{cli: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1"})

		assert.Nil(t, view)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "meta is missing")
	})

	t.Run("ByIDResolvesNameThroughTask", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectReadableBackup(t, cli, "root", "backup1", 100)

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))
		mgr.UpdateBackupTask("task-1", taskmgr.SetBackupSuccess())

		uc := &GetBackup{cli: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{ID: "task-1"})

		require.NoError(t, err)
		require.NotNil(t, view.Task)
		assert.Equal(t, "task-1", view.Task.ID())
		assert.Equal(t, "backup1", view.Task.Name())
		require.NotNil(t, view.Meta)
	})

	t.Run("ByIDErrorsWhenTaskUnknown", func(t *testing.T) {
		// An ID cannot be resolved without the task that owns it, so an
		// unknown ID has nothing to fall back to.
		uc := &GetBackup{cli: storage.NewMockClient(t), taskMgr: taskmgr.NewMgr(), rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{ID: "task-1"})

		assert.Nil(t, view)
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	})

	t.Run("IDWinsOverName", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectReadableBackup(t, cli, "root", "backup1", 100)

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))
		mgr.UpdateBackupTask("task-1", taskmgr.SetBackupSuccess())

		// No mock for "backup2": if the name were consulted the test would
		// fail on an unexpected call instead.
		uc := &GetBackup{cli: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{ID: "task-1", Name: "backup2"})

		require.NoError(t, err)
		require.NotNil(t, view.Task)
		assert.Equal(t, "task-1", view.Task.ID())
	})

	t.Run("RejectsEmptyNameAndID", func(t *testing.T) {
		uc := &GetBackup{cli: storage.NewMockClient(t), taskMgr: taskmgr.NewMgr(), rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{})

		assert.Nil(t, view)
		assert.Error(t, err)
	})

	t.Run("PathOverridesRootPath", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectReadableBackup(t, cli, "other", "backup1", 100)

		uc := &GetBackup{cli: cli, taskMgr: taskmgr.NewMgr(), rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1", Path: "other"})

		require.NoError(t, err)
		require.NotNil(t, view.Meta)
	})

	t.Run("FailsWhenExistCheckFails", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		cli.EXPECT().
			ListPrefix(mock.Anything, "root/backup1/meta/backup_meta.json", false).
			Return(nil, errors.New("connection closed"))

		uc := &GetBackup{cli: cli, taskMgr: taskmgr.NewMgr(), rootPath: "root"}
		view, err := uc.Execute(context.Background(), GetBackupRequest{Name: "backup1"})

		assert.Nil(t, view)
		assert.Error(t, err)
	})
}
