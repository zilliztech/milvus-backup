package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// expectClientConfigs teaches both mock clients the config probe NewTask does
// to resolve the transfer mode.
func expectClientConfigs(t *testing.T, milvusStorage, backupStorage *storage.MockClient) {
	t.Helper()

	milvusStorage.EXPECT().Config().Return(storage.Config{})
	backupStorage.EXPECT().Config().Return(storage.Config{})
}

// expectMetaSize teaches the mock client to answer the meta dir size probe.
func expectMetaSize(t *testing.T, cli *storage.MockClient, backupDir string, size int64) {
	t.Helper()

	iter := storage.NewMockObjectIterator([]storage.ObjectAttr{
		{Key: backupDir + "/meta/full_meta.json", Length: size},
	})
	cli.EXPECT().ListPrefix(mock.Anything, backupDir+"/meta/", true).Return(iter, nil)
}

func TestCreateBackupStart(t *testing.T) {
	t.Run("RegistersJobInTheTaskManager", func(t *testing.T) {
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		expectClientConfigs(t, milvusStorage, backupStorage)

		uc := &CreateBackup{
			params:        v2.New(),
			milvusStorage: milvusStorage,
			backupStorage: backupStorage,
			taskMgr:       taskmgr.NewMgr(),
			rootPath:      "root",
		}

		job, err := uc.Start(CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})

		require.NoError(t, err)
		require.NotNil(t, job)
		view, err := uc.taskMgr.GetBackupTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, "backup1", view.Name())
	})

	t.Run("RefusesSecondLiveJobWithSameName", func(t *testing.T) {
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		expectClientConfigs(t, milvusStorage, backupStorage)
		expectClientConfigs(t, milvusStorage, backupStorage)

		uc := &CreateBackup{
			params:        v2.New(),
			milvusStorage: milvusStorage,
			backupStorage: backupStorage,
			taskMgr:       taskmgr.NewMgr(),
			rootPath:      "root",
		}
		_, err := uc.Start(CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})
		require.NoError(t, err)

		job, err := uc.Start(CreateBackupRequest{TaskID: "task-2", Option: backup.Option{BackupName: "backup1"}})

		assert.Nil(t, job)
		assert.ErrorContains(t, err, "existing task")
	})
}

func TestCreateBackupExecute(t *testing.T) {
	t.Run("FailsWhenStartDoes", func(t *testing.T) {
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		expectClientConfigs(t, milvusStorage, backupStorage)
		expectClientConfigs(t, milvusStorage, backupStorage)

		uc := &CreateBackup{
			params:        v2.New(),
			milvusStorage: milvusStorage,
			backupStorage: backupStorage,
			taskMgr:       taskmgr.NewMgr(),
			rootPath:      "root",
		}
		_, err := uc.Start(CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})
		require.NoError(t, err)

		view, err := uc.Execute(context.Background(), CreateBackupRequest{TaskID: "task-2", Option: backup.Option{BackupName: "backup1"}})

		assert.Nil(t, view)
		assert.ErrorContains(t, err, "existing task")
	})
}

func TestCreateBackupReadView(t *testing.T) {
	t.Run("AssemblesTaskAndMeta", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectFullMeta(t, cli, "root/backup1", &backuppb.BackupInfo{Name: "backup1", Size: 100})
		expectMetaSize(t, cli, "root/backup1", 100)

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))

		uc := &CreateBackup{backupStorage: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.readView(context.Background(), "task-1", "root/backup1")

		require.NoError(t, err)
		require.NotNil(t, view.Task)
		assert.Equal(t, "task-1", view.Task.ID())
		require.NotNil(t, view.Meta)
		assert.Equal(t, "backup1", view.Meta.GetName())
		assert.Equal(t, int64(100), view.MetaSize)
	})

	t.Run("FailsWhenTaskUnknown", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		uc := &CreateBackup{backupStorage: cli, taskMgr: taskmgr.NewMgr(), rootPath: "root"}
		view, err := uc.readView(context.Background(), "task-1", "root/backup1")

		assert.Nil(t, view)
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	})

	t.Run("FailsWhenMetaUnreadable", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		cli.EXPECT().
			ListPrefix(mock.Anything, "root/backup1/meta/full_meta.json", false).
			Return(nil, errors.New("stat denied"))

		mgr := taskmgr.NewMgr()
		require.NoError(t, mgr.AddBackupTask("task-1", "backup1"))

		uc := &CreateBackup{backupStorage: cli, taskMgr: mgr, rootPath: "root"}
		view, err := uc.readView(context.Background(), "task-1", "root/backup1")

		assert.Nil(t, view)
		assert.ErrorContains(t, err, "stat denied")
	})
}

func TestCreateBackupDir(t *testing.T) {
	t.Run("UsesConfiguredRootPath", func(t *testing.T) {
		uc := &CreateBackup{rootPath: "root"}

		// mpath.BackupDir keeps a trailing separator, as the task expects.
		assert.Equal(t, "root/backup1/", uc.backupDir(CreateBackupRequest{Option: backup.Option{BackupName: "backup1"}}))
	})

	t.Run("RequestRootPathWins", func(t *testing.T) {
		uc := &CreateBackup{rootPath: "root"}

		assert.Equal(t, "other/backup1/", uc.backupDir(CreateBackupRequest{Option: backup.Option{BackupName: "backup1"}, RootPath: "other"}))
	})
}
