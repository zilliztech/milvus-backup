package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/backup"
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

func TestCreateBackupDir(t *testing.T) {
	t.Run("UsesConfiguredRootPath", func(t *testing.T) {
		uc := &CreateBackup{rootPath: "root"}

		// mpath.BackupDir keeps a trailing separator, as the task expects. A
		// per-call root path is the transport forking the config, so there is
		// no request-level override to test here.
		assert.Equal(t, "root/backup1/", uc.backupDir("backup1"))
	})
}
