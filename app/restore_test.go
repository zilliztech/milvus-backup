package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// newTestRestore builds the usecase with mock client factories, so Start never
// touches a real backend. The backup meta is served by the mock: the exist
// check lists backup_meta.json, then the read fetches full_meta.json.
func newTestRestore(t *testing.T, backupCli *storage.MockClient, milvusCli *storage.MockClient) *Restore {
	t.Helper()

	return &Restore{
		params: &v2.Config{},
		newBackupStorage: func(context.Context, string) (storage.Client, error) {
			return backupCli, nil
		},
		newMilvusStorage: func(context.Context) (storage.Client, error) {
			return milvusCli, nil
		},
	}
}

func newTestRestoreSecondary(t *testing.T, backupCli *storage.MockClient, milvusCli *storage.MockClient) *RestoreSecondary {
	t.Helper()

	return &RestoreSecondary{
		params: &v2.Config{},
		newBackupStorage: func(context.Context) (storage.Client, error) {
			return backupCli, nil
		},
		newMilvusStorage: func(context.Context) (storage.Client, error) {
			return milvusCli, nil
		},
	}
}

// expectBackupExists teaches the mock client that the backup dir exists: the
// exist check lists backup_meta.json and finds it.
func expectBackupExists(t *testing.T, cli *storage.MockClient, backupDir string) {
	t.Helper()

	key := backupDir + "meta/backup_meta.json"
	iter := storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: key, Length: 10}})
	cli.EXPECT().ListPrefix(mock.Anything, key, false).Return(iter, nil)
}

// expectNoBackup teaches the mock client that the backup dir does not exist.
func expectNoBackup(t *testing.T, cli *storage.MockClient, backupDir string) {
	t.Helper()

	key := backupDir + "meta/backup_meta.json"
	iter := storage.NewMockObjectIterator(nil)
	cli.EXPECT().ListPrefix(mock.Anything, key, false).Return(iter, nil)
}

func TestRestoreStart(t *testing.T) {
	t.Run("AssemblesAndRegistersTheJob", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)
		milvusCli := storage.NewMockClient(t)

		// Path from the request overrides the configured root.
		expectBackupExists(t, backupCli, "custom/backup1/")
		expectFullMeta(t, backupCli, "custom/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: ""})
		// Task creation reads both clients' configs to resolve the transfer mode.
		backupCli.EXPECT().Config().Return(storage.Config{})
		milvusCli.EXPECT().Config().Return(storage.Config{})

		uc := newTestRestore(t, backupCli, milvusCli)
		req := RestoreRequest{
			TaskID:     "restore_1",
			BackupName: "backup1",
			Path:       "custom",
			Plan:       &restore.Plan{},
			Option:     &restore.Option{},
		}
		job, err := uc.Start(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, "restore_1", job.TaskID())

		// Task creation registered the job with the task manager.
		view, err := uc.TaskView(job.TaskID())
		require.NoError(t, err)
		assert.Equal(t, "restore_1", view.ID())
		assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, view.StateCode())
	})

	t.Run("FailsWhenBackupNotFound", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectNoBackup(t, backupCli, "backup1/")

		uc := newTestRestore(t, backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "backup backup1 not found")
		var notFound *BackupNotFoundError
		assert.ErrorAs(t, err, &notFound)
		assert.Equal(t, "backup1", notFound.Name)
	})

	t.Run("FailsWhenExistCheckFails", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		backupCli.EXPECT().
			ListPrefix(mock.Anything, "backup1/meta/backup_meta.json", false).
			Return(nil, errors.New("stat denied"))

		uc := newTestRestore(t, backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "stat denied")
	})

	t.Run("FailsWhenMetaUnreadable", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		// The full meta cannot even be checked for existence, so the read
		// fails instead of falling back to the per-level meta.
		backupCli.EXPECT().
			ListPrefix(mock.Anything, "backup1/meta/full_meta.json", false).
			Return(nil, errors.New("read denied"))

		uc := newTestRestore(t, backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "read denied")
	})

	t.Run("FailsWhenTaskRefusesTheFormat", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: "parquet"})

		uc := newTestRestore(t, backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "new restore task")
	})

	t.Run("FailsWhenBackupStorageCreationFails", func(t *testing.T) {
		uc := &Restore{
			params: &v2.Config{},
			newBackupStorage: func(context.Context, string) (storage.Client, error) {
				return nil, errors.New("dial timeout")
			},
		}

		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "dial timeout")
	})

	t.Run("TaskViewErrorsForUnknownTask", func(t *testing.T) {
		uc := newTestRestore(t, storage.NewMockClient(t), storage.NewMockClient(t))

		_, err := uc.TaskView("missing")

		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	})
}

func TestRestoreSecondaryStart(t *testing.T) {
	t.Run("AssemblesAndRegistersTheJob", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)
		milvusCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: ""})

		uc := newTestRestoreSecondary(t, backupCli, milvusCli)
		req := RestoreSecondaryRequest{
			TaskID:          "restore_1",
			BackupName:      "backup1",
			SourceClusterID: "source",
			TargetClusterID: "target",
		}
		job, err := uc.Start(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, "restore_1", job.TaskID())

		view, err := uc.TaskView(job.TaskID())
		require.NoError(t, err)
		assert.Equal(t, "restore_1", view.ID())
		assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, view.StateCode())
	})

	t.Run("FailsWhenBackupNotFound", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectNoBackup(t, backupCli, "backup1/")

		uc := newTestRestoreSecondary(t, backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(),
			RestoreSecondaryRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "backup backup1 not found")
		var notFound *BackupNotFoundError
		assert.ErrorAs(t, err, &notFound)
		assert.Equal(t, "backup1", notFound.Name)
	})

	t.Run("FailsWhenMilvusStorageCreationFails", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: ""})

		uc := &RestoreSecondary{
			params: &v2.Config{},
			newBackupStorage: func(context.Context) (storage.Client, error) {
				return backupCli, nil
			},
			newMilvusStorage: func(context.Context) (storage.Client, error) {
				return nil, errors.New("dial timeout")
			},
		}
		_, err := uc.Start(context.Background(),
			RestoreSecondaryRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "dial timeout")
	})

	t.Run("FailsWhenBackupStorageCreationFails", func(t *testing.T) {
		uc := &RestoreSecondary{
			params: &v2.Config{},
			newBackupStorage: func(context.Context) (storage.Client, error) {
				return nil, errors.New("dial timeout")
			},
		}

		_, err := uc.Start(context.Background(),
			RestoreSecondaryRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "dial timeout")
	})
}
