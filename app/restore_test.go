package app

import (
	"context"
	"errors"
	"fmt"
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

// newTestRestore builds the usecase as a struct literal with mock clients, so
// Start never touches a real backend. The constructor creates real clients
// from config, which is exactly what the literal skips.
func newTestRestore(backupCli *storage.MockClient, milvusCli *storage.MockClient) *Restore {
	return &Restore{
		params:        v2.New(),
		backupStorage: backupCli,
		milvusStorage: milvusCli,
		taskMgr:       taskmgr.NewMgr(),
	}
}

func newTestRestoreSecondary(backupCli *storage.MockClient, milvusCli *storage.MockClient) *RestoreSecondary {
	return &RestoreSecondary{
		params:        v2.New(),
		backupStorage: backupCli,
		milvusStorage: milvusCli,
		taskMgr:       taskmgr.NewMgr(),
	}
}

// expectBackupExists teaches the mock client that the backup dir exists: the
// exist check lists backup_meta.json and finds it.
func expectBackupExists(t *testing.T, cli *storage.MockClient, backupDir string) {
	t.Helper()

	key := backupDir + "meta/backup_meta.json"
	iter := storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: key, Length: 10}})
	cli.EXPECT().NewObjectIter(mock.Anything, key, false).Return(iter)
}

// expectNoBackup teaches the mock client that the backup dir does not exist.
func expectNoBackup(t *testing.T, cli *storage.MockClient, backupDir string) {
	t.Helper()

	key := backupDir + "meta/backup_meta.json"
	iter := storage.NewMockObjectIterator(nil)
	cli.EXPECT().NewObjectIter(mock.Anything, key, false).Return(iter)
}

func TestRestoreStart(t *testing.T) {
	t.Run("AssemblesAndRegistersTheJob", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)
		milvusCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: ""})
		// Task creation reads both clients' configs to resolve the transfer mode.
		backupCli.EXPECT().Config().Return(storage.Config{})
		milvusCli.EXPECT().Config().Return(storage.Config{})

		uc := newTestRestore(backupCli, milvusCli)
		req := RestoreRequest{
			TaskID:     "restore_1",
			BackupName: "backup1",
			Plan:       &restore.Plan{},
			Option:     &restore.Option{},
		}
		job, err := uc.Start(context.Background(), req)

		require.NoError(t, err)
		require.NotNil(t, job)

		// Task creation registered the job with the task manager.
		view, err := uc.taskMgr.GetRestoreTask("restore_1")
		require.NoError(t, err)
		assert.Equal(t, "restore_1", view.ID())
		assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, view.StateCode())
	})

	t.Run("FailsWhenBackupNotFound", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectNoBackup(t, backupCli, "backup1/")

		uc := newTestRestore(backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorIs(t, err, ErrBackupNotFound)
		assert.ErrorContains(t, err, "backup backup1")
	})

	t.Run("FailsWhenExistCheckFails", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		backupCli.EXPECT().
			NewObjectIter(mock.Anything, "backup1/meta/backup_meta.json", false).
			Return(errorSeq(errors.New("stat denied")))

		uc := newTestRestore(backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "stat denied")
	})

	t.Run("FailsWhenMetaUnreadable", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		// The full meta cannot even be checked for existence, so the read
		// fails instead of falling back to the per-level meta.
		backupCli.EXPECT().
			NewObjectIter(mock.Anything, "backup1/meta/full_meta.json", false).
			Return(errorSeq(errors.New("read denied")))

		uc := newTestRestore(backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "read denied")
	})

	t.Run("FailsWhenTaskRefusesTheFormat", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: "parquet"})

		uc := newTestRestore(backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(), RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "new restore task")
	})
}

func TestRestoreSecondaryStart(t *testing.T) {
	t.Run("AssemblesAndRegistersTheJob", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)
		milvusCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: ""})

		uc := newTestRestoreSecondary(backupCli, milvusCli)
		req := RestoreSecondaryRequest{
			TaskID:          "restore_1",
			BackupName:      "backup1",
			SourceClusterID: "source",
			TargetClusterID: "target",
		}
		job, err := uc.Start(context.Background(), req)

		require.NoError(t, err)
		require.NotNil(t, job)

		view, err := uc.taskMgr.GetRestoreTask("restore_1")
		require.NoError(t, err)
		assert.Equal(t, "restore_1", view.ID())
		assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, view.StateCode())
	})

	t.Run("FailsWhenBackupNotFound", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectNoBackup(t, backupCli, "backup1/")

		uc := newTestRestoreSecondary(backupCli, storage.NewMockClient(t))
		_, err := uc.Start(context.Background(),
			RestoreSecondaryRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorIs(t, err, ErrBackupNotFound)
		assert.ErrorContains(t, err, fmt.Sprintf("app: backup backup1: %s", ErrBackupNotFound))
	})
}
