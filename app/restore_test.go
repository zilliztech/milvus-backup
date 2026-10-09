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
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

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

func TestNewRestoreJob(t *testing.T) {
	t.Run("AssemblesAndRegistersTheJob", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)
		milvusCli := storage.NewMockClient(t)
		store := jobstate.NewStore()

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: ""})
		// Task creation reads both clients' configs to resolve the transfer mode.
		backupCli.EXPECT().Config().Return(storage.Config{})
		milvusCli.EXPECT().Config().Return(storage.Config{})

		req := RestoreRequest{
			TaskID:     "restore_1",
			BackupName: "backup1",
			Plan:       &restore.Plan{},
			Option:     &restore.Option{},
		}
		job, err := newRestoreJob(context.Background(), cfg.New(), store, backupCli, milvusCli, req)

		require.NoError(t, err)
		require.NotNil(t, job)

		// Task creation registered the job with the job state store.
		view, err := store.GetRestoreTask("restore_1")
		require.NoError(t, err)
		assert.Equal(t, "restore_1", view.ID())
		assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, view.StateCode())
	})

	t.Run("FailsWhenBackupNotFound", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectNoBackup(t, backupCli, "backup1/")

		_, err := newRestoreJob(context.Background(), cfg.New(), jobstate.NewStore(), backupCli, storage.NewMockClient(t),
			RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorIs(t, err, ErrBackupNotFound)
		assert.ErrorContains(t, err, "backup backup1")
	})

	t.Run("FailsWhenExistCheckFails", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		backupCli.EXPECT().
			NewObjectIter(mock.Anything, "backup1/meta/backup_meta.json", false).
			Return(errorSeq(errors.New("stat denied")))

		_, err := newRestoreJob(context.Background(), cfg.New(), jobstate.NewStore(), backupCli, storage.NewMockClient(t),
			RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

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

		_, err := newRestoreJob(context.Background(), cfg.New(), jobstate.NewStore(), backupCli, storage.NewMockClient(t),
			RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "read denied")
	})

	t.Run("FailsWhenTaskRefusesTheFormat", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: "parquet"})

		_, err := newRestoreJob(context.Background(), cfg.New(), jobstate.NewStore(), backupCli, storage.NewMockClient(t),
			RestoreRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "new restore task")
	})
}

func TestNewRestoreSecondaryJob(t *testing.T) {
	t.Run("AssemblesAndRegistersTheJob", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)
		milvusCli := storage.NewMockClient(t)
		store := jobstate.NewStore()

		expectBackupExists(t, backupCli, "backup1/")
		expectFullMeta(t, backupCli, "backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Format: ""})

		req := RestoreSecondaryRequest{
			TaskID:          "restore_1",
			BackupName:      "backup1",
			SourceClusterID: "source",
			TargetClusterID: "target",
		}
		job, err := newRestoreSecondaryJob(context.Background(), cfg.New(), store, backupCli, milvusCli, req)

		require.NoError(t, err)
		require.NotNil(t, job)

		view, err := store.GetRestoreTask("restore_1")
		require.NoError(t, err)
		assert.Equal(t, "restore_1", view.ID())
		assert.Equal(t, backuppb.RestoreTaskStateCode_INITIAL, view.StateCode())
	})

	t.Run("FailsWhenBackupNotFound", func(t *testing.T) {
		backupCli := storage.NewMockClient(t)

		expectNoBackup(t, backupCli, "backup1/")

		_, err := newRestoreSecondaryJob(context.Background(), cfg.New(), jobstate.NewStore(), backupCli, storage.NewMockClient(t),
			RestoreSecondaryRequest{TaskID: "restore_1", BackupName: "backup1"})

		assert.ErrorIs(t, err, ErrBackupNotFound)
		assert.ErrorContains(t, err, fmt.Sprintf("app: backup backup1: %s", ErrBackupNotFound))
	})
}
