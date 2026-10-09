package app

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// expectClientConfigs teaches both mock clients the config probe NewTask does
// to resolve the transfer mode.
func expectClientConfigs(t *testing.T, milvusStorage, backupStorage *storage.MockClient) {
	t.Helper()

	milvusStorage.EXPECT().Config().Return(storage.Config{})
	milvusStorage.EXPECT().NewObjectIter(mock.Anything, mock.Anything, true).Return(storage.NewMockObjectIterator(nil)).Once()
	backupStorage.EXPECT().Config().Return(storage.Config{})
}

func TestNewBackupJob(t *testing.T) {
	t.Run("RegistersJobInTheTaskManager", func(t *testing.T) {
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		expectClientConfigs(t, milvusStorage, backupStorage)
		store := jobstate.NewStore()

		job, err := newBackupJob(context.Background(), cfg.New(), store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})

		require.NoError(t, err)
		require.NotNil(t, job)
		view, err := store.GetBackupTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, "backup1", view.Name())
	})

	t.Run("RefusesSecondLiveJobWithSameName", func(t *testing.T) {
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		expectClientConfigs(t, milvusStorage, backupStorage)
		expectClientConfigs(t, milvusStorage, backupStorage)
		store := jobstate.NewStore()

		_, err := newBackupJob(context.Background(), cfg.New(), store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})
		require.NoError(t, err)

		job, err := newBackupJob(context.Background(), cfg.New(), store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-2", Option: backup.Option{BackupName: "backup1"}})

		assert.Nil(t, job)
		assert.ErrorContains(t, err, "existing task")
	})
}

func TestNewBackupJobPreflight(t *testing.T) {
	t.Run("FailureLeavesNoJobAndSameRequestCanRetry", func(t *testing.T) {
		source := storage.NewMockClient(t)
		dest := storage.NewMockClient(t)
		params := cfg.New()
		params.Milvus.Storage.RootPath.Val = "instance/"
		store := jobstate.NewStore()
		req := CreateBackupRequest{TaskID: "retry-id", Option: backup.Option{BackupName: "retry_backup"}}
		denied := errors.New("list access denied")
		source.EXPECT().NewObjectIter(mock.Anything, "instance/insert_log/", true).Return(errorSeq(denied)).Once()
		source.EXPECT().Config().Return(storage.Config{Bucket: "source-bucket"})

		job, err := newBackupJob(context.Background(), params, store, source, dest, req)

		assert.Nil(t, job)
		assert.ErrorIs(t, err, ErrStorageNotReady)
		assert.ErrorIs(t, err, denied)
		assert.ErrorContains(t, err, "source-bucket")
		_, err = store.GetBackupTask(req.TaskID)
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
		_, err = store.GetBackupTaskByName(req.Option.BackupName)
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)

		// Reuse the same client and request after access becomes available.
		source.EXPECT().NewObjectIter(mock.Anything, "instance/insert_log/", true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: "first"}})).Once()
		dest.EXPECT().Config().Return(storage.Config{})
		job, err = newBackupJob(context.Background(), params, store, source, dest, req)
		require.NoError(t, err)
		assert.NotNil(t, job)
		view, err := store.GetBackupTask(req.TaskID)
		require.NoError(t, err)
		assert.Equal(t, req.Option.BackupName, view.Name())
	})

	t.Run("MetaOnlyDoesNotListSource", func(t *testing.T) {
		source := storage.NewMockClient(t)
		dest := storage.NewMockClient(t)
		source.EXPECT().Config().Return(storage.Config{})
		dest.EXPECT().Config().Return(storage.Config{})
		job, err := newBackupJob(context.Background(), cfg.New(), jobstate.NewStore(), source, dest,
			CreateBackupRequest{TaskID: "meta", Option: backup.Option{BackupName: "meta_backup", Strategy: backup.StrategyMetaOnly}})
		assert.NoError(t, err)
		assert.NotNil(t, job)
	})

	t.Run("BoundsListDeadlineAndPreservesCancellation", func(t *testing.T) {
		source := storage.NewMockClient(t)
		dest := storage.NewMockClient(t)
		store := jobstate.NewStore()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		source.EXPECT().NewObjectIter(mock.Anything, mock.Anything, true).RunAndReturn(func(probeCtx context.Context, _ string, _ bool) iter.Seq2[storage.ObjectAttr, error] {
			deadline, ok := probeCtx.Deadline()
			assert.True(t, ok)
			assert.WithinDuration(t, time.Now().Add(10*time.Second), deadline, time.Second)
			return func(yield func(storage.ObjectAttr, error) bool) {
				cancel()
				<-probeCtx.Done()
				yield(storage.ObjectAttr{}, context.Canceled)
			}
		}).Once()
		source.EXPECT().Config().Return(storage.Config{})
		job, err := newBackupJob(ctx, cfg.New(), store, source, dest,
			CreateBackupRequest{TaskID: "cancel", Option: backup.Option{BackupName: "cancel_backup"}})
		assert.Nil(t, job)
		assert.ErrorIs(t, err, ErrStorageNotReady)
		assert.ErrorIs(t, err, context.Canceled)
		_, err = store.GetBackupTask("cancel")
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})
}

// errorSeq fails the listing on its first iteration, standing in for listing
// errors surfaced through the sequence.
func errorSeq(err error) iter.Seq2[storage.ObjectAttr, error] {
	return func(yield func(storage.ObjectAttr, error) bool) {
		yield(storage.ObjectAttr{}, err)
	}
}
