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
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// expectClientConfigs teaches both mock clients everything a clean admission
// runs: the config probe NewTask does to resolve the transfer mode, the source
// list preflight, and the artifact dir preflight finding the dir free.
func expectClientConfigs(t *testing.T, milvusStorage, backupStorage *storage.MockClient, backupDir string) {
	t.Helper()

	milvusStorage.EXPECT().Config().Return(storage.Config{})
	milvusStorage.EXPECT().NewObjectIter(mock.Anything, mock.Anything, true).Return(storage.NewMockObjectIterator(nil)).Once()
	backupStorage.EXPECT().Config().Return(storage.Config{})
	backupStorage.EXPECT().NewObjectIter(mock.Anything, backupDir, false).Return(storage.NewMockObjectIterator(nil)).Once()
}

func TestNewBackupJob(t *testing.T) {
	t.Run("RegistersJobInTheTaskManager", func(t *testing.T) {
		params := cfg.New()
		backupDir := mpath.BackupDir(params.Backup.Storage.RootPath.Val, "backup1")
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		expectClientConfigs(t, milvusStorage, backupStorage, backupDir)
		store := jobstate.NewStore()

		job, err := newBackupJob(context.Background(), params, store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})

		require.NoError(t, err)
		require.NotNil(t, job)
		status, err := store.GetBackupTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, "backup1", status.Name)
		assert.Equal(t, jobstate.BackupStateInitial, status.State)
	})

	t.Run("RefusesSecondLiveJobWithSameName", func(t *testing.T) {
		params := cfg.New()
		backupDir := mpath.BackupDir(params.Backup.Storage.RootPath.Val, "backup1")
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		expectClientConfigs(t, milvusStorage, backupStorage, backupDir)
		expectClientConfigs(t, milvusStorage, backupStorage, backupDir)
		store := jobstate.NewStore()

		_, err := newBackupJob(context.Background(), params, store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})
		require.NoError(t, err)

		job, err := newBackupJob(context.Background(), params, store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-2", Option: backup.Option{BackupName: "backup1"}})

		assert.Nil(t, job)
		assert.ErrorContains(t, err, "existing task")
	})

	t.Run("RefusesExistingBackupDir", func(t *testing.T) {
		params := cfg.New()
		backupDir := mpath.BackupDir(params.Backup.Storage.RootPath.Val, "backup1")
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		milvusStorage.EXPECT().NewObjectIter(mock.Anything, mock.Anything, true).Return(storage.NewMockObjectIterator(nil)).Once()
		backupStorage.EXPECT().NewObjectIter(mock.Anything, backupDir, false).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: backupDir + "meta"}})).Once()

		store := jobstate.NewStore()
		job, err := newBackupJob(context.Background(), params, store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})

		// A taken name is a conflict, not an unreachable storage: the caller
		// gets a plain failure, and no job is left registered.
		assert.Nil(t, job)
		assert.ErrorContains(t, err, "backup with name backup1 already exist")
		assert.NotErrorIs(t, err, ErrStorageNotReady)
		_, err = store.GetBackupTask("task-1")
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
		_, err = store.GetBackupTaskByName("backup1")
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})

	t.Run("BackupDirListFailureIsStorageNotReady", func(t *testing.T) {
		params := cfg.New()
		backupDir := mpath.BackupDir(params.Backup.Storage.RootPath.Val, "backup1")
		milvusStorage := storage.NewMockClient(t)
		backupStorage := storage.NewMockClient(t)
		milvusStorage.EXPECT().NewObjectIter(mock.Anything, mock.Anything, true).Return(storage.NewMockObjectIterator(nil)).Once()
		denied := errors.New("list denied")
		backupStorage.EXPECT().NewObjectIter(mock.Anything, backupDir, false).Return(errorSeq(denied)).Once()
		backupStorage.EXPECT().Config().Return(storage.Config{Bucket: "dest-bucket"})

		store := jobstate.NewStore()
		job, err := newBackupJob(context.Background(), params, store, milvusStorage, backupStorage,
			CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})

		// The dir check is storage admission like the source preflight: the
		// caller may retry the same request once storage recovers.
		assert.Nil(t, job)
		assert.ErrorIs(t, err, ErrStorageNotReady)
		assert.ErrorIs(t, err, denied)
		assert.ErrorContains(t, err, backupDir)
		_, err = store.GetBackupTask("task-1")
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
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
		dest.EXPECT().NewObjectIter(mock.Anything, mpath.BackupDir(params.Backup.Storage.RootPath.Val, "retry_backup"), false).
			Return(storage.NewMockObjectIterator(nil)).Once()
		dest.EXPECT().Config().Return(storage.Config{})
		job, err = newBackupJob(context.Background(), params, store, source, dest, req)
		require.NoError(t, err)
		assert.NotNil(t, job)
		status, err := store.GetBackupTask(req.TaskID)
		require.NoError(t, err)
		assert.Equal(t, req.Option.BackupName, status.Name)
	})

	t.Run("MetaOnlyDoesNotListSource", func(t *testing.T) {
		source := storage.NewMockClient(t)
		dest := storage.NewMockClient(t)
		source.EXPECT().Config().Return(storage.Config{})
		dest.EXPECT().Config().Return(storage.Config{})
		dest.EXPECT().NewObjectIter(mock.Anything, mpath.BackupDir(cfg.New().Backup.Storage.RootPath.Val, "meta_backup"), false).
			Return(storage.NewMockObjectIterator(nil)).Once()
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

// stubBackupTask stands in for the backup run. The job owns the settlement,
// so the stub is a plain tasklet: whatever it answers is what the job
// records, which is exactly the property under test.
type stubBackupTask struct{ err error }

func (s stubBackupTask) Execute(context.Context) error { return s.err }

func newStubBackupJob(t *testing.T, store *jobstate.Store, taskID string, task tasklet.Tasklet) *BackupJob {
	t.Helper()
	tracker, err := store.AddBackupTask(taskID, "backup1")
	require.NoError(t, err)
	return &BackupJob{task: task, tracker: tracker, done: make(chan struct{})}
}

func TestBackupJobRun(t *testing.T) {
	t.Run("SuccessSettlesTheStatus", func(t *testing.T) {
		store := jobstate.NewStore()
		job := newStubBackupJob(t, store, "task-1", stubBackupTask{})

		// Run is fire-and-forget; the outcome is read back through Wait.
		job.Run(context.Background())
		status, err := job.Wait(context.Background())
		require.NoError(t, err)
		assert.Equal(t, jobstate.BackupStateSuccess, status.State)
		assert.False(t, status.EndTime.IsZero())
		assert.True(t, status.Terminal())

		// The store sees the same settlement the Wait answered with.
		stored, err := store.GetBackupTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, jobstate.BackupStateSuccess, stored.State)
	})

	t.Run("FailureLandsInTheStatus", func(t *testing.T) {
		store := jobstate.NewStore()
		job := newStubBackupJob(t, store, "task-1", stubBackupTask{err: errors.New("boom")})

		job.Run(context.Background())
		status, err := job.Wait(context.Background())
		require.NoError(t, err)
		assert.Equal(t, jobstate.BackupStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
		assert.False(t, status.EndTime.IsZero())
		assert.True(t, status.Terminal())
	})
}

func TestBackupJobWait(t *testing.T) {
	t.Run("StopsWhenContextCanceled", func(t *testing.T) {
		store := jobstate.NewStore()
		job := newStubBackupJob(t, store, "task-1", stubBackupTask{})

		// The job never runs, so only the context can release the wait.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := job.Wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// errorSeq fails the listing on its first iteration, standing in for listing
// errors surfaced through the sequence.
func errorSeq(err error) iter.Seq2[storage.ObjectAttr, error] {
	return func(yield func(storage.ObjectAttr, error) bool) {
		yield(storage.ObjectAttr{}, err)
	}
}
