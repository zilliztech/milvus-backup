package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	milvusStorage.EXPECT().NewObjectIter(mock.Anything, mock.Anything, true).Return(storage.NewMockObjectIterator(nil)).Once()
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

		job, err := uc.Start(context.Background(), CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})

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
		_, err := uc.Start(context.Background(), CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})
		require.NoError(t, err)

		job, err := uc.Start(context.Background(), CreateBackupRequest{TaskID: "task-2", Option: backup.Option{BackupName: "backup1"}})

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
		_, err := uc.Start(context.Background(), CreateBackupRequest{TaskID: "task-1", Option: backup.Option{BackupName: "backup1"}})
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

func TestCreateBackupPreflight(t *testing.T) {
	t.Run("FailureLeavesNoJobAndSameRequestCanRetry", func(t *testing.T) {
		source := storage.NewMockClient(t)
		dest := storage.NewMockClient(t)
		params := v2.New()
		params.Milvus.Storage.RootPath.Val = "instance/"
		uc := &CreateBackup{params: params, milvusStorage: source, backupStorage: dest, taskMgr: taskmgr.NewMgr(), rootPath: "backup-root"}
		req := CreateBackupRequest{TaskID: "retry-id", Option: backup.Option{BackupName: "retry_backup"}}
		denied := errors.New("list access denied")
		iter := &preflightIterator{}
		iter.On("Next", mock.Anything).Return(storage.ObjectAttr{}, false, denied).Once()
		iter.On("Close").Return(nil).Once()
		t.Cleanup(func() { iter.AssertExpectations(t) })
		source.EXPECT().NewObjectIter(mock.Anything, "instance/insert_log/", true).Return(iter).Once()
		source.EXPECT().Config().Return(storage.Config{Bucket: "source-bucket"})

		job, err := uc.Start(context.Background(), req)

		assert.Nil(t, job)
		assert.ErrorIs(t, err, ErrStorageNotReady)
		assert.ErrorIs(t, err, denied)
		assert.ErrorContains(t, err, "source-bucket")
		_, err = uc.taskMgr.GetBackupTask(req.TaskID)
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
		_, err = uc.taskMgr.GetBackupTaskByName(req.Option.BackupName)
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)

		// Reuse the same client and request after access becomes available.
		successIter := &preflightIterator{}
		successIter.On("Next", mock.Anything).Return(storage.ObjectAttr{Key: "first"}, true, nil).Once()
		successIter.On("Close").Return(nil).Once()
		t.Cleanup(func() { successIter.AssertExpectations(t) })
		source.EXPECT().NewObjectIter(mock.Anything, "instance/insert_log/", true).Return(successIter).Once()
		dest.EXPECT().Config().Return(storage.Config{})
		job, err = uc.Start(context.Background(), req)
		require.NoError(t, err)
		assert.NotNil(t, job)
		view, err := uc.taskMgr.GetBackupTask(req.TaskID)
		require.NoError(t, err)
		assert.Equal(t, req.Option.BackupName, view.Name())
	})

	t.Run("MetaOnlyDoesNotListSource", func(t *testing.T) {
		source := storage.NewMockClient(t)
		dest := storage.NewMockClient(t)
		source.EXPECT().Config().Return(storage.Config{})
		dest.EXPECT().Config().Return(storage.Config{})
		uc := &CreateBackup{params: v2.New(), milvusStorage: source, backupStorage: dest, taskMgr: taskmgr.NewMgr()}
		job, err := uc.Start(context.Background(), CreateBackupRequest{TaskID: "meta", Option: backup.Option{BackupName: "meta_backup", Strategy: backup.StrategyMetaOnly}})
		assert.NoError(t, err)
		assert.NotNil(t, job)
	})

	t.Run("BoundsListDeadlineAndPreservesCancellation", func(t *testing.T) {
		source := storage.NewMockClient(t)
		dest := storage.NewMockClient(t)
		uc := &CreateBackup{params: v2.New(), milvusStorage: source, backupStorage: dest, taskMgr: taskmgr.NewMgr()}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		iter := &preflightIterator{}
		iter.On("Next", mock.Anything).Run(func(args mock.Arguments) {
			cancel()
			<-args.Get(0).(context.Context).Done()
		}).Return(storage.ObjectAttr{}, false, context.Canceled).Once()
		iter.On("Close").Return(nil).Once()
		t.Cleanup(func() { iter.AssertExpectations(t) })
		source.EXPECT().NewObjectIter(mock.Anything, mock.Anything, true).RunAndReturn(func(probeCtx context.Context, _ string, _ bool) storage.ObjectIterator {
			deadline, ok := probeCtx.Deadline()
			assert.True(t, ok)
			assert.WithinDuration(t, time.Now().Add(10*time.Second), deadline, time.Second)
			return iter
		}).Once()
		source.EXPECT().Config().Return(storage.Config{})
		view, err := uc.Execute(ctx, CreateBackupRequest{TaskID: "cancel", Option: backup.Option{BackupName: "cancel_backup"}})
		assert.Nil(t, view)
		assert.ErrorIs(t, err, ErrStorageNotReady)
		assert.ErrorIs(t, err, context.Canceled)
		_, err = uc.taskMgr.GetBackupTask("cancel")
		assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	})
}

// errorIterator fails every read. Since the iterator constructor cannot fail,
// it stands in for listing errors that now surface through Next.
type errorIterator struct{ err error }

func (e *errorIterator) Next(context.Context) (storage.ObjectAttr, bool, error) {
	return storage.ObjectAttr{}, false, e.err
}

func (e *errorIterator) Close() error { return nil }

// preflightIterator detects extra reads and verifies cleanup on success or error.
type preflightIterator struct{ mock.Mock }

func (i *preflightIterator) Next(ctx context.Context) (storage.ObjectAttr, bool, error) {
	r := i.Called(ctx)
	return r.Get(0).(storage.ObjectAttr), r.Bool(1), r.Error(2)
}

func (i *preflightIterator) Close() error { return i.Called().Error(0) }
