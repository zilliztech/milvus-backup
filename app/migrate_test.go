package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// stubMigrateTask stands in for the migrate task so the job's lifecycle
// wiring runs without cloud storage behind the copy.
type stubMigrateTask struct{ err error }

func (s stubMigrateTask) Execute(context.Context) error { return s.err }

func newStubMigrateJob(store *jobstate.Store, taskID string, task tasklet.Tasklet) *MigrateJob {
	tracker := store.AddMigrateTask(taskID, 0)
	return &MigrateJob{task: task, tracker: tracker, done: make(chan struct{})}
}

func TestMigrateJobRun(t *testing.T) {
	t.Run("SuccessSettlesTheStatus", func(t *testing.T) {
		store := jobstate.NewStore()
		job := newStubMigrateJob(store, "task-1", stubMigrateTask{})

		// Run is fire-and-forget; the outcome is read back through Wait.
		job.Run(context.Background())
		status, err := job.Wait(context.Background())
		require.NoError(t, err)
		assert.Equal(t, jobstate.MigrateStateSuccess, status.State)
		assert.False(t, status.EndTime.IsZero())
		assert.True(t, status.Terminal())

		// The store sees the same settlement the Wait answered with.
		stored, err := store.GetMigrateTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, jobstate.MigrateStateSuccess, stored.State)
	})

	t.Run("FailureLandsInTheStatus", func(t *testing.T) {
		store := jobstate.NewStore()
		job := newStubMigrateJob(store, "task-1", stubMigrateTask{err: errors.New("boom")})

		job.Run(context.Background())
		status, err := job.Wait(context.Background())
		require.NoError(t, err)
		assert.Equal(t, jobstate.MigrateStateFail, status.State)
		assert.Equal(t, "boom", status.ErrorMessage)
		assert.False(t, status.EndTime.IsZero())
		assert.True(t, status.Terminal())
	})
}

func TestMigrateJobWait(t *testing.T) {
	t.Run("StopsWhenContextCanceled", func(t *testing.T) {
		store := jobstate.NewStore()
		job := newStubMigrateJob(store, "task-1", stubMigrateTask{})

		// The job never runs, so only the context can release the wait.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := job.Wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestNewMigrateJob(t *testing.T) {
	migrateParams := func() *cfg.Config {
		var params cfg.Config
		params.Backup.Storage.RootPath.Val = "root"
		return &params
	}

	t.Run("RefusesWhenMetaUnreadable", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		// The meta exist check fails, so the job is never registered.
		cli.EXPECT().
			NewObjectIter(mock.Anything, "root/backup1/meta/full_meta.json", false).
			Return(errorSeq(errors.New("stat denied")))

		store := jobstate.NewStore()
		_, err := newMigrateJob(context.Background(), store, nil, cli, migrateParams(),
			MigrateRequest{TaskID: "task-1", BackupName: "backup1", ClusterID: "cluster-1"})

		assert.ErrorContains(t, err, "stat denied")
		_, err = store.GetMigrateTask("task-1")
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})

	t.Run("RegistersWithBackupSize", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Size: 1 << 30})

		store := jobstate.NewStore()
		job, err := newMigrateJob(context.Background(), store, nil, cli, migrateParams(),
			MigrateRequest{TaskID: "task-1", BackupName: "backup1", ClusterID: "cluster-1"})
		require.NoError(t, err)

		// Registration is part of construction: the snapshot is there before
		// Run, still untouched by the task.
		status := job.tracker.Snapshot()
		assert.Equal(t, jobstate.MigrateStateInitial, status.State)
		assert.Equal(t, int64(1<<30), status.TotalSize)

		stored, err := store.GetMigrateTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, int64(1<<30), stored.TotalSize)
	})
}
