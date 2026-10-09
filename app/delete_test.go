package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestDeleteJobRun(t *testing.T) {
	t.Run("DeletesEveryObjectUnderTheBackupDir", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1"})

		objs := []storage.ObjectAttr{
			{Key: "root/backup1/meta/full_meta.json", Length: 10},
			{Key: "root/backup1/data/1.parquet", Length: 100},
			{Key: "root/backup1/data/2.parquet", Length: 200},
		}
		// The prefix is listed twice concurrently: once by the counting
		// lister, once by the delete itself.
		cli.EXPECT().
			NewObjectIter(mock.Anything, "root/backup1/", true).
			Return(storage.NewMockObjectIterator(objs)).
			Times(2)
		for _, obj := range objs {
			cli.EXPECT().DeleteObject(mock.Anything, obj.Key).Return(nil)
		}

		store := jobstate.NewStore()
		job, err := newDeleteJob(context.Background(), store, cli, "root",
			DeleteBackupRequest{TaskID: "task-1", BackupName: "backup1"})
		require.NoError(t, err)

		// Run is fire-and-forget; the outcome is read back through Wait.
		job.Run(context.Background())
		status, err := job.Wait(context.Background())
		require.NoError(t, err)
		assert.Equal(t, jobstate.DeleteStateSuccess, status.State)
		assert.Equal(t, int64(len(objs)), status.Discovered)
		assert.Equal(t, int64(len(objs)), status.Deleted)
		assert.True(t, status.ListingDone)
		assert.True(t, status.Terminal())
	})

	t.Run("FailsWhenListingFails", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1"})
		// Both the lister's and the delete's listing fail; either one failing
		// must fail the whole job.
		cli.EXPECT().
			NewObjectIter(mock.Anything, "root/backup1/", true).
			Return(errorSeq(errors.New("connection closed"))).
			Times(2)

		store := jobstate.NewStore()
		job, err := newDeleteJob(context.Background(), store, cli, "root",
			DeleteBackupRequest{TaskID: "task-1", BackupName: "backup1"})
		require.NoError(t, err)

		job.Run(context.Background())
		status, err := job.Wait(context.Background())
		require.NoError(t, err)
		assert.Equal(t, jobstate.DeleteStateFail, status.State)
		assert.Contains(t, status.ErrorMessage, "connection closed")
	})
}

func TestDeleteJobWait(t *testing.T) {
	t.Run("StopsWhenContextCanceled", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1"})

		store := jobstate.NewStore()
		job, err := newDeleteJob(context.Background(), store, cli, "root",
			DeleteBackupRequest{TaskID: "task-1", BackupName: "backup1"})
		require.NoError(t, err)

		// The job never runs, so only the context can release the wait.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = job.Wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestNewDeleteJob(t *testing.T) {
	t.Run("RefusesWhenMetaUnreadable", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		// The meta exist check fails, so the job is never registered; an
		// unexpected DeleteObject call would fail the mock.
		cli.EXPECT().
			NewObjectIter(mock.Anything, "root/backup1/meta/full_meta.json", false).
			Return(errorSeq(errors.New("stat denied")))

		store := jobstate.NewStore()
		_, err := newDeleteJob(context.Background(), store, cli, "root",
			DeleteBackupRequest{TaskID: "task-1", BackupName: "backup1"})

		assert.ErrorContains(t, err, "stat denied")
		_, err = store.GetDeleteTask("task-1")
		assert.ErrorIs(t, err, jobstate.ErrTaskNotFound)
	})

	t.Run("RefusesDuplicateNameInFlight", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1"})

		store := jobstate.NewStore()
		_, err := store.AddDeleteTask("task-0", "backup1")
		require.NoError(t, err)

		// The name already has a delete in flight, so registration refuses
		// the job before anything is deleted.
		_, err = newDeleteJob(context.Background(), store, cli, "root",
			DeleteBackupRequest{TaskID: "task-1", BackupName: "backup1"})
		assert.ErrorContains(t, err, "backup1")
	})
}
