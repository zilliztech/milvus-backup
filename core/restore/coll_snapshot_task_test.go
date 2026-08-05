package restore

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func newTestCollSnapshotTask(t *testing.T, grpcCli milvus.Grpc, dropExist bool) *collSnapshotTask {
	targetNS := namespace.New("db2", "coll2")

	mgr := taskmgr.NewMgr()
	mgr.AddRestoreTask("task-1")

	collBackup := &backuppb.CollectionBackupInfo{
		DbName:         "db1",
		CollectionName: "coll1",
		Size:           4096,
		SnapshotBackup: &backuppb.SnapshotBackupInfo{
			MetadataPath: "bundle/snapshots/1/metadata/2.json",
		},
	}

	task := newCollSnapshotTask(collSnapshotTaskArgs{
		taskID:     "task-1",
		collBackup: collBackup,
		targetNS:   targetNS,
		source:     snapshotSource{dirURI: "s3://backup-bucket/backup/mybackup", externalSpec: `{"extfs":{}}`},
		dropExist:  dropExist,
		grpcCli:    grpcCli,
		taskMgr:    mgr,
	})
	task.pollInterval = time.Millisecond
	task.logger = zap.NewNop()

	return task
}

func TestCollSnapshotTask_Execute(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, in milvus.RestoreExternalSnapshotInput) (int64, error) {
				// The uri is rebuilt from the backup directory, and the collection is
				// created under its target name rather than the one it was backed up as.
				assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json", in.SnapshotMetadataURI)
				assert.Equal(t, "db2", in.DB)
				assert.Equal(t, "coll2", in.TargetCollectionName)
				assert.Equal(t, `{"extfs":{}}`, in.ExternalSpec)
				return 9001, nil
			})
		// One round still executing, so the poll loop is exercised rather than skipped.
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{
				State:    milvuspb.RestoreSnapshotState_RestoreSnapshotExecuting,
				Progress: 40,
			}, nil).Once()
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{
				State:    milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted,
				Progress: 100,
			}, nil).Once()

		task := newTestCollSnapshotTask(t, cli, false)
		require.NoError(t, task.Execute(context.Background()))

		view, err := task.taskMgr.GetRestoreTask("task-1")
		require.NoError(t, err)
		assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, view.CollTasks()[task.targetNS].StateCode())
	})

	// A failed job is reported through the info rather than as an error from the query.
	t.Run("FailedJob", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{
				State:  milvuspb.RestoreSnapshotState_RestoreSnapshotFailed,
				Reason: "copy failed",
			}, nil)

		task := newTestCollSnapshotTask(t, cli, false)
		err := task.Execute(context.Background())
		assert.ErrorContains(t, err, "copy failed")
	})

	// Milvus creates the collection itself and refuses to restore over an existing one,
	// so drop_exist_collection has to be honored before the job is submitted.
	t.Run("DropExistCollection", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "db2", "coll2").Return(true, nil).Once()
		cli.EXPECT().DropCollection(mock.Anything, "db2", "coll2").Return(nil).Once()
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)

		task := newTestCollSnapshotTask(t, cli, true)
		require.NoError(t, task.Execute(context.Background()))
	})

	t.Run("DropExistCollectionWhenAbsent", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "db2", "coll2").Return(false, nil).Once()
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)

		task := newTestCollSnapshotTask(t, cli, true)
		require.NoError(t, task.Execute(context.Background()))
	})

	// Nothing is submitted for a collection whose bundle the meta never recorded.
	t.Run("NoMetadataPath", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)

		task := newTestCollSnapshotTask(t, cli, false)
		task.collBackup.SnapshotBackup = nil
		assert.Error(t, task.Execute(context.Background()))
	})

	// max_shard_num is a bound, not an exact value: a cap no smaller than the bundle's
	// shard count changes nothing, so it is accepted and the restore proceeds.
	t.Run("MaxShardNumSatisfied", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)

		task := newTestCollSnapshotTask(t, cli, false)
		task.collBackup.ShardsNum = 2
		task.maxShardNum = 4
		require.NoError(t, task.Execute(context.Background()))
	})

	// Milvus creates the collection from the bundle, so a cap that would actually bind
	// cannot be honored: nothing is submitted and the collection is reported failed.
	t.Run("MaxShardNumBinds", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)

		task := newTestCollSnapshotTask(t, cli, false)
		task.collBackup.ShardsNum = 8
		task.maxShardNum = 4
		err := task.Execute(context.Background())
		assert.ErrorContains(t, err, "exceeding max_shard_num")
	})
}
