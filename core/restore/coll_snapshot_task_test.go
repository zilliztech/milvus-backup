package restore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func newTestCollSnapshotTask(t *testing.T, grpcCli milvus.Grpc, dropExist bool) *collSnapshotTask {
	target := collref.New("db2", "coll2")

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
		target:     target,
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
		assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, view.CollTasks()[task.target].StateCode())
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

	// The description cannot be written into the bundle's schema, so an override is applied
	// with an AlterCollection once the restore job completes.
	t.Run("DescOverride", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)
		cli.EXPECT().AlterCollection(mock.Anything, "db2", "coll2", mock.Anything).
			RunAndReturn(func(_ context.Context, db, collName string, props []*commonpb.KeyValuePair) error {
				assert.Len(t, props, 1)
				assert.Equal(t, common.CollectionDescription, props[0].GetKey())
				assert.Equal(t, "new desc", props[0].GetValue())
				return nil
			})

		task := newTestCollSnapshotTask(t, cli, false)
		task.descOverride = "new desc"
		require.NoError(t, task.Execute(context.Background()))
	})

	// Without an override, the collection keeps the description from the bundle and nothing
	// is altered after the restore.
	t.Run("DescOverrideEmpty", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)

		task := newTestCollSnapshotTask(t, cli, false)
		require.NoError(t, task.Execute(context.Background()))
	})

	// A failed alter surfaces through the task like any other restore failure.
	t.Run("DescOverrideAlterFails", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)
		cli.EXPECT().AlterCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("alter failed"))

		task := newTestCollSnapshotTask(t, cli, false)
		task.descOverride = "new desc"
		err := task.Execute(context.Background())
		assert.ErrorContains(t, err, "alter collection description")
	})

	// Skipped params ride along in the bundle's schema, so they are dropped with
	// delete_keys after the job completes, only from the objects the meta says carry them.
	t.Run("SkipParams", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)
		cli.EXPECT().DropCollectionProperties(mock.Anything, "db2", "coll2", []string{"mmap.enabled"}).
			Return(nil).Once()
		// Only the vector field carries the skipped key, so only it is altered.
		cli.EXPECT().DropCollectionFieldProperties(mock.Anything, "db2", "coll2", "vec", []string{"mmap.enabled"}).
			Return(nil).Once()
		cli.EXPECT().DropIndexProperties(mock.Anything, "db2", "coll2", "idx_vec", []string{"mmap.enabled"}).
			Return(nil).Once()

		task := newTestCollSnapshotTask(t, cli, false)
		task.collBackup.Schema = &backuppb.CollectionSchema{
			Properties: []*backuppb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}},
			Fields: []*backuppb.FieldSchema{
				{Name: "pk", TypeParams: []*backuppb.KeyValuePair{{Key: "max_length", Value: "64"}}},
				{Name: "vec", TypeParams: []*backuppb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}}},
			},
		}
		task.collBackup.IndexInfos = []*backuppb.IndexInfo{
			{IndexName: "idx_vec", Params: map[string]string{"mmap.enabled": "true"}},
			{IndexName: "idx_other", Params: map[string]string{"nlist": "128"}},
		}
		task.skipParams = SkipParams{
			CollectionProperties: []string{"mmap.enabled"},
			FieldTypeParams:      []string{"mmap.enabled"},
			FieldIndexParams:     []string{"mmap.enabled"},
		}
		require.NoError(t, task.Execute(context.Background()))
	})

	// A key the backup never recorded is not deleted: dropping an absent override would be
	// a no-op on the server, so no call is made at all.
	t.Run("SkipParamsAbsentFromMeta", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)

		task := newTestCollSnapshotTask(t, cli, false)
		task.collBackup.Schema = &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{
				{Name: "vec", TypeParams: []*backuppb.KeyValuePair{{Key: "dim", Value: "128"}}},
			},
		}
		task.skipParams = SkipParams{
			CollectionProperties: []string{"mmap.enabled"},
			FieldTypeParams:      []string{"mmap.enabled"},
			IndexParams:          []string{"mmap.enabled"},
		}
		require.NoError(t, task.Execute(context.Background()))
	})

	// A failed drop surfaces through the task like any other restore failure.
	t.Run("SkipParamsDropFails", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil)
		cli.EXPECT().DropCollectionProperties(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("drop failed"))

		task := newTestCollSnapshotTask(t, cli, false)
		task.collBackup.Schema = &backuppb.CollectionSchema{
			Properties: []*backuppb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}},
		}
		task.skipParams = SkipParams{CollectionProperties: []string{"mmap.enabled"}}
		err := task.Execute(context.Background())
		assert.ErrorContains(t, err, "drop skipped collection properties")
	})
}
