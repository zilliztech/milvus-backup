package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func newTestCollSnapshotTask(t *testing.T, collRef collref.Name, grpc milvus.Grpc) (*collSnapshotTask, *metaBuilder) {
	mgr := taskmgr.NewMgr()
	require.NoError(t, mgr.AddBackupTask("task-1", "mybackup"))
	mgr.UpdateBackupTask("task-1", taskmgr.AddBackupCollTasks([]collref.Name{collRef}))

	builder := newMetaBuilder("task-1", "mybackup")
	builder.addCollection(collRef, &backuppb.CollectionBackupInfo{CollectionId: 1, CollectionName: collRef.CollName()})

	task := &collSnapshotTask{
		taskID:       "task-1",
		collRef:      collRef,
		snapshotName: "mbk_mybackup",
		target:       snapshotTarget{Path: "s3://backup-bucket/backup/mybackup/bundle", Dir: "bundle"},
		pollInterval: time.Millisecond,
		grpc:         grpc,
		taskMgr:      mgr,
		metaBuilder:  builder,
		logger:       zap.NewNop(),
	}

	return task, builder
}

func TestCollSnapshotTask_Execute(t *testing.T) {
	collRef := collref.New("db1", "coll1")

	t.Run("Success", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup", time.Duration(0)).Return(nil)
		cli.EXPECT().DescribeSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").
			Return(&milvuspb.DescribeSnapshotResponse{CreateTs: 456}, nil)
		cli.EXPECT().ExportSnapshot(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, in milvus.ExportSnapshotInput) (int64, error) {
				assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle", in.TargetPath)
				assert.Equal(t, "mbk_mybackup", in.SnapshotName)
				return 9001, nil
			})
		// One round still executing, so the poll loop is exercised rather than skipped.
		cli.EXPECT().GetExportSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.ExportSnapshotInfo{
				State:       milvuspb.ExportSnapshotState_ExportSnapshotExecuting,
				TotalFiles:  10,
				CopiedFiles: 4,
			}, nil).Once()
		cli.EXPECT().GetExportSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.ExportSnapshotInfo{
				State:               milvuspb.ExportSnapshotState_ExportSnapshotCompleted,
				TotalFiles:          10,
				CopiedFiles:         10,
				TotalBytes:          4096,
				SnapshotMetadataUri: "s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json",
			}, nil).Once()
		cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").Return(nil).Once()

		task, builder := newTestCollSnapshotTask(t, collRef, cli)
		require.NoError(t, task.Execute(context.Background()))

		coll := builder.data.GetCollectionBackups()[0]
		assert.Equal(t, "bundle/snapshots/1/metadata/2.json", coll.GetSnapshotBackup().GetMetadataPath())
		assert.EqualValues(t, 10, coll.GetSnapshotBackup().GetTotalFiles())
		assert.EqualValues(t, 4096, coll.GetSnapshotBackup().GetTotalBytes())
		// The collection size comes from the export job, and backup_timestamp from the
		// snapshot's own boundary rather than a flush response.
		assert.EqualValues(t, 4096, coll.GetSize())
		assert.EqualValues(t, 456, coll.GetBackupTimestamp())
	})

	// A failed job is reported through the info, and the snapshot still has to go.
	t.Run("FailedJobDropsSnapshot", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup", time.Duration(0)).Return(nil)
		cli.EXPECT().DescribeSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").
			Return(&milvuspb.DescribeSnapshotResponse{CreateTs: 456}, nil)
		cli.EXPECT().ExportSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetExportSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.ExportSnapshotInfo{
				State:  milvuspb.ExportSnapshotState_ExportSnapshotFailed,
				Reason: "copy failed",
			}, nil)
		cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").Return(nil).Once()

		task, builder := newTestCollSnapshotTask(t, collRef, cli)
		err := task.Execute(context.Background())
		assert.ErrorContains(t, err, "copy failed")
		assert.Nil(t, builder.data.GetCollectionBackups()[0].GetSnapshotBackup())
	})

	// A bundle written somewhere other than the target cannot be recorded as a path
	// relative to the backup directory, so it is an error rather than a bad record.
	t.Run("MetadataOutsideTarget", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup", time.Duration(0)).Return(nil)
		cli.EXPECT().DescribeSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").
			Return(&milvuspb.DescribeSnapshotResponse{}, nil)
		cli.EXPECT().ExportSnapshot(mock.Anything, mock.Anything).Return(9001, nil)
		cli.EXPECT().GetExportSnapshotState(mock.Anything, int64(9001)).
			Return(&milvuspb.ExportSnapshotInfo{
				State:               milvuspb.ExportSnapshotState_ExportSnapshotCompleted,
				SnapshotMetadataUri: "s3://other-bucket/snapshots/1/metadata/2.json",
			}, nil)
		cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").Return(nil).Once()

		task, _ := newTestCollSnapshotTask(t, collRef, cli)
		assert.Error(t, task.Execute(context.Background()))
	})
}

// DataCoord releases the export's pin on its own reconcile tick, so the first drop
// after a job completes can still be refused.
func TestCollSnapshotTask_DropSnapshotRetries(t *testing.T) {
	collRef := collref.New("db1", "coll1")

	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").
		Return(errors.New("snapshot is pinned")).Once()
	cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").Return(nil).Once()

	task, _ := newTestCollSnapshotTask(t, collRef, cli)
	task.dropSnapshot(context.Background())
}
