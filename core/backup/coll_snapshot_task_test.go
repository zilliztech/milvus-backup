package backup

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/cfg"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/snapshotbundle"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// snapshotTaskFixture seeds a real milvus-side source snapshot (referenced
// layout, written through the bundle library) on a local milvus store, and
// wires a snapshot task against local milvus and backup stores.
type snapshotTaskFixture struct {
	milvusDir     string
	backupDir     string
	bundleDir     string
	exportDir     string
	exportRootURI string
	sourceS3      string
	binlogPath    string
	statsPath     string
	binlogData    []byte
	statsData     []byte
}

func newSnapshotTaskFixture(t *testing.T) *snapshotTaskFixture {
	t.Helper()

	ctx := context.Background()
	f := &snapshotTaskFixture{
		milvusDir:  t.TempDir(),
		backupDir:  t.TempDir(),
		binlogData: []byte("binlog-bytes"),
		statsData:  []byte("stats-bytes"),
	}
	f.bundleDir = filepath.Join(f.backupDir, "bundle")
	f.exportDir = filepath.Join(f.bundleDir, "exports", "test-export")
	f.exportRootURI = "http://minio:9000/backup/mybackup/bundle/exports/test-export"
	f.binlogPath = filepath.Join(f.milvusDir, "files", "insert_log", "db1", "coll1", "1001", "101", "1.parquet")
	f.statsPath = filepath.Join(f.milvusDir, "files", "stats_log", "db1", "coll1", "1001", "101", "2.parquet")

	milvusStore := bundleStore{cli: newLocalTestClient(t, f.milvusDir)}
	source := &snapshotbundle.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 2, CollectionId: 1, Name: "mbk_mybackup"},
		Collection: &datapb.CollectionDescription{
			NumShards: 1,
			// The internal schema the server writes: every field, $meta included,
			// where DescribeCollection hides it.
			Schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
					{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
					{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
					{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
					{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true, Nullable: true},
				},
			},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:      1001,
				PartitionId:    2001,
				StorageVersion: 2,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: f.binlogPath, LogSize: int64(len(f.binlogData))}}},
				},
				Statslogs: []*datapb.FieldBinlog{
					{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: f.statsPath, LogSize: int64(len(f.statsData))}}},
				},
			},
		},
		Layout:     datapb.SnapshotLayout_SnapshotLayoutReferenced,
		SegmentIDs: []int64{1001},
	}
	source.SnapshotInfo.S3Location = filepath.Join(f.milvusDir, "snapshots", "1", "metadata", "2.json")
	f.sourceS3 = source.SnapshotInfo.GetS3Location()

	// The server persists a referenced snapshot with a direct write, so the
	// fixture does the same rather than going through the export staging path.
	_, _, err := snapshotbundle.SaveBundle(ctx, milvusStore, source, f.milvusDir, datapb.SnapshotLayout_SnapshotLayoutReferenced)
	require.NoError(t, err)

	require.NoError(t, milvusStore.Write(ctx, f.binlogPath, f.binlogData))
	require.NoError(t, milvusStore.Write(ctx, f.statsPath, f.statsData))
	return f
}

func newLocalTestClient(t *testing.T, root string) storage.Client {
	t.Helper()
	cli, err := storage.NewClient(context.Background(), storage.Config{Provider: cfg.ProviderLocal})
	require.NoError(t, err)
	return cli
}

func (f *snapshotTaskFixture) newTask(t *testing.T, grpc milvus.Grpc) (*collSnapshotTask, *metaBuilder, *jobstate.BackupTracker) {
	t.Helper()

	mgr := jobstate.NewStore()
	tracker, err := mgr.AddBackupTask("task-1", "mybackup")
	require.NoError(t, err)

	builder := newMetaBuilder("task-1", "mybackup")
	collRef := collref.New("db1", "coll1")
	builder.addCollection(collRef, &backuppb.CollectionBackupInfo{CollectionId: 1, CollectionName: collRef.CollName()})

	task := &collSnapshotTask{
		taskID:         "task-1",
		collRef:        collRef,
		snapshotName:   "mbk_mybackup",
		target:         snapshotTarget{dir: f.bundleDir, rootURI: "http://minio:9000/backup/mybackup/bundle"},
		exportDir:      f.exportDir,
		exportRootURI:  f.exportRootURI,
		milvusStorage:  newLocalTestClient(t, f.milvusDir),
		milvusRootPath: filepath.Join(f.milvusDir, "files"),
		backupStore:    bundleStore{cli: newLocalTestClient(t, f.backupDir)},
		streaming:      true,
		copySem:        semaphore.NewWeighted(4),
		tracker:        tracker.NewCollDMLTracker(collRef),
		metaBuilder:    builder,
		grpc:           grpc,
		logger:         zap.NewNop(),
	}
	return task, builder, tracker
}

func TestCollSnapshotTask_Execute(t *testing.T) {
	collRef := collref.New("db1", "coll1")

	t.Run("Success", func(t *testing.T) {
		f := newSnapshotTaskFixture(t)
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup", time.Duration(0)).Return(nil)
		cli.EXPECT().DescribeSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").
			Return(&milvuspb.DescribeSnapshotResponse{CreateTs: 456, S3Location: f.sourceS3}, nil)
		cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").Return(nil).Once()

		task, builder, tracker := f.newTask(t, cli)
		require.NoError(t, task.Execute(context.Background()))

		// Every referenced file landed under the bundle's files subtree with
		// its bytes intact.
		copied, err := task.backupStore.Read(context.Background(), filepath.Join(f.exportDir, "files", "insert_log", "db1", "coll1", "1001", "101", "1.parquet"))
		require.NoError(t, err)
		assert.Equal(t, f.binlogData, copied)

		// The published bundle loads back through the library, and its paths
		// point into the bundle.
		snapshot, err := snapshotbundle.LoadSnapshotData(
			context.Background(), task.backupStore,
			filepath.Join(f.exportDir, "snapshots", "1", "metadata", "2.json"), true)
		require.NoError(t, err)
		assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutSelfContained, snapshot.Layout)
		// The bundle's self-location must name the real metadata object: the
		// export root URI already carries exports/<namespace>, so the relative
		// key past it is snapshots/... only.
		assert.Equal(t,
			f.exportRootURI+"/snapshots/1/metadata/2.json",
			snapshot.SnapshotInfo.GetS3Location())
		require.Len(t, snapshot.Segments, 1)
		assert.Equal(t,
			filepath.Join(f.exportDir, "files", "insert_log", "db1", "coll1", "1001", "101", "1.parquet"),
			snapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())

		coll := builder.data.GetCollectionBackups()[0]
		assert.Equal(t, "bundle/exports/test-export/snapshots/1/metadata/2.json", coll.GetSnapshotBackup().GetMetadataPath())
		assert.EqualValues(t, 2, coll.GetSnapshotBackup().GetTotalFiles())
		assert.EqualValues(t, len(f.binlogData)+len(f.statsData), coll.GetSnapshotBackup().GetTotalBytes())
		// The collection size comes from the copied snapshot files, and
		// backup_timestamp from the snapshot's own boundary rather than a flush
		// response.
		assert.EqualValues(t, len(f.binlogData)+len(f.statsData), coll.GetSize())
		assert.EqualValues(t, 456, coll.GetBackupTimestamp())

		dml := tracker.Snapshot().Colls[collRef].DML
		require.NotNil(t, dml)
		assert.Equal(t, jobstate.BackupCollDMLStateSuccess, dml.State)
		assert.Equal(t, int64(len(f.binlogData)+len(f.statsData)), dml.TotalSize)
	})

	// The DDL half's DescribeCollection never returns $meta; the bundle's own
	// schema does, and the task records it on the collection — no etcd needed.
	t.Run("RecordsDynamicFieldFromBundleSchema", func(t *testing.T) {
		f := newSnapshotTaskFixture(t)
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup", time.Duration(0)).Return(nil)
		cli.EXPECT().DescribeSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").
			Return(&milvuspb.DescribeSnapshotResponse{CreateTs: 456, S3Location: f.sourceS3}, nil)
		cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").Return(nil).Once()

		task, builder, _ := f.newTask(t, cli)
		coll := builder.data.GetCollectionBackups()[0]
		coll.Schema = &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields: []*backuppb.FieldSchema{
				{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: backuppb.DataType_Int64},
				{FieldID: 101, Name: "vec", DataType: backuppb.DataType_FloatVector},
			},
		}
		require.NoError(t, task.Execute(context.Background()))

		fields := coll.GetSchema().GetFields()
		require.Len(t, fields, 3)
		assert.Equal(t, "$meta", fields[2].GetName())
		assert.Equal(t, int64(102), fields[2].GetFieldID())
		assert.True(t, fields[2].GetIsDynamic())
		assert.True(t, fields[2].GetNullable())
		assert.Empty(t, builder.collectionsMissingDynField())
	})

	// A source file that vanished between snapshot creation and the copy fails
	// the backup before anything is copied, and the snapshot still has to go.
	t.Run("MissingSourceFileDropsSnapshot", func(t *testing.T) {
		f := newSnapshotTaskFixture(t)
		require.NoError(t, newLocalTestClient(t, f.milvusDir).DeleteObject(context.Background(), f.statsPath))

		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup", time.Duration(0)).Return(nil)
		cli.EXPECT().DescribeSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").
			Return(&milvuspb.DescribeSnapshotResponse{CreateTs: 456, S3Location: f.sourceS3}, nil)
		cli.EXPECT().DropSnapshot(mock.Anything, "db1", "coll1", "mbk_mybackup").Return(nil).Once()

		task, builder, tracker := f.newTask(t, cli)
		err := task.Execute(context.Background())
		assert.ErrorContains(t, err, "head snapshot file")
		assert.Nil(t, builder.data.GetCollectionBackups()[0].GetSnapshotBackup())

		dml := tracker.Snapshot().Colls[collRef].DML
		require.NotNil(t, dml)
		assert.Equal(t, jobstate.BackupCollDMLStateFail, dml.State)
	})
}
