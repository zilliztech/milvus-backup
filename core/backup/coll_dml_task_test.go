package backup

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

func newTestCollDMLTask() *collDMLTask { return &collDMLTask{logger: zap.NewNop()} }

func TestCollDDLTask_groupID(t *testing.T) {
	t.Run("NormalSegment", func(t *testing.T) {
		task := newTestCollDMLTask()
		groupID := task.groupID(&milvuspb.PersistentSegmentInfo{SegmentID: 100, PartitionID: 200})
		assert.Equal(t, int64(100), groupID)
	})

	t.Run("AllPartitionSegment", func(t *testing.T) {
		task := newTestCollDMLTask()
		groupID := task.groupID(&milvuspb.PersistentSegmentInfo{SegmentID: 100, PartitionID: -1})
		assert.Equal(t, int64(0), groupID)
	})
}

func newMockStorage(t *testing.T, prefix string, objs []storage.ObjectAttr) storage.Client {
	iter := storage.NewMockObjectIterator(objs)

	st := storage.NewMockClient(t)
	st.EXPECT().
		NewObjectIter(mock.Anything, prefix, true).
		Return(iter)
	return st
}

func TestCollDMLTask_listInsertLogByListFile(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		dir := mpath.MilvusInsertLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
		objs := []storage.ObjectAttr{
			{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(1)), Length: 1},
			{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(2)), Length: 2},
			{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(1)), Length: 3},
			{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(2)), Length: 4},
		}

		st := newMockStorage(t, dir, objs)
		dmlt := newTestCollDMLTask()
		dmlt.milvusStorage = st
		dmlt.milvusRootPath = "base"

		fields, size, err := dmlt.listInsertLogByListFile(context.Background(), dir)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), size)
		assert.Len(t, fields, 2)
		sort.Slice(fields, func(i, j int) bool { return fields[i].GetFieldID() < fields[j].GetFieldID() })
		assert.Len(t, fields[0].GetBinlogs(), 2)
		assert.ElementsMatch(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
			{LogId: 1, LogPath: objs[0].Key, LogSize: 1},
			{LogId: 2, LogPath: objs[1].Key, LogSize: 2},
		})

		assert.Len(t, fields[1].GetBinlogs(), 2)
		assert.ElementsMatch(t, fields[1].GetBinlogs(), []*backuppb.Binlog{
			{LogId: 1, LogPath: objs[2].Key, LogSize: 3},
			{LogId: 2, LogPath: objs[3].Key, LogSize: 4},
		})
	})

	t.Run("FileNumNotEqual", func(t *testing.T) {
		dir := mpath.MilvusInsertLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
		objs := []storage.ObjectAttr{
			{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(1)), Length: 1},
			{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(2)), Length: 2},
			{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(1)), Length: 3},
		}
		st := newMockStorage(t, dir, objs)

		dmlt := newTestCollDMLTask()
		dmlt.milvusStorage = st
		dmlt.milvusRootPath = "base"

		_, _, err := dmlt.listInsertLogByListFile(context.Background(), dir)
		assert.Error(t, err)
	})
}

func TestCollDMLTask_listDeltaLogByListFile(t *testing.T) {
	dir := mpath.MilvusDeltaLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	objs := []storage.ObjectAttr{
		{Key: mpath.Join(dir, mpath.LogID(1)), Length: 1},
		{Key: mpath.Join(dir, mpath.LogID(2)), Length: 2},
		{Key: mpath.Join(dir, mpath.LogID(3)), Length: 3},
		{Key: mpath.Join(dir, mpath.LogID(4)), Length: 4},
	}
	st := newMockStorage(t, dir, objs)

	dmlt := newTestCollDMLTask()
	dmlt.milvusStorage = st
	dmlt.milvusRootPath = "base"

	fields, size, err := dmlt.listDeltaLogByListFile(context.Background(), dir)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
	assert.Len(t, fields, 1)

	assert.Len(t, fields[0].GetBinlogs(), 4)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: objs[0].Key, LogSize: 1},
		{LogId: 2, LogPath: objs[1].Key, LogSize: 2},
		{LogId: 3, LogPath: objs[2].Key, LogSize: 3},
		{LogId: 4, LogPath: objs[3].Key, LogSize: 4},
	})
}

func TestCollDMLTask_listInsertLogByAPI(t *testing.T) {
	dir := mpath.MilvusInsertLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	objs := []storage.ObjectAttr{
		{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(1)), Length: 1},
		{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(2)), Length: 2},
		{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(1)), Length: 3},
		{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(2)), Length: 4},
	}

	dmlt := newTestCollDMLTask()
	dmlt.milvusStorage = newMockStorage(t, dir, objs)
	dmlt.milvusRootPath = "base"

	binlogs := []milvus.BinlogInfo{{FieldID: 1, LogIDs: []int64{1, 2}}, {FieldID: 2, LogIDs: []int64{1, 2}}}
	fields, size, err := dmlt.listInsertLogByAPI(context.Background(), dir, binlogs)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
	assert.Len(t, fields, 2)

	assert.Len(t, fields[0].GetBinlogs(), 2)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: objs[0].Key, LogSize: 1},
		{LogId: 2, LogPath: objs[1].Key, LogSize: 2},
	})

	assert.Len(t, fields[1].GetBinlogs(), 2)
	assert.Equal(t, fields[1].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: objs[2].Key, LogSize: 3},
		{LogId: 2, LogPath: objs[3].Key, LogSize: 4},
	})
}

func TestCollDMLTask_listDeltaLogByAPI(t *testing.T) {
	dir := mpath.MilvusDeltaLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	objs := []storage.ObjectAttr{
		{Key: mpath.Join(dir, mpath.LogID(1)), Length: 1},
		{Key: mpath.Join(dir, mpath.LogID(2)), Length: 2},
	}

	dmlt := newTestCollDMLTask()
	dmlt.milvusStorage = newMockStorage(t, dir, objs)
	dmlt.milvusRootPath = "base"

	binlogs := []milvus.BinlogInfo{{FieldID: 1, LogIDs: []int64{1, 2}}}
	fields, size, err := dmlt.listDeltaLogByAPI(context.Background(), dir, binlogs)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), size)
	assert.Len(t, fields, 1)

	assert.Len(t, fields[0].GetBinlogs(), 2)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: objs[0].Key, LogSize: 1},
		{LogId: 2, LogPath: objs[1].Key, LogSize: 2},
	})
}

func TestCollDMLTask_verifySegmentsData(t *testing.T) {
	const (
		backupDir = "bak"
		collID    = int64(1)
		fieldID   = int64(100)
	)

	newSeg := func(partID, segID, insertLogID, deltaLogID int64) *backuppb.SegmentBackupInfo {
		return &backuppb.SegmentBackupInfo{
			CollectionId: collID,
			PartitionId:  partID,
			SegmentId:    segID,
			GroupId:      segID,
			Binlogs: []*backuppb.FieldBinlog{
				{FieldID: fieldID, Binlogs: []*backuppb.Binlog{
					{LogId: insertLogID, LogSize: 5},
				}},
			},
			Deltalogs: []*backuppb.FieldBinlog{
				{Binlogs: []*backuppb.Binlog{
					{LogId: deltaLogID, LogSize: 7},
				}},
			},
		}
	}
	insertKey := func(seg *backuppb.SegmentBackupInfo, logID int64) string {
		return mpath.Join(
			mpath.BackupInsertLogDir(backupDir, mpath.CollectionID(collID), mpath.PartitionID(seg.GetPartitionId()), mpath.SegmentID(seg.GetSegmentId()), mpath.GroupID(seg.GetGroupId())),
			mpath.FieldID(fieldID), mpath.LogID(logID))
	}
	deltaKey := func(seg *backuppb.SegmentBackupInfo, logID int64) string {
		return mpath.Join(
			mpath.BackupDeltaLogDir(backupDir, mpath.CollectionID(collID), mpath.PartitionID(seg.GetPartitionId()), mpath.SegmentID(seg.GetSegmentId()), mpath.GroupID(seg.GetGroupId())),
			mpath.LogID(logID))
	}
	insertPart := func(partID int64) string {
		return mpath.BackupInsertLogDir(backupDir, mpath.CollectionID(collID), mpath.PartitionID(partID))
	}
	deltaPart := func(partID int64) string {
		return mpath.BackupDeltaLogDir(backupDir, mpath.CollectionID(collID), mpath.PartitionID(partID))
	}

	segA := newSeg(2, 3, 10, 20)
	segB := newSeg(4, 5, 11, 21)

	newTask := func(st storage.Client) *collDMLTask {
		dmlt := newTestCollDMLTask()
		dmlt.backupStorage = st
		dmlt.backupDir = backupDir
		return dmlt
	}

	t.Run("AllPresent", func(t *testing.T) {
		st := storage.NewMockClient(t)
		st.EXPECT().NewObjectIter(mock.Anything, insertPart(2), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: insertKey(segA, 10), Length: 5}})).Once()
		st.EXPECT().NewObjectIter(mock.Anything, deltaPart(2), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: deltaKey(segA, 20), Length: 7}})).Once()

		err := newTask(st).verifySegmentsData(context.Background(), collID, []*backuppb.SegmentBackupInfo{segA})
		assert.NoError(t, err)
	})

	t.Run("ListsEachPartitionNotCollection", func(t *testing.T) {
		st := storage.NewMockClient(t)
		// the collection dir itself must never be listed
		st.EXPECT().NewObjectIter(mock.Anything, insertPart(2), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: insertKey(segA, 10), Length: 5}})).Once()
		st.EXPECT().NewObjectIter(mock.Anything, insertPart(4), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: insertKey(segB, 11), Length: 5}})).Once()
		st.EXPECT().NewObjectIter(mock.Anything, deltaPart(2), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: deltaKey(segA, 20), Length: 7}})).Once()
		st.EXPECT().NewObjectIter(mock.Anything, deltaPart(4), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: deltaKey(segB, 21), Length: 7}})).Once()

		err := newTask(st).verifySegmentsData(context.Background(), collID, []*backuppb.SegmentBackupInfo{segA, segB})
		assert.NoError(t, err)
	})

	t.Run("InsertMissing", func(t *testing.T) {
		st := storage.NewMockClient(t)
		st.EXPECT().NewObjectIter(mock.Anything, insertPart(2), true).
			Return(storage.NewMockObjectIterator(nil)).Once()

		err := newTask(st).verifySegmentsData(context.Background(), collID, []*backuppb.SegmentBackupInfo{segA})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "verify insert logs")
	})

	t.Run("MissingInOnePartition", func(t *testing.T) {
		st := storage.NewMockClient(t)
		st.EXPECT().NewObjectIter(mock.Anything, insertPart(2), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: insertKey(segA, 10), Length: 5}})).Maybe()
		st.EXPECT().NewObjectIter(mock.Anything, insertPart(4), true).
			Return(storage.NewMockObjectIterator(nil)).Once()

		err := newTask(st).verifySegmentsData(context.Background(), collID, []*backuppb.SegmentBackupInfo{segA, segB})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "verify insert logs")
		assert.Contains(t, err.Error(), insertPart(4)) // the failing partition prefix is in the error
	})

	t.Run("DeltaSizeMismatch", func(t *testing.T) {
		st := storage.NewMockClient(t)
		st.EXPECT().NewObjectIter(mock.Anything, insertPart(2), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: insertKey(segA, 10), Length: 5}})).Once()
		st.EXPECT().NewObjectIter(mock.Anything, deltaPart(2), true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: deltaKey(segA, 20), Length: 1}})).Once()

		err := newTask(st).verifySegmentsData(context.Background(), collID, []*backuppb.SegmentBackupInfo{segA})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "verify delta logs")
	})

	t.Run("NoSegments", func(t *testing.T) {
		st := storage.NewMockClient(t) // no list expected

		err := newTask(st).verifySegmentsData(context.Background(), collID, nil)
		assert.NoError(t, err)
	})
}

func TestGroupByChildPrefix(t *testing.T) {
	dir := "bak/binlogs/insert_log/1/"

	t.Run("GroupByFirstLevel", func(t *testing.T) {
		groups := groupByChildPrefix(dir, map[string]int64{
			dir + "2/3/3/100/10": 1,
			dir + "2/4/4/100/11": 2,
			dir + "-1/0/5/12":    3,
		})
		assert.Equal(t, map[string]map[string]int64{
			dir + "2/": {
				dir + "2/3/3/100/10": 1,
				dir + "2/4/4/100/11": 2,
			},
			dir + "-1/": {
				dir + "-1/0/5/12": 3,
			},
		}, groups)
	})

	t.Run("FallbackToDir", func(t *testing.T) {
		groups := groupByChildPrefix(dir, map[string]int64{
			dir + "file":             1, // no sub dir below dir
			"other/insert_log/1/2/x": 2, // not under dir
		})
		assert.Equal(t, map[string]map[string]int64{
			dir: {
				dir + "file":             1,
				"other/insert_log/1/2/x": 2,
			},
		}, groups)
	})

	t.Run("Empty", func(t *testing.T) {
		assert.Empty(t, groupByChildPrefix(dir, nil))
	})
}

func TestCollDMLTask_Execute(t *testing.T) {
	collRef := collref.New("db1", "coll1")

	newArgs := func(t *testing.T, cli milvus.Grpc, backupStorage storage.Client) (collTaskArgs, *jobstate.BackupTracker) {
		mgr := jobstate.NewStore()
		tracker, err := mgr.AddBackupTask("task-1", "mybackup")
		require.NoError(t, err)

		return collTaskArgs{
			TaskID:        "task-1",
			tracker:       tracker,
			MetaBuilder:   newMetaBuilder("task-1", "mybackup"),
			Grpc:          cli,
			BackupStorage: backupStorage,
			BackupDir:     "bak",
			gcCtrl:        &emptyGCCtrl{},
		}, tracker
	}

	t.Run("SettlesSuccess", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().DescribeCollection(mock.Anything, "db1", "coll1").
			Return(&milvuspb.DescribeCollectionResponse{CollectionID: 1}, nil)
		cli.EXPECT().GetPersistentSegmentInfo(mock.Anything, "db1", "coll1").Return(nil, nil)

		// No segments means nothing to copy, and the verify step short-circuits
		// on an empty expectation set without listing.
		args, tracker := newArgs(t, cli, storage.NewMockClient(t))
		require.NoError(t, newCollDMLTask(collRef, args).Execute(context.Background()))

		dml := tracker.Snapshot().Colls[collRef].DML
		require.NotNil(t, dml)
		assert.Equal(t, jobstate.BackupCollDMLStateSuccess, dml.State)
		assert.Zero(t, dml.TotalSize)
		assert.False(t, dml.EndTime.IsZero())
	})

	t.Run("SettlesFail", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().DescribeCollection(mock.Anything, "db1", "coll1").Return(nil, errors.New("boom"))

		args, tracker := newArgs(t, cli, storage.NewMockClient(t))
		err := newCollDMLTask(collRef, args).Execute(context.Background())
		assert.ErrorContains(t, err, "boom")

		dml := tracker.Snapshot().Colls[collRef].DML
		require.NotNil(t, dml)
		assert.Equal(t, jobstate.BackupCollDMLStateFail, dml.State)
		assert.Contains(t, dml.ErrorMessage, "boom")
	})
}
