package backup

import (
	"context"
	"sort"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
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
		ListPrefix(mock.Anything, prefix, true).
		Return(iter, nil)
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
		partID    = int64(2)
		segID     = int64(3)
		groupID   = int64(3)
		fieldID   = int64(100)
	)

	seg := &backuppb.SegmentBackupInfo{
		CollectionId: collID,
		PartitionId:  partID,
		SegmentId:    segID,
		GroupId:      groupID,
		Binlogs: []*backuppb.FieldBinlog{
			{FieldID: fieldID, Binlogs: []*backuppb.Binlog{
				{LogId: 10, LogPath: "milvus/insert_log/1/2/3/100/10", LogSize: 5},
			}},
		},
		Deltalogs: []*backuppb.FieldBinlog{
			{Binlogs: []*backuppb.Binlog{
				{LogId: 20, LogPath: "milvus/delta_log/1/2/3/20", LogSize: 7},
			}},
		},
	}

	insertKey := mpath.Join(
		mpath.BackupInsertLogDir(backupDir, mpath.CollectionID(collID), mpath.PartitionID(partID), mpath.SegmentID(segID), mpath.GroupID(groupID)),
		mpath.FieldID(fieldID), mpath.LogID(10))
	deltaKey := mpath.Join(
		mpath.BackupDeltaLogDir(backupDir, mpath.CollectionID(collID), mpath.PartitionID(partID), mpath.SegmentID(segID), mpath.GroupID(groupID)),
		mpath.LogID(20))

	insertColl := mpath.BackupInsertLogDir(backupDir, mpath.CollectionID(collID))
	deltaColl := mpath.BackupDeltaLogDir(backupDir, mpath.CollectionID(collID))

	t.Run("AllPresent", func(t *testing.T) {
		st := storage.NewMockClient(t)
		st.EXPECT().ListPrefix(mock.Anything, insertColl, true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: insertKey, Length: 5}}), nil).Once()
		st.EXPECT().ListPrefix(mock.Anything, deltaColl, true).
			Return(storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: deltaKey, Length: 7}}), nil).Once()

		dmlt := newTestCollDMLTask()
		dmlt.backupStorage = st
		dmlt.backupDir = backupDir

		err := dmlt.verifySegmentsData(context.Background(), collID, []*backuppb.SegmentBackupInfo{seg})
		assert.NoError(t, err)
	})

	t.Run("InsertMissing", func(t *testing.T) {
		st := storage.NewMockClient(t)
		st.EXPECT().ListPrefix(mock.Anything, insertColl, true).
			Return(storage.NewMockObjectIterator(nil), nil).Once()

		dmlt := newTestCollDMLTask()
		dmlt.backupStorage = st
		dmlt.backupDir = backupDir

		err := dmlt.verifySegmentsData(context.Background(), collID, []*backuppb.SegmentBackupInfo{seg})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "verify insert logs")
	})
}
