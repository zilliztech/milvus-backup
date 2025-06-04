package backup

import (
	"context"
	"sort"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
)

func newTestCollectionTask() *CollectionTask { return &CollectionTask{logger: zap.NewNop()} }

func newMockStorage(t *testing.T, prefix string, objs []storage.ObjectAttr) storage.Client {
	iter := storage.NewMockObjectIterator(objs)

	st := storage.NewMockClient(t)
	st.EXPECT().
		ListPrefix(mock.Anything, prefix, true).
		Return(iter, nil)
	return st
}

func TestCollectionTask_groupID(t *testing.T) {
	t.Run("NormalSegment", func(t *testing.T) {
		task := newTestCollectionTask()
		groupID := task.groupID(&milvuspb.PersistentSegmentInfo{SegmentID: 100, PartitionID: 200})
		assert.Equal(t, int64(100), groupID)
	})

	t.Run("AllPartitionSegment", func(t *testing.T) {
		task := newTestCollectionTask()
		groupID := task.groupID(&milvuspb.PersistentSegmentInfo{SegmentID: 100, PartitionID: -1})
		assert.Equal(t, int64(0), groupID)
	})
}

func TestCollectionTask_listInsertLogByListFile(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		dir := mpath.MilvusInsertLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
		objs := []storage.ObjectAttr{
			{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(1)), Length: 1},
			{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(2)), Length: 2},
			{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(1)), Length: 3},
			{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(2)), Length: 4},
		}

		st := newMockStorage(t, dir, objs)
		ct := newTestCollectionTask()
		ct.milvusStorage = st
		ct.milvusRootPath = "base"

		fields, size, err := ct.listInsertLogByListFile(context.Background(), dir)
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

		ct := newTestCollectionTask()
		ct.milvusStorage = st
		ct.milvusRootPath = "base"

		_, _, err := ct.listInsertLogByListFile(context.Background(), dir)
		assert.Error(t, err)
	})

}

func TestCollectionTask_listDeltaLogByListFile(t *testing.T) {
	dir := mpath.MilvusDeltaLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	objs := []storage.ObjectAttr{
		{Key: mpath.Join(dir, mpath.LogID(1)), Length: 1},
		{Key: mpath.Join(dir, mpath.LogID(2)), Length: 2},
		{Key: mpath.Join(dir, mpath.LogID(3)), Length: 3},
		{Key: mpath.Join(dir, mpath.LogID(4)), Length: 4},
	}
	st := newMockStorage(t, dir, objs)

	ct := newTestCollectionTask()
	ct.milvusStorage = st
	ct.milvusRootPath = "base"

	fields, size, err := ct.listDeltaLogByListFile(context.Background(), dir)
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

func TestCollectionTask_listInsertLogByAPI(t *testing.T) {
	dir := mpath.MilvusInsertLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	objs := []storage.ObjectAttr{
		{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(1)), Length: 1},
		{Key: mpath.Join(dir, mpath.FieldID(1), mpath.LogID(2)), Length: 2},
		{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(1)), Length: 3},
		{Key: mpath.Join(dir, mpath.FieldID(2), mpath.LogID(2)), Length: 4},
	}

	ct := newTestCollectionTask()
	ct.milvusStorage = newMockStorage(t, dir, objs)
	ct.milvusRootPath = "base"

	binlogs := []client.BinlogInfo{{FieldID: 1, LogIDs: []int64{1, 2}}, {FieldID: 2, LogIDs: []int64{1, 2}}}
	fields, size, err := ct.listInsertLogByAPI(context.Background(), dir, binlogs)
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

func TestCollectionTask_listDeltaLogByAPI(t *testing.T) {
	dir := mpath.MilvusDeltaLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	objs := []storage.ObjectAttr{
		{Key: mpath.Join(dir, mpath.LogID(1)), Length: 1},
		{Key: mpath.Join(dir, mpath.LogID(2)), Length: 2},
	}

	ct := newTestCollectionTask()
	ct.milvusStorage = newMockStorage(t, dir, objs)
	ct.milvusRootPath = "base"

	binlogs := []client.BinlogInfo{{FieldID: 1, LogIDs: []int64{1, 2}}}
	fields, size, err := ct.listDeltaLogByAPI(context.Background(), dir, binlogs)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), size)
	assert.Len(t, fields, 1)

	assert.Len(t, fields[0].GetBinlogs(), 2)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: objs[0].Key, LogSize: 1},
		{LogId: 2, LogPath: objs[1].Key, LogSize: 2},
	})
}
