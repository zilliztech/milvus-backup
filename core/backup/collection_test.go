package backup

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/mocks"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
)

func newTestCollectionTask() *CollectionTask { return &CollectionTask{logger: zap.NewNop()} }

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
	st := mocks.NewMockChunkManager(t)
	dir := mpath.MilvusInsertLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	keys := []string{
		mpath.Join(dir, mpath.FieldID(1), mpath.LogID(1)),
		mpath.Join(dir, mpath.FieldID(1), mpath.LogID(2)),
		mpath.Join(dir, mpath.FieldID(2), mpath.LogID(1)),
		mpath.Join(dir, mpath.FieldID(2), mpath.LogID(2)),
	}
	st.EXPECT().
		ListWithPrefix(mock.Anything, "bucket", dir, true).
		Return(keys, []int64{1, 2, 3, 4}, nil)

	ct := newTestCollectionTask()
	ct.milvusStorage = st
	ct.milvusBucket = "bucket"
	ct.milvusRootPath = "base"

	fields, size, err := ct.listInsertLogByListFile(context.Background(), dir)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
	assert.Len(t, fields, 2)

	assert.Len(t, fields[0].GetBinlogs(), 2)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: keys[0], LogSize: 1},
		{LogId: 2, LogPath: keys[1], LogSize: 2},
	})

	assert.Len(t, fields[1].GetBinlogs(), 2)
	assert.Equal(t, fields[1].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: keys[2], LogSize: 3},
		{LogId: 2, LogPath: keys[3], LogSize: 4},
	})
}

func TestCollectionTask_listDeltaLogByListFile(t *testing.T) {
	st := mocks.NewMockChunkManager(t)
	dir := mpath.MilvusDeltaLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	keys := []string{
		mpath.Join(dir, mpath.LogID(1)),
		mpath.Join(dir, mpath.LogID(2)),
		mpath.Join(dir, mpath.LogID(3)),
		mpath.Join(dir, mpath.LogID(4)),
	}
	st.EXPECT().
		ListWithPrefix(mock.Anything, "bucket", dir, true).
		Return(keys, []int64{1, 2, 3, 4}, nil)

	ct := newTestCollectionTask()
	ct.milvusStorage = st
	ct.milvusBucket = "bucket"
	ct.milvusRootPath = "base"

	fields, size, err := ct.listDeltaLogByListFile(context.Background(), dir)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
	assert.Len(t, fields, 1)

	assert.Len(t, fields[0].GetBinlogs(), 4)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: keys[0], LogSize: 1},
		{LogId: 2, LogPath: keys[1], LogSize: 2},
		{LogId: 3, LogPath: keys[2], LogSize: 3},
		{LogId: 4, LogPath: keys[3], LogSize: 4},
	})
}

func TestCollectionTask_listInsertLogByAPI(t *testing.T) {
	st := mocks.NewMockChunkManager(t)
	dir := mpath.MilvusInsertLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	keys := []string{
		mpath.Join(dir, mpath.FieldID(1), mpath.LogID(1)),
		mpath.Join(dir, mpath.FieldID(1), mpath.LogID(2)),
		mpath.Join(dir, mpath.FieldID(2), mpath.LogID(1)),
		mpath.Join(dir, mpath.FieldID(2), mpath.LogID(2)),
	}

	st.EXPECT().
		ListWithPrefix(mock.Anything, "bucket", dir, true).
		Return(keys, []int64{1, 2, 3, 4}, nil)

	ct := newTestCollectionTask()
	ct.milvusStorage = st
	ct.milvusBucket = "bucket"
	ct.milvusRootPath = "base"

	binlogs := []client.BinlogInfo{{FieldID: 1, LogIDs: []int64{1, 2}}, {FieldID: 2, LogIDs: []int64{1, 2}}}
	fields, size, err := ct.listInsertLogByAPI(context.Background(), dir, binlogs)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
	assert.Len(t, fields, 2)

	assert.Len(t, fields[0].GetBinlogs(), 2)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: keys[0], LogSize: 1},
		{LogId: 2, LogPath: keys[1], LogSize: 2},
	})

	assert.Len(t, fields[1].GetBinlogs(), 2)
	assert.Equal(t, fields[1].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: keys[2], LogSize: 3},
		{LogId: 2, LogPath: keys[3], LogSize: 4},
	})
}

func TestCollectionTask_listDeltaLogByAPI(t *testing.T) {
	st := mocks.NewMockChunkManager(t)
	dir := mpath.MilvusDeltaLogDir("base", mpath.CollectionID(1), mpath.PartitionID(2), mpath.SegmentID(3))
	keys := []string{mpath.Join(dir, mpath.LogID(1)), mpath.Join(dir, mpath.LogID(2))}

	st.EXPECT().
		ListWithPrefix(mock.Anything, "bucket", dir, true).
		Return(keys, []int64{1, 2}, nil)

	ct := newTestCollectionTask()
	ct.milvusStorage = st
	ct.milvusBucket = "bucket"
	ct.milvusRootPath = "base"

	binlogs := []client.BinlogInfo{{FieldID: 1, LogIDs: []int64{1, 2}}}
	fields, size, err := ct.listDeltaLogByAPI(context.Background(), dir, binlogs)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), size)
	assert.Len(t, fields, 1)

	assert.Len(t, fields[0].GetBinlogs(), 2)
	assert.Equal(t, fields[0].GetBinlogs(), []*backuppb.Binlog{
		{LogId: 1, LogPath: keys[0], LogSize: 1},
		{LogId: 2, LogPath: keys[1], LogSize: 2},
	})
}
