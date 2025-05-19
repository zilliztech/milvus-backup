package pbconv

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/meta/taskmgr"
	"github.com/zilliztech/milvus-backup/core/mocks"
	"github.com/zilliztech/milvus-backup/core/namespace"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestBakKVToMilvusKV(t *testing.T) {
	kvs := []*backuppb.KeyValuePair{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	expect := []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	res := BakKVToMilvusKV(kvs)
	assert.Len(t, res, len(expect))
	assert.ElementsMatch(t, res, expect)

	skip := []string{"key1"}
	expect = []*commonpb.KeyValuePair{{Key: "key2", Value: "value2"}}
	res = BakKVToMilvusKV(kvs, skip...)
	assert.Len(t, res, len(kvs)-len(skip))
	assert.ElementsMatch(t, res, expect)
}

func TestMilvusKVToBakKV(t *testing.T) {
	kvs := []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	expect := []*backuppb.KeyValuePair{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	res := MilvusKVToBakKV(kvs)

	assert.Len(t, res, len(expect))
	assert.ElementsMatch(t, res, expect)
}

func TestMilvusKVToMap(t *testing.T) {
	kvs := []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	expect := map[string]string{"key1": "value1", "key2": "value2"}
	res := MilvusKVToMap(kvs)

	assert.Len(t, res, len(expect))
	assert.Equal(t, res, expect)
}

func TestRestoreCollTaskViewToResp(t *testing.T) {
	now := time.Now()
	ns := namespace.New("db1", "coll1")
	taskView := mocks.NewMockRestoreCollTaskView(t)
	taskView.EXPECT().ID().Return("id1")
	taskView.EXPECT().StateCode().Return(backuppb.RestoreTaskStateCode_INITIAL)
	taskView.EXPECT().ErrorMessage().Return("error message")
	taskView.EXPECT().StartTime().Return(now)
	taskView.EXPECT().EndTime().Return(now)
	taskView.EXPECT().Progress().Return(int32(100))

	res := RestoreCollTaskViewToResp(ns, taskView)
	assert.Equal(t, res.Id, "id1")
	assert.Equal(t, res.StateCode, backuppb.RestoreTaskStateCode_INITIAL)
	assert.Equal(t, res.ErrorMessage, "error message")
	assert.Equal(t, res.StartTime, now.Unix())
	assert.Equal(t, res.EndTime, now.Unix())
	assert.Equal(t, res.Progress, int32(100))
}

func TestRestoreTaskViewToResp(t *testing.T) {
	now := time.Now()

	collTaskView := mocks.NewMockRestoreCollTaskView(t)
	collTaskView.EXPECT().ID().Return("id1")
	collTaskView.EXPECT().StateCode().Return(backuppb.RestoreTaskStateCode_INITIAL)
	collTaskView.EXPECT().ErrorMessage().Return("error message")
	collTaskView.EXPECT().StartTime().Return(now)
	collTaskView.EXPECT().EndTime().Return(now)
	collTaskView.EXPECT().Progress().Return(int32(100))

	taskView := mocks.NewMockRestoreTaskView(t)
	taskView.EXPECT().ID().Return("id1")
	taskView.EXPECT().StateCode().Return(backuppb.RestoreTaskStateCode_INITIAL)
	taskView.EXPECT().ErrorMessage().Return("error message")
	taskView.EXPECT().StartTime().Return(now)
	taskView.EXPECT().EndTime().Return(now)
	taskView.EXPECT().Progress().Return(int32(100))
	taskView.EXPECT().CollTasks().Return(map[namespace.NS]taskmgr.RestoreCollTaskView{
		namespace.New("db1", "coll1"): collTaskView,
	})

	res := RestoreTaskViewToResp(taskView)
	assert.Equal(t, res.Id, "id1")
	assert.Equal(t, res.StateCode, backuppb.RestoreTaskStateCode_INITIAL)
	assert.Equal(t, res.ErrorMessage, "error message")
	assert.Equal(t, res.StartTime, now.Unix())
	assert.Equal(t, res.EndTime, now.Unix())
	assert.Equal(t, res.Progress, int32(100))

	assert.Len(t, res.CollectionRestoreTasks, 1)
	assert.Equal(t, res.CollectionRestoreTasks[0].Id, "id1")
	assert.Equal(t, res.CollectionRestoreTasks[0].StateCode, backuppb.RestoreTaskStateCode_INITIAL)
	assert.Equal(t, res.CollectionRestoreTasks[0].ErrorMessage, "error message")
	assert.Equal(t, res.CollectionRestoreTasks[0].StartTime, now.Unix())
	assert.Equal(t, res.CollectionRestoreTasks[0].EndTime, now.Unix())
	assert.Equal(t, res.CollectionRestoreTasks[0].Progress, int32(100))
}
