package pbconv

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
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
	taskView := taskmgr.NewMockRestoreCollTaskView(t)
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

	collTaskView := taskmgr.NewMockRestoreCollTaskView(t)
	collTaskView.EXPECT().ID().Return("id1")
	collTaskView.EXPECT().StateCode().Return(backuppb.RestoreTaskStateCode_INITIAL)
	collTaskView.EXPECT().ErrorMessage().Return("error message")
	collTaskView.EXPECT().StartTime().Return(now)
	collTaskView.EXPECT().EndTime().Return(now)
	collTaskView.EXPECT().Progress().Return(int32(100))

	taskView := taskmgr.NewMockRestoreTaskView(t)
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

func TestBase64MsgPosition(t *testing.T) {
	position := &msgpb.MsgPosition{
		ChannelName: "channel",
		MsgID:       []byte("msg_id"),
		Timestamp:   10,
		MsgGroup:    "msg_group",
	}

	str, err := Base64MsgPosition(position)
	assert.NoError(t, err)
	decodeByte, err := base64.StdEncoding.DecodeString(str)
	assert.NoError(t, err)
	decodePos := &msgpb.MsgPosition{}
	err = proto.Unmarshal(decodeByte, decodePos)
	assert.NoError(t, err)

	assert.Equal(t, position.ChannelName, decodePos.ChannelName)
	assert.Equal(t, position.MsgID, decodePos.MsgID)
	assert.Equal(t, position.Timestamp, decodePos.Timestamp)
	assert.Equal(t, position.MsgGroup, decodePos.MsgGroup)
}

func TestBase64DecodeMsgPosition(t *testing.T) {
	position := &msgpb.MsgPosition{
		ChannelName: "channel",
		MsgID:       []byte("msg_id"),
		Timestamp:   10,
		MsgGroup:    "msg_group",
	}

	posStr, err := Base64MsgPosition(position)
	assert.NoError(t, err)

	decodePos, err := Base64DecodeMsgPosition(posStr)
	assert.NoError(t, err)

	assert.Equal(t, position.ChannelName, decodePos.ChannelName)
	assert.Equal(t, position.MsgID, decodePos.MsgID)
	assert.Equal(t, position.Timestamp, decodePos.Timestamp)
	assert.Equal(t, position.MsgGroup, decodePos.MsgGroup)
}
