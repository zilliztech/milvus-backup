package pbconv

import (
	"encoding/base64"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

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
