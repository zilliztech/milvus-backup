package utils

import (
	"encoding/base64"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

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
