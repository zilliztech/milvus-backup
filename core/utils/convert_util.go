package utils

import (
	"encoding/base64"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

func Base64MsgPosition(position *msgpb.MsgPosition) string {
	positionByte, err := proto.Marshal(position)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(positionByte)
}
