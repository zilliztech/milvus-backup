package milvus

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

const (
	_messageKeyTT = "_tt"
	_messageVCh   = "_vc"
)

func GetTT(msg *commonpb.ImmutableMessage) (uint64, error) {
	tt := msg.GetProperties()[_messageKeyTT]
	return message.DecodeUint64(tt)
}

func GetVch(msg *commonpb.ImmutableMessage) string {
	return msg.GetProperties()[_messageVCh]
}

func GetPch(msg *commonpb.ImmutableMessage) string {
	vch := GetVch(msg)
	return funcutil.ToPhysicalChannel(vch)
}
