package milvus

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var _ message.MessageID = (*FakeMessageID)(nil)

type FakeMessageID struct {
	tt uint64
}

func NewFakeMessageID(tt uint64) *FakeMessageID {
	return &FakeMessageID{tt: tt}
}

func (f *FakeMessageID) WALName() message.WALName {
	return message.WALNameKafka
}

func (f *FakeMessageID) LT(id message.MessageID) bool {
	return f.tt < id.(*FakeMessageID).tt
}

func (f *FakeMessageID) LTE(id message.MessageID) bool {
	return f.tt <= id.(*FakeMessageID).tt
}

func (f *FakeMessageID) EQ(id message.MessageID) bool {
	return f.tt == id.(*FakeMessageID).tt
}

func (f *FakeMessageID) Marshal() string {
	return fmt.Sprintf("%d", f.tt)
}

func (f *FakeMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		Id:      message.EncodeUint64(f.tt),
		WALName: commonpb.WALName_Kafka,
	}
}

func (f *FakeMessageID) String() string {
	return fmt.Sprintf("%d", f.tt)
}
