package secondary

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var _ message.MessageID = (*fakeMessageID)(nil)

type fakeMessageID struct {
	tt uint64
}

func newFakeMessageID(tt uint64) *fakeMessageID {
	return &fakeMessageID{tt: tt}
}

func (f *fakeMessageID) WALName() message.WALName {
	return message.WALNameKafka
}

func (f *fakeMessageID) LT(id message.MessageID) bool {
	if f.tt < id.(*fakeMessageID).tt {
		return true
	}

	return false
}

func (f *fakeMessageID) LTE(id message.MessageID) bool {
	if f.tt <= id.(*fakeMessageID).tt {
		return true
	}

	return false
}

func (f *fakeMessageID) EQ(id message.MessageID) bool {
	if f.tt == id.(*fakeMessageID).tt {
		return true
	}

	return false
}

func (f *fakeMessageID) Marshal() string {
	return fmt.Sprintf("%d", f.tt)
}

func (f *fakeMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		Id:      message.EncodeUint64(f.tt),
		WALName: commonpb.WALName_Kafka,
	}
}

func (f *fakeMessageID) String() string {
	return fmt.Sprintf("%d", f.tt)
}
