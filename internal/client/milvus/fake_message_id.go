package milvus

import (
	"encoding/base64"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	wpb "github.com/zilliztech/woodpecker/proto"
	"google.golang.org/protobuf/proto"
)

var _ message.MessageID = (*FakeMessageID)(nil)

// FakeMessageID is a logical message id fabricated by the secondary-restore
// replay stream. The value is only a replay-local counter, but the WAL name
// is contract-carrying: the id travels in the replicate header, and milvus
// persists the header id into the per-source replicate checkpoint that a
// later real CDC resumes the source cluster's WAL from. An id whose WAL name
// does not match the source's WAL makes that resume fail permanently with
// STREAMING_CODE_WALNAME_MISMATCH, so the fabricated id must claim the
// source's WAL kind. Learn it from the backup's flush-all messages with
// SupportedFakeWALName before building a replay stream.
type FakeMessageID struct {
	tt      uint64
	walName message.WALName
}

func NewFakeMessageID(tt uint64, walName message.WALName) *FakeMessageID {
	return &FakeMessageID{tt: tt, walName: walName}
}

// SupportedFakeWALName reports whether FakeMessageID can encode ids for the
// given WAL kind.
func SupportedFakeWALName(walName message.WALName) bool {
	switch walName {
	case message.WALNameKafka,
		message.WALNameRocksmq,
		message.WALNameWoodpecker,
		message.WALNamePulsar:
		return true
	}
	return false
}

func (f *FakeMessageID) WALName() message.WALName {
	return f.walName
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

// Marshal encodes the id exactly the way the named WAL's unmarshaler in
// milvus decodes it, so the id survives the round trip through a real
// cluster: kafka and rocksmq ids are base-36 int64s, woodpecker ids a base64
// protobuf LogMessageId, pulsar ids a base64 serialized bookkeeper message
// id.
func (f *FakeMessageID) Marshal() string {
	switch f.walName {
	case message.WALNameKafka, message.WALNameRocksmq:
		return message.EncodeInt64(int64(f.tt))
	case message.WALNameWoodpecker:
		// Same bytes wp.LogMessageId.Serialize produces: the generated
		// LogMessageIdData proto is the whole wire format. The proto package
		// is imported directly because woodpecker/woodpecker/log drags in the
		// cloud-sdk shims, which do not compile against this module's
		// minio-go.
		raw, err := proto.Marshal(&wpb.LogMessageIdData{SegId: 0, EntryId: int64(f.tt)})
		if err != nil {
			panic(fmt.Sprintf("marshal a woodpecker log message id: %v", err))
		}
		return base64.StdEncoding.EncodeToString(raw)
	case message.WALNamePulsar:
		id := pulsar.NewMessageID(int64(f.tt), 0, -1, -1)
		return base64.StdEncoding.EncodeToString(id.Serialize())
	}
	panic(fmt.Sprintf("unsupported wal name for a fake message id: %s", f.walName))
}

func (f *FakeMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		Id:      f.Marshal(),
		WALName: commonpb.WALName(f.walName),
	}
}

func (f *FakeMessageID) String() string {
	return fmt.Sprintf("fake-%s-%d", f.walName, f.tt)
}
