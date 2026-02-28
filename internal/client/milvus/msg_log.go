package milvus

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// msgLogObject wraps *commonpb.ImmutableMessage to implement zapcore.ObjectMarshaler,
// providing human-readable debug logging without requiring WAL message ID registration.
type msgLogObject struct {
	msg *commonpb.ImmutableMessage
}

func newMsgLogObject(msg *commonpb.ImmutableMessage) *msgLogObject {
	return &msgLogObject{msg: msg}
}

func (m *msgLogObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	props := m.msg.GetProperties()

	enc.AddString("id", m.msg.GetId().GetId())

	if t := props["_t"]; t != "" {
		enc.AddString("type", unmarshalMsgType(t).String())
	}
	if vc := props["_vc"]; vc != "" {
		enc.AddString("vchannel", vc)
	}
	if tt := props["_tt"]; tt != "" {
		if v, err := message.DecodeUint64(tt); err == nil {
			enc.AddUint64("timetick", v)
		}
	}

	enc.AddInt("payload_size", len(m.msg.GetPayload()))

	m.marshalHeader(enc, props)

	return nil
}

func (m *msgLogObject) marshalHeader(enc zapcore.ObjectEncoder, props map[string]string) {
	h := props["_h"]
	if h == "" {
		return
	}

	msgType := unmarshalMsgType(props["_t"])
	ver := unmarshalVersion(props["_v"])

	typ, ok := message.GetSerializeType(message.NewMessageTypeWithVersion(msgType, ver))
	if !ok {
		return
	}

	header := reflect.New(typ.HeaderType.Elem()).Interface().(proto.Message)
	if err := message.DecodeProto(h, header); err != nil {
		enc.AddString("headerDecodeError", err.Error())
		return
	}

	switch header := header.(type) {
	case *message.InsertMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		segIDs := make([]string, 0, len(header.GetPartitions()))
		rows := make([]string, 0, len(header.GetPartitions()))
		for _, p := range header.GetPartitions() {
			segIDs = append(segIDs, strconv.FormatInt(p.GetSegmentAssignment().GetSegmentId(), 10))
			rows = append(rows, strconv.FormatUint(p.Rows, 10))
		}
		enc.AddString("segmentIDs", strings.Join(segIDs, "|"))
		enc.AddString("rows", strings.Join(rows, "|"))
	case *message.DeleteMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddUint64("rows", header.GetRows())
	case *message.CreateCollectionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *message.DropCollectionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *message.CreatePartitionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("partitionID", header.GetPartitionId())
	case *message.DropPartitionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("partitionID", header.GetPartitionId())
	case *message.CreateSegmentMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("segmentID", header.GetSegmentId())
	case *message.FlushMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("segmentID", header.GetSegmentId())
	case *message.ManualFlushMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *message.CreateDatabaseMessageHeader:
		enc.AddString("dbName", header.GetDbName())
		enc.AddInt64("dbID", header.GetDbId())
	case *message.DropDatabaseMessageHeader:
		enc.AddString("dbName", header.GetDbName())
		enc.AddInt64("dbID", header.GetDbId())
	case *message.CreateIndexMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("fieldID", header.GetFieldId())
		enc.AddInt64("indexID", header.GetIndexId())
		enc.AddString("indexName", header.GetIndexName())
	case *message.ImportMessageHeader:
		// import message header has no useful fields to log
	case *message.AlterLoadConfigMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *message.DropLoadConfigMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	}
}

func unmarshalMsgType(s string) message.MessageType {
	v, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return message.MessageTypeUnknown
	}
	return message.MessageType(v)
}

func unmarshalVersion(s string) message.Version {
	if s == "" {
		return message.VersionOld
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return message.VersionOld
	}
	return message.Version(v)
}
