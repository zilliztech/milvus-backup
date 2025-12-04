package milvus

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

var _ message.MessageID = (*fakeMessageID)(nil)

type fakeMessageID struct {
	tt uint64
}

func newFakeMessageID(tt uint64) *fakeMessageID {
	return &fakeMessageID{tt: tt}
}

func (f *fakeMessageID) WALName() message.WALName {
	return message.WALNameUnknown
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
		Id:      fmt.Sprintf("%d", f.tt),
		WALName: commonpb.WALName_Unknown,
	}
}

func (f *fakeMessageID) String() string {
	return fmt.Sprintf("%d", f.tt)
}

type Stream interface {
	Send(msg message.MutableMessage) error
}

type StreamClient struct {
	cli milvuspb.MilvusService_CreateReplicateStreamClient

	mu sync.Mutex
	tt uint64

	sourceClusterID string
}

func NewStreamClient(srcClusterID string, cli milvuspb.MilvusService_CreateReplicateStreamClient) *StreamClient {
	return &StreamClient{cli: cli, sourceClusterID: srcClusterID}
}

func (s *StreamClient) Send(mutMsg message.MutableMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tt++

	immutableMessage := mutMsg.WithTimeTick(s.tt).
		WithLastConfirmed(newFakeMessageID(s.tt)).
		IntoImmutableMessage(newFakeMessageID(s.tt)).
		IntoImmutableMessageProto()

	req := &milvuspb.ReplicateRequest{
		Request: &milvuspb.ReplicateRequest_ReplicateMessage{
			ReplicateMessage: &milvuspb.ReplicateMessage{
				SourceClusterId: s.sourceClusterID,
				Message: &commonpb.ImmutableMessage{
					Id:         immutableMessage.GetId(),
					Payload:    immutableMessage.GetPayload(),
					Properties: immutableMessage.GetProperties(),
				},
			},
		},
	}

	log.Debug("stream: send message", zap.String("msg", immutableMessage.String()))

	if err := s.cli.Send(req); err != nil {
		return fmt.Errorf("stream: send message: %w", err)
	}

	return nil
}
