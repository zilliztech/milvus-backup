package milvus

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/retry"
)

// Stream is the replicate stream client used by the secondary restore path.
//
// Time-tick allocation lives inside the stream client: callers hand over a
// MutableMessage and the stream client stamps the wire time-tick atomically
// with the dispatch to the matching pchannel. Pre-built immutable messages
// (e.g. flush-all messages whose time-ticks come from the source cluster) are
// passed through Forward without re-stamping.
type Stream interface {
	// Alloc returns the next monotonic timestamp from the stream client's
	// internal allocator. Use it for message body timestamps such as
	// MsgBase.Timestamp so that body and wire time-ticks share one axis.
	Alloc() uint64

	// Send stamps a wire time-tick on the mutable message and dispatches it to
	// the pchannel matching the message's vchannel. The (alloc, dispatch) pair
	// is performed atomically with respect to other Send/Forward calls so that
	// per-pchannel time-tick monotonicity is preserved.
	Send(ctx context.Context, msg message.MutableMessage) error

	// Forward dispatches a pre-built immutable message to its pchannel without
	// re-stamping its time-tick. Used to replay messages whose time-ticks were
	// assigned by the source cluster.
	Forward(ctx context.Context, msg *commonpb.ImmutableMessage) error

	// WaitConfirm blocks until every pchannel has received a confirmation
	// covering the highest time-tick that was sent on it.
	WaitConfirm()
}

type StreamClient struct {
	pchClient map[string]*pchClient

	tsAlloc *tsAlloc

	// dispatchMu serializes (alloc tt + dispatch to pchClient) so that the
	// per-pchannel time-tick stays monotonic across concurrent callers.
	dispatchMu sync.Mutex
}

func NewStreamClient(srcClusterID, taskID string, pch []string, grpc Grpc) (*StreamClient, error) {
	pchClients := make(map[string]*pchClient, len(pch))
	for _, p := range pch {
		pchCli, err := newPchClient(srcClusterID, taskID, p, grpc)
		if err != nil {
			return nil, fmt.Errorf("stream: new pch client: %w", err)
		}
		pchClients[p] = pchCli
	}

	s := &StreamClient{
		pchClient: pchClients,
		tsAlloc:   newTSAlloc(),
	}

	return s, nil
}

func (s *StreamClient) Alloc() uint64 {
	return s.tsAlloc.Alloc()
}

func (s *StreamClient) Send(ctx context.Context, msg message.MutableMessage) error {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()

	ts := s.tsAlloc.Alloc()
	id := newFakeMessageID(ts)
	immutable := msg.
		WithTimeTick(ts).
		WithLastConfirmed(id).
		IntoImmutableMessage(id).
		IntoImmutableMessageProto()

	log.Debug("stream: send message", zap.Object("msg", newMsgLogObject(immutable)))

	pch := GetPch(immutable)
	if pch == "" {
		return fmt.Errorf("stream: no pch in message")
	}
	cli, ok := s.pchClient[pch]
	if !ok {
		return fmt.Errorf("stream: no pch client for %s", pch)
	}

	if err := cli.send(ctx, immutable); err != nil {
		return fmt.Errorf("stream: send message: %w", err)
	}

	return nil
}

func (s *StreamClient) Forward(ctx context.Context, immutable *commonpb.ImmutableMessage) error {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()

	log.Debug("stream: forward message", zap.Object("msg", newMsgLogObject(immutable)))

	pch := GetPch(immutable)
	if pch == "" {
		return fmt.Errorf("stream: no pch in message")
	}
	cli, ok := s.pchClient[pch]
	if !ok {
		return fmt.Errorf("stream: no pch client for %s", pch)
	}

	if err := cli.send(ctx, immutable); err != nil {
		return fmt.Errorf("stream: forward message: %w", err)
	}

	return nil
}

func (s *StreamClient) WaitConfirm() {
	for _, cli := range s.pchClient {
		cli.waitConfirm()
	}
}

type pchClient struct {
	sourceClusterID string
	grpc            Grpc

	cli            milvuspb.MilvusService_CreateReplicateStreamClient
	connCancelFunc context.CancelFunc

	cond        *sync.Cond
	mu          sync.Mutex
	sentTT      uint64
	confirmedTT uint64

	logger *zap.Logger
}

func newPchClient(sourceClusterID, taskID, pch string, grpc Grpc) (*pchClient, error) {
	p := &pchClient{
		grpc:            grpc,
		sourceClusterID: sourceClusterID,

		logger: log.With(zap.String("task_id", taskID), zap.String("pch", pch)),
	}
	p.cond = sync.NewCond(&p.mu)

	if err := p.newStreamClient(); err != nil {
		return nil, fmt.Errorf("stream: new stream client: %w", err)
	}

	return p, nil
}

func (p *pchClient) newStreamClient() error {
	if p.connCancelFunc != nil {
		p.connCancelFunc()
	}

	p.logger.Info("create stream client")
	if p.cli != nil {
		if err := p.cli.CloseSend(); err != nil {
			p.logger.Warn("close stream error", zap.Error(err))
		}
	}

	connCtx, cancel := context.WithCancel(context.Background())
	p.connCancelFunc = cancel

	cli, err := p.grpc.CreateReplicateStream(connCtx, p.sourceClusterID)
	if err != nil {
		return fmt.Errorf("create replicate stream: %w", err)
	}

	p.cli = cli
	go p.recvLoop(cli)

	return nil
}

func (p *pchClient) newReq(msg *commonpb.ImmutableMessage) *milvuspb.ReplicateRequest {
	return &milvuspb.ReplicateRequest{
		Request: &milvuspb.ReplicateRequest_ReplicateMessage{
			ReplicateMessage: &milvuspb.ReplicateMessage{
				SourceClusterId: p.sourceClusterID,
				Message:         msg,
			},
		},
	}
}

func (p *pchClient) send(ctx context.Context, msg *commonpb.ImmutableMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tt, err := GetTT(msg)
	if err != nil {
		return fmt.Errorf("stream: get tt: %w", err)
	}

	err = retry.Do(ctx, func() error {
		if err := p.cli.Send(p.newReq(msg)); err != nil {
			p.logger.Warn("send to stream error, try to reconnect", zap.Error(err))

			if err := p.newStreamClient(); err != nil {
				return fmt.Errorf("stream: new stream client: %w", err)
			}

			return fmt.Errorf("stream: send message: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("stream: send message: %w", err)
	}

	p.sentTT = tt

	return nil
}

func (p *pchClient) recvLoop(cli milvuspb.MilvusService_CreateReplicateStreamClient) {
	for {
		resp, err := cli.Recv()
		if err != nil {
			p.logger.Warn("recv from stream error", zap.Error(err))
			return
		}

		confirmTT := resp.GetReplicateConfirmedMessageInfo().GetConfirmedTimeTick()
		p.logger.Info("recv confirm", zap.Uint64("confirmed_tt", confirmTT), zap.Uint64("sent_tt", p.sentTT))
		p.mu.Lock()
		p.confirmedTT = confirmTT
		p.mu.Unlock()
		p.cond.Signal()
	}
}

func (p *pchClient) waitConfirm() {
	p.mu.Lock()

	for p.confirmedTT < p.sentTT {
		p.logger.Info("wait confirm", zap.Uint64("confirmed_tt", p.confirmedTT), zap.Uint64("sent_tt", p.sentTT))
		p.cond.Wait()
	}

	p.mu.Unlock()
}
