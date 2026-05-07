package milvus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

const (
	// pchReconnectInitialBackoff and pchReconnectMaxBackoff control the delay
	// between reconnect attempts when the gRPC stream fails.
	pchReconnectInitialBackoff = 100 * time.Millisecond
	pchReconnectMaxBackoff     = 10 * time.Second
)

// Stream is the replicate stream client used by the secondary restore path.
//
// Send/Forward enqueue into a per-pchannel in-memory replay buffer; a
// background sender goroutine drains the buffer onto the current gRPC stream
// and replays from the head on every reconnect. Messages are removed only
// when the broker confirms their time-tick, so an in-flight gRPC failure no
// longer drops data.
type Stream interface {
	// Send atomically allocates a monotonic time-tick `ts`, invokes build(ts)
	// to construct messages (the caller may use ts for body timestamps such
	// as MsgBase.Timestamp), then stamps each returned message with its own
	// fresh wire time-tick (ts, ts+1, ts+2, ...). Per-msg wire tt avoids
	// collisions when a broadcast routes multiple messages to the same
	// pchannel — the broker dedups by (pch, wire tt). Routes by pchannel and
	// enqueues. build runs under the dispatch lock — keep it short.
	//
	// Returns once every message is buffered (not when the broker has
	// confirmed it).
	Send(ctx context.Context, build func(ts uint64) []message.MutableMessage) error

	// Forward enqueues pre-built immutable messages on their matching
	// pchannels without re-stamping their time-ticks. Used to replay messages
	// whose time-ticks were assigned by the source cluster (e.g. flush-all
	// messages captured at backup time).
	Forward(ctx context.Context, msgs ...*commonpb.ImmutableMessage) error

	// WaitConfirm blocks until every pchannel has drained its replay buffer,
	// i.e. the broker has confirmed every enqueued message.
	WaitConfirm()

	// Close stops every per-pchannel reconnect loop and releases resources.
	// Safe to call multiple times.
	Close()
}

type StreamClient struct {
	pchClient map[string]*pchClient

	// dispatchMu serializes Send so (alloc wire tt + build + stamp + enqueue)
	// is atomic, keeping per-pchannel wire time-ticks monotonic across
	// concurrent callers without any external locking.
	dispatchMu sync.Mutex
	ttCounter  uint64 // guarded by dispatchMu

	ctx        context.Context
	cancelFunc context.CancelFunc
	closeOnce  sync.Once
}

func NewStreamClient(srcClusterID, taskID string, pch []string, grpc Grpc) *StreamClient {
	ctx, cancel := context.WithCancel(context.Background())

	pchClients := make(map[string]*pchClient, len(pch))
	for _, p := range pch {
		pchClients[p] = newPchClient(ctx, srcClusterID, taskID, p, grpc)
	}

	return &StreamClient{
		pchClient:  pchClients,
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func (s *StreamClient) Send(ctx context.Context, build func(ts uint64) []message.MutableMessage) error {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()

	s.ttCounter++
	bodyTS := s.ttCounter

	msgs := build(bodyTS)
	if len(msgs) == 0 {
		return nil
	}

	byPch := make(map[string][]*commonpb.ImmutableMessage)
	for i, msg := range msgs {
		// Reuse bodyTS for the first message; alloc fresh tts for the rest so
		// that broadcasts whose msgs collide on the same pchannel don't share
		// a (pch, tt) pair (broker dedups by it).
		wireTS := bodyTS
		if i > 0 {
			s.ttCounter++
			wireTS = s.ttCounter
		}

		imm := msg.WithTimeTick(wireTS).
			WithLastConfirmed(NewFakeMessageID(wireTS)).
			IntoImmutableMessage(NewFakeMessageID(wireTS)).
			IntoImmutableMessageProto()

		pch := GetPch(imm)
		if pch == "" {
			return fmt.Errorf("stream: no pch in message")
		}
		byPch[pch] = append(byPch[pch], imm)
	}

	for pch, pchMsgs := range byPch {
		cli, ok := s.pchClient[pch]
		if !ok {
			return fmt.Errorf("stream: no pch client for %s", pch)
		}
		for _, m := range pchMsgs {
			log.Debug("stream: send message", zap.Object("msg", newMsgLogObject(m)))
		}
		if err := cli.enqueue(ctx, pchMsgs...); err != nil {
			return fmt.Errorf("stream: send message: %w", err)
		}
	}

	return nil
}

func (s *StreamClient) Forward(ctx context.Context, msgs ...*commonpb.ImmutableMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	byPch := make(map[string][]*commonpb.ImmutableMessage)
	for _, msg := range msgs {
		log.Debug("stream: forward message", zap.Object("msg", newMsgLogObject(msg)))

		pch := GetPch(msg)
		if pch == "" {
			return fmt.Errorf("stream: no pch in message")
		}
		byPch[pch] = append(byPch[pch], msg)
	}

	for pch, pchMsgs := range byPch {
		cli, ok := s.pchClient[pch]
		if !ok {
			return fmt.Errorf("stream: no pch client for %s", pch)
		}
		if err := cli.enqueue(ctx, pchMsgs...); err != nil {
			return fmt.Errorf("stream: forward message: %w", err)
		}
	}

	return nil
}

func (s *StreamClient) WaitConfirm() {
	for _, cli := range s.pchClient {
		cli.waitConfirm()
	}
}

// Close cancels every pchannel's reconnect loop and releases its resources.
// Safe to call multiple times.
func (s *StreamClient) Close() {
	s.closeOnce.Do(func() {
		s.cancelFunc()
		for _, cli := range s.pchClient {
			cli.wait()
		}
	})
}

// pchClient owns one replicate stream to a single pchannel. It is structured
// as three cooperating goroutines:
//
//   - runForever: outer reconnect loop with exponential backoff. Opens a new
//     gRPC stream, rewinds the queue read cursor, spawns sendLoop+recvLoop,
//     waits for either to fail, then reconnects.
//
//   - sendLoop: drains the queue and writes messages to the current stream.
//     On gRPC error it returns and the outer loop reconnects.
//
//   - recvLoop: reads ConfirmedTimeTick acks from the current stream and
//     drops the matching prefix from the queue. On gRPC error it returns and
//     the outer loop reconnects.
//
// Producers (enqueue) only touch the queue and never block on the network.
type pchClient struct {
	sourceClusterID string
	pch             string
	grpc            Grpc

	queue msgQueue

	ctx        context.Context
	finishedCh chan struct{}

	logger *zap.Logger
}

func newPchClient(ctx context.Context, sourceClusterID, taskID, pch string, grpc Grpc) *pchClient {
	p := &pchClient{
		sourceClusterID: sourceClusterID,
		pch:             pch,
		grpc:            grpc,

		queue: newMemMsgQueue(),

		ctx:        ctx,
		finishedCh: make(chan struct{}),

		logger: log.With(zap.String("task_id", taskID), zap.String("pch", pch)),
	}
	go p.runForever()
	return p
}

func (p *pchClient) enqueue(ctx context.Context, msgs ...*commonpb.ImmutableMessage) error {
	return p.queue.Enqueue(ctx, msgs...)
}

func (p *pchClient) waitConfirm() {
	if err := p.queue.WaitEmpty(p.ctx); err != nil {
		p.logger.Warn("wait confirm aborted", zap.Error(err))
	}
}

func (p *pchClient) wait() {
	<-p.finishedCh
}

func (p *pchClient) runForever() {
	defer close(p.finishedCh)

	backoff := pchReconnectInitialBackoff
	for {
		if p.ctx.Err() != nil {
			return
		}

		established := p.runOneConnection()

		if p.ctx.Err() != nil {
			return
		}

		if established {
			backoff = pchReconnectInitialBackoff
			continue
		}

		p.logger.Warn("replicate stream reconnect", zap.Duration("backoff", backoff))
		select {
		case <-time.After(backoff):
		case <-p.ctx.Done():
			return
		}
		backoff *= 2
		if backoff > pchReconnectMaxBackoff {
			backoff = pchReconnectMaxBackoff
		}
	}
}

// runOneConnection opens one gRPC stream, runs sendLoop+recvLoop until either
// fails, then tears down. Returns true if the connection actually established
// (so the outer loop resets backoff), false otherwise.
func (p *pchClient) runOneConnection() (established bool) {
	connCtx, connCancel := context.WithCancel(p.ctx)
	defer connCancel()

	cli, err := p.grpc.CreateReplicateStream(connCtx, p.sourceClusterID)
	if err != nil {
		p.logger.Warn("create replicate stream failed", zap.Error(err))
		return false
	}
	defer func() {
		if err := cli.CloseSend(); err != nil {
			p.logger.Debug("close stream send", zap.Error(err))
		}
	}()

	p.logger.Info("replicate stream connected")

	// Rewind the read cursor so any unconfirmed messages from the previous
	// connection are replayed on this fresh stream in time-tick order.
	p.queue.SeekToHead()

	sendErrCh := make(chan error, 1)
	recvErrCh := make(chan error, 1)
	go func() {
		sendErrCh <- p.sendLoop(connCtx, cli)
		close(sendErrCh)
	}()
	go func() {
		recvErrCh <- p.recvLoop(connCtx, cli)
		close(recvErrCh)
	}()

	var loopErr error
	select {
	case <-p.ctx.Done():
	case loopErr = <-sendErrCh:
	case loopErr = <-recvErrCh:
	}

	connCancel()
	// Drain both channels so two loops never race over the same gRPC client
	// across reconnects. Each goroutine writes exactly once and closes the
	// channel, so the receive below returns the zero value (nil error)
	// immediately once both loops have exited, even for whichever channel
	// the select above already consumed.
	<-sendErrCh
	<-recvErrCh

	if loopErr != nil && !errors.Is(loopErr, context.Canceled) {
		p.logger.Warn("replicate stream loop failed", zap.Error(loopErr))
	}
	return true
}

func (p *pchClient) sendLoop(ctx context.Context, cli milvuspb.MilvusService_CreateReplicateStreamClient) error {
	for {
		msg, err := p.queue.ReadNext(ctx)
		if err != nil {
			return err
		}
		if err := cli.Send(p.newReq(msg)); err != nil {
			return fmt.Errorf("stream: send: %w", err)
		}
	}
}

func (p *pchClient) recvLoop(ctx context.Context, cli milvuspb.MilvusService_CreateReplicateStreamClient) error {
	for {
		resp, err := cli.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("stream: recv: %w", err)
		}
		confirmedTT := resp.GetReplicateConfirmedMessageInfo().GetConfirmedTimeTick()
		if confirmedTT == 0 {
			continue
		}
		dropped := p.queue.Confirm(confirmedTT)
		if dropped > 0 {
			p.logger.Debug("recv confirm",
				zap.Uint64("confirmed_tt", confirmedTT),
				zap.Int("dropped", dropped))
		}
	}
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
