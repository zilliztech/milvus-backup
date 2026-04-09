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
	// pchQueueCapacity bounds the number of unconfirmed replicate messages
	// buffered per pchannel. Producers block (backpressure) once the buffer is
	// full. The bound exists primarily as a safety net against a stalled
	// broker; in normal operation the broker confirms quickly and the buffer
	// stays small.
	pchQueueCapacity = 4096

	// pchReconnectInitialBackoff and pchReconnectMaxBackoff control the delay
	// between reconnect attempts when the gRPC stream fails.
	pchReconnectInitialBackoff = 100 * time.Millisecond
	pchReconnectMaxBackoff     = 10 * time.Second
)

// Stream is the replicate stream client used by the secondary restore path.
//
// Time-tick allocation lives inside the stream client: callers hand over a
// MutableMessage and the stream client stamps the wire time-tick atomically
// with the dispatch to the matching pchannel. Pre-built immutable messages
// (e.g. flush-all messages whose time-ticks come from the source cluster) are
// passed through Forward without re-stamping.
//
// Each pchannel owns an in-memory replay buffer of unconfirmed messages.
// Send/Forward enqueue into that buffer; a per-pchannel goroutine drains it
// onto the current gRPC stream and replays from the head on every reconnect,
// so an in-flight gRPC failure no longer drops data.
type Stream interface {
	// Alloc returns the next monotonic timestamp from the stream client's
	// internal allocator. Use it for message body timestamps such as
	// MsgBase.Timestamp so that body and wire time-ticks share one axis.
	Alloc() uint64

	// Send stamps a wire time-tick on the mutable message and enqueues it on
	// the pchannel matching the message's vchannel. Returns once the message
	// is in the replay buffer (not when the broker has confirmed it).
	Send(ctx context.Context, msg message.MutableMessage) error

	// Forward enqueues a pre-built immutable message on its pchannel without
	// re-stamping its time-tick. Used to replay messages whose time-ticks were
	// assigned by the source cluster.
	Forward(ctx context.Context, msg *commonpb.ImmutableMessage) error

	// WaitConfirm blocks until every pchannel has either drained its replay
	// buffer (i.e. all messages have been confirmed by the broker) or
	// encountered a permanent error. Returns the first error observed.
	WaitConfirm(ctx context.Context) error

	// Close cancels every pchannel's reconnect loop and releases its
	// resources. Safe to call multiple times.
	Close()
}

type StreamClient struct {
	pchClient map[string]*pchClient

	tsAlloc *tsAlloc

	// dispatchMu serializes (alloc tt + enqueue) so that the per-pchannel
	// time-tick stays monotonic across concurrent callers.
	dispatchMu sync.Mutex

	closeOnce sync.Once
}

func NewStreamClient(srcClusterID, taskID string, pch []string, grpc Grpc) (*StreamClient, error) {
	pchClients := make(map[string]*pchClient, len(pch))
	for _, p := range pch {
		pchCli := newPchClient(srcClusterID, taskID, p, grpc)
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

	if err := cli.enqueue(ctx, immutable); err != nil {
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

	if err := cli.enqueue(ctx, immutable); err != nil {
		return fmt.Errorf("stream: forward message: %w", err)
	}
	return nil
}

func (s *StreamClient) WaitConfirm(ctx context.Context) error {
	for _, cli := range s.pchClient {
		if err := cli.waitConfirm(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *StreamClient) Close() {
	s.closeOnce.Do(func() {
		for _, cli := range s.pchClient {
			cli.close()
		}
	})
}

// pchClient owns one replicate stream to a single pchannel. It is structured
// as three cooperating goroutines:
//
//   - runForever: the outer reconnect loop. Opens a new gRPC stream, rewinds
//     the queue read cursor, spawns sendLoop+recvLoop, waits for either to
//     fail, and starts the next iteration with exponential backoff.
//
//   - sendLoop: drains the queue and writes messages to the current stream.
//     On gRPC error it returns and the outer loop reconnects.
//
//   - recvLoop: reads ConfirmedTimeTick acks from the current stream and
//     drops the matching prefix from the queue. On gRPC error it returns and
//     the outer loop reconnects.
//
// Producers (Send/Forward) only touch the queue via enqueue and never block on
// the network.
type pchClient struct {
	sourceClusterID string
	pch             string
	grpc            Grpc

	queue *msgQueue

	ctx        context.Context
	cancelFunc context.CancelFunc
	finishedCh chan struct{}

	// statusMu protects permErr and broadcasts via statusCond when state
	// transitions that waitConfirm cares about happen (cleanup, perm failure,
	// shutdown).
	statusMu   sync.Mutex
	statusCond *sync.Cond
	permErr    error

	logger *zap.Logger
}

func newPchClient(sourceClusterID, taskID, pch string, grpc Grpc) *pchClient {
	ctx, cancel := context.WithCancel(context.Background())
	p := &pchClient{
		sourceClusterID: sourceClusterID,
		pch:             pch,
		grpc:            grpc,

		queue: newMsgQueue(pchQueueCapacity),

		ctx:        ctx,
		cancelFunc: cancel,
		finishedCh: make(chan struct{}),

		logger: log.With(zap.String("task_id", taskID), zap.String("pch", pch)),
	}
	p.statusCond = sync.NewCond(&p.statusMu)
	go p.runForever()
	return p
}

func (p *pchClient) enqueue(ctx context.Context, msg *commonpb.ImmutableMessage) error {
	p.statusMu.Lock()
	if p.permErr != nil {
		err := p.permErr
		p.statusMu.Unlock()
		return err
	}
	p.statusMu.Unlock()

	return p.queue.Enqueue(ctx, msg)
}

func (p *pchClient) waitConfirm(ctx context.Context) error {
	// A watchdog that wakes the cond on ctx cancellation, so the loop below
	// observes ctx.Err() promptly instead of parking forever.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			p.statusMu.Lock()
			p.statusCond.Broadcast()
			p.statusMu.Unlock()
		case <-stop:
		}
	}()

	p.statusMu.Lock()
	defer p.statusMu.Unlock()

	for {
		if p.permErr != nil {
			return p.permErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if p.queue.Len() == 0 {
			return nil
		}
		p.statusCond.Wait()
	}
}

func (p *pchClient) close() {
	p.cancelFunc()
	p.queue.Close()
	<-p.finishedCh
}

func (p *pchClient) runForever() {
	defer close(p.finishedCh)

	backoff := pchReconnectInitialBackoff
	for {
		if p.ctx.Err() != nil {
			return
		}

		ok := p.runOneConnection()

		if p.ctx.Err() != nil {
			return
		}

		if ok {
			backoff = pchReconnectInitialBackoff
		} else {
			p.logger.Warn("replicate stream connection failed, retrying", zap.Duration("backoff", backoff))
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

	// Rewind the read cursor so that any unconfirmed messages from the previous
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
	// Drain both channels so that two loops never race over the same gRPC
	// client across reconnects. Each channel is closed by its goroutine after
	// the single send, so the receives below return immediately once the
	// loops have exited (zero value if already consumed by the case above).
	<-sendErrCh
	<-recvErrCh

	if loopErr != nil && !errors.Is(loopErr, context.Canceled) {
		p.logger.Warn("replicate stream loop failed", zap.Error(loopErr))
	}
	return true
}

func (p *pchClient) sendLoop(ctx context.Context, cli milvuspb.MilvusService_CreateReplicateStreamClient) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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
		dropped := p.queue.CleanupConfirmedMessages(confirmedTT)
		if dropped > 0 {
			p.logger.Debug("recv confirm",
				zap.Uint64("confirmed_tt", confirmedTT),
				zap.Int("dropped", dropped),
				zap.Int("remaining", p.queue.Len()))
			p.statusMu.Lock()
			p.statusCond.Broadcast()
			p.statusMu.Unlock()
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
