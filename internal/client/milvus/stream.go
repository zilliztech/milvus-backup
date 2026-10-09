package milvus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

const (
	// pchReconnectInitialBackoff and pchReconnectMaxBackoff control the delay
	// between reconnect attempts when the gRPC stream fails.
	pchReconnectInitialBackoff = 100 * time.Millisecond
	pchReconnectMaxBackoff     = 10 * time.Second

	// pchMinEstablishedLifetime is how long a stream must stay open, when it
	// confirms nothing, before the reconnect loop treats the attempt as
	// established and resets backoff. An accept-then-immediate close is
	// shorter than this and takes the backoff path.
	pchMinEstablishedLifetime = time.Second

	// pchFailureSummaryInterval rate-limits the channel-level failure report.
	// Per-attempt lines stay at Debug; the Warn is at most one per interval.
	pchFailureSummaryInterval = 5 * time.Second

	// pchNoProgressWarnAfter is how long WaitConfirm stays quiet before
	// warning that a non-empty channel has not been confirmed.
	pchNoProgressWarnAfter = 15 * time.Second
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

	// mu guards the failure streak and the confirm counters. Lock order is
	// mu then the queue mutex: never call into the queue while holding mu.
	mu                  sync.Mutex
	consecutiveFailures int
	failingSince        time.Time
	lastSummaryAt       time.Time
	totalConfirmed      int
	lastConfirmAt       time.Time
	connConfirmed       int // messages this connection's Confirm dropped
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
	done := make(chan error, 1)
	go func() {
		done <- p.queue.WaitEmpty(p.ctx)
	}()

	waitStart := time.Now()
	ticker := time.NewTicker(pchNoProgressWarnAfter)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			if err != nil {
				p.logger.Warn("wait confirm aborted", zap.Error(err))
			}
			return
		case now := <-ticker.C:
			p.warnIfNoProgress(waitStart, now)
		}
	}
}

// warnIfNoProgress logs when the queue is non-empty and this channel has not
// confirmed for pchNoProgressWarnAfter. Silence is measured from lastConfirmAt
// when that is set, otherwise from the later of the wait start and
// failingSince. A confirm already older than the interval does not look
// healthy just because the wait started now, and a zero failingSince does not
// warn a freshly enqueued channel on entry.
func (p *pchClient) warnIfNoProgress(waitStart, now time.Time) {
	p.mu.Lock()
	confirmed := p.totalConfirmed
	lastConfirm := p.lastConfirmAt
	failingSince := p.failingSince
	p.mu.Unlock()

	anchor := waitStart
	if !lastConfirm.IsZero() {
		anchor = lastConfirm
	} else if failingSince.After(anchor) {
		anchor = failingSince
	}
	silent := now.Sub(anchor)
	if silent < pchNoProgressWarnAfter {
		return
	}

	head, n, ok := p.queue.Head()
	if !ok {
		return
	}
	p.logger.Warn("replicate stream made no progress",
		zap.Int("confirmed", confirmed),
		zap.Int("queued", n),
		zap.Duration("no_progress_for", silent),
		zap.Object("head", newMsgLogObject(head)))
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

		established, cause := p.runOneConnection()

		// Parent cancellation is not a failure of the channel. Return before
		// the backoff sleep and before the streak is touched.
		if p.ctx.Err() != nil {
			return
		}

		if established {
			p.resetFailureStreak()
			backoff = pchReconnectInitialBackoff
			continue
		}

		if !errors.Is(cause, context.Canceled) {
			p.noteFailure(cause, backoff)
		}

		p.logger.Debug("replicate stream reconnect", zap.Duration("backoff", backoff))
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
// fails, then tears down. established is true only when this connection
// confirmed at least one message or stayed open for pchMinEstablishedLifetime,
// so the outer loop resets backoff. A stream the peer accepts and closes at
// once is not established and takes the backoff path. cause is the create or
// loop error, nil when the connection ended without one.
func (p *pchClient) runOneConnection() (established bool, cause error) {
	connCtx, connCancel := context.WithCancel(p.ctx)
	defer connCancel()

	p.mu.Lock()
	p.connConfirmed = 0
	p.mu.Unlock()

	cli, err := p.grpc.CreateReplicateStream(connCtx, p.sourceClusterID)
	if err != nil {
		p.logger.Debug("create replicate stream failed", zap.Error(err))
		return false, err
	}
	defer func() {
		if err := cli.CloseSend(); err != nil {
			p.logger.Debug("close stream send", zap.Error(err))
		}
	}()

	openedAt := time.Now()
	p.logger.Debug("replicate stream opened")

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
		p.logger.Debug("replicate stream loop failed", zap.Error(loopErr))
	}

	p.mu.Lock()
	confirmed := p.connConfirmed > 0
	p.mu.Unlock()
	if confirmed || time.Since(openedAt) >= pchMinEstablishedLifetime {
		return true, loopErr
	}
	return false, loopErr
}

// resetFailureStreak clears the consecutive-failure report. Confirm totals are
// kept: they count every message this channel has ever had acknowledged.
func (p *pchClient) resetFailureStreak() {
	p.mu.Lock()
	p.consecutiveFailures = 0
	p.failingSince = time.Time{}
	p.lastSummaryAt = time.Time{}
	p.mu.Unlock()
}

// noteFailure records one non-established attempt. The Warn is the channel
// report — streak length, how long it has lasted, how many messages have been
// confirmed, and the oldest unconfirmed message — emitted on the second
// consecutive failure and then at most once per pchFailureSummaryInterval.
// The first failure is only counted: it can race the producer's enqueue, so
// the head of queue would be missing, and a one-off blip is not the report.
func (p *pchClient) noteFailure(cause error, backoff time.Duration) {
	if errors.Is(cause, context.Canceled) {
		return
	}

	now := time.Now()
	p.mu.Lock()
	p.consecutiveFailures++
	if p.failingSince.IsZero() {
		p.failingSince = now
	}
	failingFor := now.Sub(p.failingSince)
	consecutive := p.consecutiveFailures
	confirmed := p.totalConfirmed
	due := consecutive >= 2 && (p.lastSummaryAt.IsZero() || now.Sub(p.lastSummaryAt) >= pchFailureSummaryInterval)
	if due {
		p.lastSummaryAt = now
	}
	p.mu.Unlock()
	if !due {
		return
	}

	head, n, ok := p.queue.Head()
	fields := []zap.Field{
		zap.Int("consecutive_failures", consecutive),
		zap.Duration("failing_for", failingFor),
		zap.Int("confirmed", confirmed),
		zap.Int("queued", n),
		zap.Duration("backoff", backoff),
		zap.Error(cause),
	}
	if ok {
		fields = append(fields, zap.Object("head", newMsgLogObject(head)))
	}
	p.logger.Warn("replicate stream failing", fields...)
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
			p.observeConfirm(dropped)
			p.logger.Debug("recv confirm",
				zap.Uint64("confirmed_tt", confirmedTT),
				zap.Int("dropped", dropped))
		}
	}
}

// observeConfirm counts messages this connection and this channel have had
// acknowledged. The Info line fires on the first confirm of the connection,
// not when the RPC is merely opened.
func (p *pchClient) observeConfirm(dropped int) {
	p.mu.Lock()
	first := p.connConfirmed == 0
	p.connConfirmed += dropped
	p.totalConfirmed += dropped
	p.lastConfirmAt = time.Now()
	p.mu.Unlock()
	if first {
		p.logger.Info("replicate stream connected")
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
