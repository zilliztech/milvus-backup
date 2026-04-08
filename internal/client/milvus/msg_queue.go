package milvus

import (
	"context"
	"errors"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

var errQueueClosed = errors.New("stream: msg queue closed")

// msgQueue is a bounded in-memory replay buffer for replicate messages.
//
// It separates two cursors:
//   - the storage head, which is advanced only when the broker confirms a
//     message via CleanupConfirmedMessages
//   - the read cursor, which sendLoop advances as it ships messages on the
//     current gRPC stream
//
// On reconnect, callers invoke SeekToHead to rewind the read cursor so that
// every still-buffered (i.e. unconfirmed) message is replayed in time-tick
// order on the new connection. The storage head only advances on confirms,
// so as long as the broker eventually checkpoints the messages, the buffer
// drains.
//
// Enqueue blocks when the buffer is full so that producers experience
// backpressure rather than unbounded memory growth.
type msgQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	buf     []*commonpb.ImmutableMessage
	readIdx int
	cap     int
	closed  bool
}

func newMsgQueue(capacity int) *msgQueue {
	if capacity <= 0 {
		panic("msgQueue: capacity must be > 0")
	}
	q := &msgQueue{cap: capacity}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

func (q *msgQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.buf)
}

// Enqueue appends msg to the buffer. Blocks until capacity is available or ctx
// is canceled. Once enqueued, the message is owned by the queue and will be
// shipped on the current connection (and replayed on reconnect) until the
// broker confirms its time-tick.
func (q *msgQueue) Enqueue(ctx context.Context, msg *commonpb.ImmutableMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.buf) >= q.cap && !q.closed {
		if err := q.waitWithCtx(ctx, q.notFull); err != nil {
			return err
		}
	}
	if q.closed {
		return errQueueClosed
	}

	q.buf = append(q.buf, msg)
	q.notEmpty.Signal()
	return nil
}

// ReadNext returns the message at the read cursor and advances it by one. The
// message stays in the buffer until CleanupConfirmedMessages drops it. Blocks
// when the cursor has caught up with the tail.
func (q *msgQueue) ReadNext(ctx context.Context) (*commonpb.ImmutableMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.readIdx >= len(q.buf) && !q.closed {
		if err := q.waitWithCtx(ctx, q.notEmpty); err != nil {
			return nil, err
		}
	}
	if q.closed && q.readIdx >= len(q.buf) {
		return nil, errQueueClosed
	}

	m := q.buf[q.readIdx]
	q.readIdx++
	return m, nil
}

// SeekToHead rewinds the read cursor to the head of the storage so that the
// next ReadNext returns the oldest unconfirmed message. Called from the
// reconnect path before a new sendLoop starts on a fresh connection.
func (q *msgQueue) SeekToHead() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.readIdx = 0
	if len(q.buf) > 0 {
		q.notEmpty.Broadcast()
	}
}

// CleanupConfirmedMessages drops every message whose time-tick is <=
// confirmedTT and returns the number of messages removed. The read cursor is
// adjusted by the same amount, clamped at 0.
func (q *msgQueue) CleanupConfirmedMessages(confirmedTT uint64) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.buf) == 0 {
		return 0
	}

	cut := 0
	for cut < len(q.buf) {
		tt, err := GetTT(q.buf[cut])
		if err != nil || tt > confirmedTT {
			break
		}
		cut++
	}
	if cut == 0 {
		return 0
	}

	for i := 0; i < cut; i++ {
		q.buf[i] = nil
	}
	q.buf = q.buf[cut:]
	q.readIdx -= cut
	if q.readIdx < 0 {
		q.readIdx = 0
	}

	q.notFull.Broadcast()
	return cut
}

// Close marks the queue as closed and wakes every waiter. Subsequent Enqueue
// calls fail; ReadNext drains the remaining messages and then fails.
func (q *msgQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
}

// waitWithCtx blocks on cond until either ctx is canceled or another goroutine
// signals. The mutex must be held by the caller; it is released for the
// duration of the wait and reacquired before return.
func (q *msgQueue) waitWithCtx(ctx context.Context, cond *sync.Cond) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Spawn a watcher that broadcasts the cond when ctx fires so that any
	// goroutine parked on Wait wakes up and observes the cancellation.
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			cond.Broadcast()
			q.mu.Unlock()
		case <-stop:
		}
	}()

	cond.Wait()
	close(stop)

	return ctx.Err()
}
