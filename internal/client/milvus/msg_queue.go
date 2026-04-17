package milvus

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// msgQueue is an in-memory replay buffer for replicate messages.
//
// It keeps two cursors:
//   - the storage head, advanced only by Confirm when the broker acknowledges a
//     time-tick
//   - the read cursor, advanced by ReadNext as the sender ships messages on the
//     current gRPC stream
//
// On reconnect the owner calls SeekToHead to rewind the read cursor so every
// still-unconfirmed message is re-sent in time-tick order on the new stream.
type msgQueue interface {
	Enqueue(ctx context.Context, msgs ...*commonpb.ImmutableMessage) error
	ReadNext(ctx context.Context) (*commonpb.ImmutableMessage, error)
	Confirm(confirmedTT uint64) int
	SeekToHead()
	WaitEmpty(ctx context.Context) error
}

type memMsgQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	isEmpty  *sync.Cond

	buf     []*commonpb.ImmutableMessage
	readIdx int
}

func newMemMsgQueue() *memMsgQueue {
	q := &memMsgQueue{}
	q.notEmpty = sync.NewCond(&q.mu)
	q.isEmpty = sync.NewCond(&q.mu)
	return q
}

func (q *memMsgQueue) Enqueue(_ context.Context, msgs ...*commonpb.ImmutableMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.buf = append(q.buf, msgs...)
	q.notEmpty.Broadcast()
	return nil
}

func (q *memMsgQueue) ReadNext(ctx context.Context) (*commonpb.ImmutableMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.readIdx >= len(q.buf) {
		if err := waitCondCtx(ctx, q.notEmpty); err != nil {
			return nil, err
		}
	}

	m := q.buf[q.readIdx]
	q.readIdx++
	return m, nil
}

// Confirm drops every message whose time-tick is <= confirmedTT and returns
// the number of messages removed. The read cursor is adjusted by the same
// amount, clamped at 0.
func (q *memMsgQueue) Confirm(confirmedTT uint64) int {
	q.mu.Lock()
	defer q.mu.Unlock()

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

	if len(q.buf) == 0 {
		q.isEmpty.Broadcast()
	}
	return cut
}

// SeekToHead rewinds the read cursor so the next ReadNext returns the oldest
// unconfirmed message. Called from the reconnect path before a new sender
// starts on a fresh stream.
func (q *memMsgQueue) SeekToHead() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.readIdx = 0
	if len(q.buf) > 0 {
		q.notEmpty.Broadcast()
	}
}

func (q *memMsgQueue) WaitEmpty(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.buf) > 0 {
		if err := waitCondCtx(ctx, q.isEmpty); err != nil {
			return err
		}
	}
	return nil
}

// waitCondCtx parks on cond until it is signaled or ctx is canceled. The
// caller must hold cond.L. A watcher goroutine broadcasts on ctx.Done so the
// parked caller wakes up and observes the cancellation.
func waitCondCtx(ctx context.Context, cond *sync.Cond) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cond.L.Lock()
			cond.Broadcast()
			cond.L.Unlock()
		case <-stop:
		}
	}()

	cond.Wait()
	close(stop)

	return ctx.Err()
}
