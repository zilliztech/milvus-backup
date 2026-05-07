package milvus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/stretchr/testify/assert"
)

func newMsg(tt uint64) *commonpb.ImmutableMessage {
	return &commonpb.ImmutableMessage{
		Properties: map[string]string{
			_messageKeyTT: message.EncodeUint64(tt),
		},
	}
}

func msgTT(t *testing.T, m *commonpb.ImmutableMessage) uint64 {
	t.Helper()
	tt, err := GetTT(m)
	assert.NoError(t, err)
	return tt
}

func TestMemMsgQueue_EnqueueReadNext(t *testing.T) {
	q := newMemMsgQueue()
	ctx := context.Background()

	assert.NoError(t, q.Enqueue(ctx, newMsg(1), newMsg(2), newMsg(3)))

	for i := uint64(1); i <= 3; i++ {
		m, err := q.ReadNext(ctx)
		assert.NoError(t, err)
		assert.Equal(t, i, msgTT(t, m))
	}
}

func TestMemMsgQueue_ReadNextBlocksUntilEnqueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemMsgQueue()

		got := make(chan uint64, 1)
		go func() {
			m, err := q.ReadNext(context.Background())
			assert.NoError(t, err)
			got <- msgTT(t, m)
		}()

		// ReadNext must be durably blocked on the cond — nothing in got yet.
		synctest.Wait()
		select {
		case <-got:
			t.Fatal("ReadNext returned before Enqueue")
		default:
		}

		assert.NoError(t, q.Enqueue(context.Background(), newMsg(42)))
		synctest.Wait()
		assert.Equal(t, uint64(42), <-got)
	})
}

func TestMemMsgQueue_ReadNextCancelsOnCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemMsgQueue()
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			_, err := q.ReadNext(ctx)
			done <- err
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()
		assert.ErrorIs(t, <-done, context.Canceled)
	})
}

func TestMemMsgQueue_ConfirmDropsPrefixAndAdjustsReadIdx(t *testing.T) {
	q := newMemMsgQueue()
	ctx := context.Background()

	assert.NoError(t, q.Enqueue(ctx, newMsg(1), newMsg(2), newMsg(3), newMsg(4)))
	// Read two messages so readIdx = 2.
	_, _ = q.ReadNext(ctx)
	_, _ = q.ReadNext(ctx)
	assert.Equal(t, 2, q.readIdx)

	// Confirm tt=3 — drops the first 3 messages; readIdx clamps to 0.
	dropped := q.Confirm(3)
	assert.Equal(t, 3, dropped)
	assert.Len(t, q.buf, 1)
	assert.Equal(t, 0, q.readIdx)

	// The remaining message is tt=4.
	m, err := q.ReadNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), msgTT(t, m))
}

func TestMemMsgQueue_ConfirmNoopOnUnknownTT(t *testing.T) {
	q := newMemMsgQueue()
	ctx := context.Background()

	assert.NoError(t, q.Enqueue(ctx, newMsg(10), newMsg(20)))
	assert.Equal(t, 0, q.Confirm(5)) // lower than any buffered tt
	assert.Len(t, q.buf, 2)
}

func TestMemMsgQueue_SeekToHeadReplays(t *testing.T) {
	q := newMemMsgQueue()
	ctx := context.Background()

	assert.NoError(t, q.Enqueue(ctx, newMsg(1), newMsg(2)))
	_, _ = q.ReadNext(ctx)
	_, _ = q.ReadNext(ctx)
	// Both consumed, none confirmed → still in buf.
	assert.Equal(t, 2, q.readIdx)

	q.SeekToHead()
	assert.Equal(t, 0, q.readIdx)

	// Replay both.
	m, _ := q.ReadNext(ctx)
	assert.Equal(t, uint64(1), msgTT(t, m))
	m, _ = q.ReadNext(ctx)
	assert.Equal(t, uint64(2), msgTT(t, m))
}

func TestMemMsgQueue_SeekToHeadWakesWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemMsgQueue()
		ctx := context.Background()

		assert.NoError(t, q.Enqueue(ctx, newMsg(1)))
		_, _ = q.ReadNext(ctx) // readIdx = 1, nothing pending

		got := make(chan uint64, 1)
		go func() {
			m, err := q.ReadNext(ctx)
			assert.NoError(t, err)
			got <- msgTT(t, m)
		}()

		synctest.Wait()
		q.SeekToHead()
		synctest.Wait()
		assert.Equal(t, uint64(1), <-got)
	})
}

func TestMemMsgQueue_WaitEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemMsgQueue()
		ctx := context.Background()

		// Empty queue — WaitEmpty returns immediately.
		assert.NoError(t, q.WaitEmpty(ctx))

		assert.NoError(t, q.Enqueue(ctx, newMsg(1), newMsg(2)))

		done := make(chan error, 1)
		go func() { done <- q.WaitEmpty(ctx) }()

		synctest.Wait()
		select {
		case <-done:
			t.Fatal("WaitEmpty returned before queue drained")
		default:
		}

		assert.Equal(t, 2, q.Confirm(2))
		synctest.Wait()
		assert.NoError(t, <-done)
	})
}

func TestMemMsgQueue_WaitEmptyCancelsOnCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemMsgQueue()
		assert.NoError(t, q.Enqueue(context.Background(), newMsg(1)))

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- q.WaitEmpty(ctx) }()

		synctest.Wait()
		cancel()
		synctest.Wait()
		assert.ErrorIs(t, <-done, context.Canceled)
	})
}

// TestMemMsgQueue_ReplayCycle simulates the reconnect flow: enqueue, consume,
// partial confirm, SeekToHead, re-consume the remainder, final confirm.
func TestMemMsgQueue_ReplayCycle(t *testing.T) {
	q := newMemMsgQueue()
	ctx := context.Background()

	assert.NoError(t, q.Enqueue(ctx, newMsg(1), newMsg(2), newMsg(3)))

	// Sender ships all three.
	for range 3 {
		_, err := q.ReadNext(ctx)
		assert.NoError(t, err)
	}

	// Broker confirms only up to tt=1.
	assert.Equal(t, 1, q.Confirm(1))
	assert.Len(t, q.buf, 2)

	// Stream breaks; outer loop calls SeekToHead and replays from tt=2.
	q.SeekToHead()
	m, _ := q.ReadNext(ctx)
	assert.Equal(t, uint64(2), msgTT(t, m))
	m, _ = q.ReadNext(ctx)
	assert.Equal(t, uint64(3), msgTT(t, m))

	// Final confirm drains.
	assert.Equal(t, 2, q.Confirm(3))
	assert.NoError(t, q.WaitEmpty(ctx))
}

// TestMemMsgQueue_ConcurrentProducerConsumer stresses the queue with a
// producer and a consumer running in parallel, then confirms everything.
func TestMemMsgQueue_ConcurrentProducerConsumer(t *testing.T) {
	q := newMemMsgQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 500

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= n; i++ {
			assert.NoError(t, q.Enqueue(ctx, newMsg(i)))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= n; i++ {
			m, err := q.ReadNext(ctx)
			assert.NoError(t, err)
			assert.Equal(t, i, msgTT(t, m), fmt.Sprintf("unexpected tt at seq %d", i))
		}
	}()

	wg.Wait()
	assert.Equal(t, n, q.Confirm(n))
	assert.NoError(t, q.WaitEmpty(ctx))
}
