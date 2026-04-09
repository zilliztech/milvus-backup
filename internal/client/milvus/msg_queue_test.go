package milvus

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/stretchr/testify/assert"
)

// newImmutableForTest builds a minimal ImmutableMessage carrying the given
// time-tick in the same property the production code reads from.
func newImmutableForTest(tt uint64) *commonpb.ImmutableMessage {
	return &commonpb.ImmutableMessage{
		Properties: map[string]string{
			"_tt": message.EncodeUint64(tt),
		},
	}
}

func TestMsgQueue_EnqueueAndReadInOrder(t *testing.T) {
	q := newMsgQueue(8)

	for i := uint64(1); i <= 4; i++ {
		assert.NoError(t, q.Enqueue(context.Background(), newImmutableForTest(i)))
	}
	assert.Equal(t, 4, q.Len())

	for i := uint64(1); i <= 4; i++ {
		got, err := q.ReadNext(context.Background())
		assert.NoError(t, err)
		tt, _ := GetTT(got)
		assert.Equal(t, i, tt)
	}
}

func TestMsgQueue_CleanupConfirmedDropsPrefix(t *testing.T) {
	q := newMsgQueue(8)

	for i := uint64(1); i <= 5; i++ {
		assert.NoError(t, q.Enqueue(context.Background(), newImmutableForTest(i)))
	}

	dropped := q.CleanupConfirmedMessages(3)
	assert.Equal(t, 3, dropped)
	assert.Equal(t, 2, q.Len())

	got, err := q.ReadNext(context.Background())
	assert.NoError(t, err)
	tt, _ := GetTT(got)
	assert.Equal(t, uint64(4), tt)
}

func TestMsgQueue_SeekToHeadReplays(t *testing.T) {
	q := newMsgQueue(8)

	for i := uint64(1); i <= 3; i++ {
		assert.NoError(t, q.Enqueue(context.Background(), newImmutableForTest(i)))
	}

	// Drain via the read cursor; storage stays intact.
	for i := 0; i < 3; i++ {
		_, err := q.ReadNext(context.Background())
		assert.NoError(t, err)
	}
	assert.Equal(t, 3, q.Len())

	q.SeekToHead()

	// All three messages should be readable again from the head.
	for i := uint64(1); i <= 3; i++ {
		got, err := q.ReadNext(context.Background())
		assert.NoError(t, err)
		tt, _ := GetTT(got)
		assert.Equal(t, i, tt)
	}
}

func TestMsgQueue_CleanupAdjustsReadCursor(t *testing.T) {
	q := newMsgQueue(8)

	for i := uint64(1); i <= 5; i++ {
		assert.NoError(t, q.Enqueue(context.Background(), newImmutableForTest(i)))
	}
	// Advance read cursor past msgs 1, 2, 3.
	for i := 0; i < 3; i++ {
		_, _ = q.ReadNext(context.Background())
	}

	// Cleanup confirms tt<=2; cursor should now point at the new index of msg 3.
	q.CleanupConfirmedMessages(2)
	assert.Equal(t, 3, q.Len())

	got, err := q.ReadNext(context.Background())
	assert.NoError(t, err)
	tt, _ := GetTT(got)
	assert.Equal(t, uint64(4), tt, "cursor should resume after the dropped prefix")
}

func TestMsgQueue_EnqueueBlocksWhenFull(t *testing.T) {
	q := newMsgQueue(2)

	assert.NoError(t, q.Enqueue(context.Background(), newImmutableForTest(1)))
	assert.NoError(t, q.Enqueue(context.Background(), newImmutableForTest(2)))

	// Third Enqueue must block until cleanup frees a slot.
	enqueued := make(chan error, 1)
	go func() {
		enqueued <- q.Enqueue(context.Background(), newImmutableForTest(3))
	}()

	select {
	case <-enqueued:
		t.Fatal("Enqueue should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	q.CleanupConfirmedMessages(1)

	select {
	case err := <-enqueued:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Enqueue did not unblock after cleanup")
	}
}

func TestMsgQueue_EnqueueRespectsContextCancel(t *testing.T) {
	q := newMsgQueue(1)
	assert.NoError(t, q.Enqueue(context.Background(), newImmutableForTest(1)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- q.Enqueue(ctx, newImmutableForTest(2))
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Enqueue did not return after ctx cancel")
	}
}

func TestMsgQueue_ReadNextRespectsContextCancel(t *testing.T) {
	q := newMsgQueue(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := q.ReadNext(ctx)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("ReadNext did not return after ctx cancel")
	}
}

func TestMsgQueue_CloseUnblocksWaiters(t *testing.T) {
	q := newMsgQueue(1)

	done := make(chan error, 1)
	go func() {
		_, err := q.ReadNext(context.Background())
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	q.Close()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, errQueueClosed)
	case <-time.After(time.Second):
		t.Fatal("ReadNext did not return after Close")
	}
}
