package milvus

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeReplicateStream is a hand-rolled
// milvuspb.MilvusService_CreateReplicateStreamClient implementation that the
// pchClient tests drive directly. Each instance models one underlying gRPC
// stream; reconnect spins up a fresh fakeReplicateStream.
type fakeReplicateStream struct {
	// sentCh receives every successfully Sent request.
	sentCh chan *milvuspb.ReplicateRequest

	// recvCh feeds responses into Recv. Tests push confirms by sending on it.
	recvCh chan *milvuspb.ReplicateResponse

	// stopCh is closed when the stream is "torn down" (either by the test
	// calling closeRecv to simulate a network drop, or by the production code
	// invoking CloseSend on connection teardown). Recv returns recvErr (or
	// io.EOF) once stopCh is closed; Send returns recvErr.
	stopCh   chan struct{}
	stopOnce sync.Once

	// recvErr is the error returned by Recv (and Send) after stopCh closes.
	recvErr atomic.Value // error
}

func newFakeReplicateStream() *fakeReplicateStream {
	return &fakeReplicateStream{
		sentCh: make(chan *milvuspb.ReplicateRequest, 256),
		recvCh: make(chan *milvuspb.ReplicateResponse, 64),
		stopCh: make(chan struct{}),
	}
}

func (f *fakeReplicateStream) Send(req *milvuspb.ReplicateRequest) error {
	select {
	case <-f.stopCh:
		if v := f.recvErr.Load(); v != nil {
			return v.(error)
		}
		return io.EOF
	default:
	}
	f.sentCh <- req
	return nil
}

func (f *fakeReplicateStream) Recv() (*milvuspb.ReplicateResponse, error) {
	select {
	case resp := <-f.recvCh:
		return resp, nil
	case <-f.stopCh:
		if v := f.recvErr.Load(); v != nil {
			return nil, v.(error)
		}
		return nil, io.EOF
	}
}

// closeRecv simulates a network drop. Subsequent Send/Recv calls return err.
func (f *fakeReplicateStream) closeRecv(err error) {
	if err != nil {
		f.recvErr.Store(err)
	}
	f.stopOnce.Do(func() { close(f.stopCh) })
}

// pushConfirm enqueues a ConfirmedTimeTick response to be returned by the next
// Recv call.
func (f *fakeReplicateStream) pushConfirm(tt uint64) {
	f.recvCh <- &milvuspb.ReplicateResponse{
		Response: &milvuspb.ReplicateResponse_ReplicateConfirmedMessageInfo{
			ReplicateConfirmedMessageInfo: &milvuspb.ReplicateConfirmedMessageInfo{
				ConfirmedTimeTick: tt,
			},
		},
	}
}

// grpc.ClientStream method stubs — pchClient never calls these other than
// CloseSend, but the interface forces us to provide them.

func (f *fakeReplicateStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeReplicateStream) Trailer() metadata.MD         { return nil }
func (f *fakeReplicateStream) CloseSend() error {
	f.closeRecv(nil)
	return nil
}
func (f *fakeReplicateStream) Context() context.Context { return context.Background() }
func (f *fakeReplicateStream) SendMsg(m any) error      { return nil }
func (f *fakeReplicateStream) RecvMsg(m any) error      { return nil }

var _ milvuspb.MilvusService_CreateReplicateStreamClient = (*fakeReplicateStream)(nil)

// fakeGrpc returns a sequence of pre-built fake streams; each call to
// CreateReplicateStream consumes the next one. Tests push streams to model
// reconnects.
type fakeGrpc struct {
	mu      sync.Mutex
	streams []*fakeReplicateStream
	openErr error
}

func newFakeGrpc(streams ...*fakeReplicateStream) *fakeGrpc {
	return &fakeGrpc{streams: streams}
}

func (g *fakeGrpc) CreateReplicateStream(ctx context.Context, sourceClusterID string) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.openErr != nil {
		return nil, g.openErr
	}
	if len(g.streams) == 0 {
		return nil, errors.New("fakeGrpc: no more streams")
	}
	s := g.streams[0]
	g.streams = g.streams[1:]

	// A real gRPC bidi stream is bound to the context: when ctx fires, the
	// transport tears down and Recv/Send return errors. Mirror that here so
	// runOneConnection's connCancel reliably unblocks the fake stream.
	go func() {
		<-ctx.Done()
		s.closeRecv(ctx.Err())
	}()

	return s, nil
}

func newPchClientForTest(grpc Grpc) *pchClient {
	return newPchClient("src-cluster", "task-1", "rootcoord-dml_0", grpc)
}

// waitFor polls cond until it returns true or the deadline elapses.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestPchClient_SendThenConfirmDrainsBuffer(t *testing.T) {
	stream := newFakeReplicateStream()
	g := newFakeGrpc(stream)

	p := newPchClientForTest(&pchTestGrpc{fake: g})
	defer p.close()

	for i := uint64(1); i <= 3; i++ {
		assert.NoError(t, p.enqueue(context.Background(), newImmutableForTest(i)))
	}

	// All three requests should land on the fake stream.
	for i := 0; i < 3; i++ {
		select {
		case req := <-stream.sentCh:
			assert.NotNil(t, req.GetReplicateMessage())
		case <-time.After(time.Second):
			t.Fatal("expected request on fake stream")
		}
	}

	// Confirm everything.
	stream.pushConfirm(3)

	waitFor(t, time.Second, "queue drained after confirm", func() bool {
		return p.queue.Len() == 0
	})

	// WaitConfirm should now return immediately.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, p.waitConfirm(ctx))
}

func TestPchClient_PartialConfirmDropsPrefixOnly(t *testing.T) {
	stream := newFakeReplicateStream()
	g := newFakeGrpc(stream)

	p := newPchClientForTest(&pchTestGrpc{fake: g})
	defer p.close()

	for i := uint64(1); i <= 5; i++ {
		assert.NoError(t, p.enqueue(context.Background(), newImmutableForTest(i)))
	}
	for i := 0; i < 5; i++ {
		<-stream.sentCh
	}

	stream.pushConfirm(3)
	waitFor(t, time.Second, "prefix dropped", func() bool { return p.queue.Len() == 2 })
}

func TestPchClient_ReconnectReplaysUnconfirmed(t *testing.T) {
	stream1 := newFakeReplicateStream()
	stream2 := newFakeReplicateStream()
	g := newFakeGrpc(stream1, stream2)

	p := newPchClientForTest(&pchTestGrpc{fake: g})
	defer p.close()

	// Enqueue 5 messages on stream1.
	for i := uint64(1); i <= 5; i++ {
		assert.NoError(t, p.enqueue(context.Background(), newImmutableForTest(i)))
	}
	// Drain stream1 sent channel.
	for i := 0; i < 5; i++ {
		<-stream1.sentCh
	}

	// Confirm tt<=2, then kill stream1 to force a reconnect.
	stream1.pushConfirm(2)
	waitFor(t, time.Second, "prefix dropped", func() bool { return p.queue.Len() == 3 })
	stream1.closeRecv(errors.New("connection reset"))

	// pchClient should reconnect onto stream2 and replay msgs 3, 4, 5 in
	// time-tick order.
	for i := uint64(3); i <= 5; i++ {
		select {
		case req := <-stream2.sentCh:
			tt, err := GetTT(req.GetReplicateMessage().GetMessage())
			assert.NoError(t, err)
			assert.Equal(t, i, tt, "replay must follow time-tick order")
		case <-time.After(2 * time.Second):
			t.Fatalf("expected replay of msg %d on stream2", i)
		}
	}
}

func TestPchClient_NewSendsAfterReconnectFlowToNewStream(t *testing.T) {
	stream1 := newFakeReplicateStream()
	stream2 := newFakeReplicateStream()
	g := newFakeGrpc(stream1, stream2)

	p := newPchClientForTest(&pchTestGrpc{fake: g})
	defer p.close()

	assert.NoError(t, p.enqueue(context.Background(), newImmutableForTest(1)))
	<-stream1.sentCh
	stream1.pushConfirm(1)
	waitFor(t, time.Second, "msg 1 confirmed", func() bool { return p.queue.Len() == 0 })

	stream1.closeRecv(errors.New("reset"))

	// After reconnect, new sends should land on stream2.
	assert.NoError(t, p.enqueue(context.Background(), newImmutableForTest(2)))
	select {
	case req := <-stream2.sentCh:
		tt, _ := GetTT(req.GetReplicateMessage().GetMessage())
		assert.Equal(t, uint64(2), tt)
	case <-time.After(2 * time.Second):
		t.Fatal("expected new send on stream2")
	}
}

func TestPchClient_WaitConfirmRespectsContextCancel(t *testing.T) {
	stream := newFakeReplicateStream()
	g := newFakeGrpc(stream)

	p := newPchClientForTest(&pchTestGrpc{fake: g})
	defer p.close()

	assert.NoError(t, p.enqueue(context.Background(), newImmutableForTest(1)))
	<-stream.sentCh

	// Buffer holds 1 unconfirmed message; cancel ctx should unblock waitConfirm.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.waitConfirm(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("waitConfirm did not return on ctx cancel")
	}
}

// pchTestGrpc adapts a fakeGrpc into the Grpc interface. Only
// CreateReplicateStream is exercised; everything else panics if called.
type pchTestGrpc struct {
	Grpc
	fake *fakeGrpc
}

func (g *pchTestGrpc) CreateReplicateStream(ctx context.Context, sourceClusterID string) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
	return g.fake.CreateReplicateStream(ctx, sourceClusterID)
}

// silence unused linter on the helper alias
var _ grpc.ClientStream = (*fakeReplicateStream)(nil)
