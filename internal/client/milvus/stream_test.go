package milvus

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-backup/internal/log"
)

// fakeReplicateStream satisfies milvuspb.MilvusService_CreateReplicateStreamClient.
// Recv returns responses pushed via pushConfirm, terminates with an error from
// failRecv, and also observes the per-stream ctx (mirroring real gRPC where a
// canceled client context unblocks Recv).
type fakeReplicateStream struct {
	grpc.ClientStream // zero value — any method except the ones below will nil-panic

	ctx      context.Context
	mu       sync.Mutex
	sent     []*milvuspb.ReplicateRequest
	recvCh   chan *milvuspb.ReplicateResponse
	recvErr  chan error
	closedCh chan struct{}
}

func newFakeReplicateStream() *fakeReplicateStream {
	return &fakeReplicateStream{
		recvCh:   make(chan *milvuspb.ReplicateResponse, 64),
		recvErr:  make(chan error, 1),
		closedCh: make(chan struct{}),
	}
}

func (f *fakeReplicateStream) Send(req *milvuspb.ReplicateRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.closedCh:
		return io.EOF
	case <-f.ctx.Done():
		return f.ctx.Err()
	default:
	}
	f.sent = append(f.sent, req)
	return nil
}

func (f *fakeReplicateStream) Recv() (*milvuspb.ReplicateResponse, error) {
	select {
	case resp := <-f.recvCh:
		return resp, nil
	case err := <-f.recvErr:
		return nil, err
	case <-f.closedCh:
		return nil, io.EOF
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeReplicateStream) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.closedCh:
	default:
		close(f.closedCh)
	}
	return nil
}

func (f *fakeReplicateStream) sentMsgs() []*commonpb.ImmutableMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*commonpb.ImmutableMessage, len(f.sent))
	for i, r := range f.sent {
		out[i] = r.GetReplicateMessage().GetMessage()
	}
	return out
}

func (f *fakeReplicateStream) pushConfirm(tt uint64) {
	f.recvCh <- &milvuspb.ReplicateResponse{
		Response: &milvuspb.ReplicateResponse_ReplicateConfirmedMessageInfo{
			ReplicateConfirmedMessageInfo: &milvuspb.ReplicateConfirmedMessageInfo{
				ConfirmedTimeTick: tt,
			},
		},
	}
}

func (f *fakeReplicateStream) failRecv(err error) {
	select {
	case f.recvErr <- err:
	default:
	}
}

// fakeGrpc hands out the next stream from a pre-built sequence on each call —
// letting reconnect tests provide the replacement stream ahead of time.
type fakeGrpc struct {
	Grpc // interface — the embedded nil value satisfies Grpc; any unused method nil-panics

	streams []*fakeReplicateStream
	idx     atomic.Int64
}

func newFakeGrpc(streams ...*fakeReplicateStream) *fakeGrpc {
	return &fakeGrpc{streams: streams}
}

func (g *fakeGrpc) CreateReplicateStream(ctx context.Context, _ string) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
	i := g.idx.Add(1) - 1
	if i >= int64(len(g.streams)) {
		return nil, errors.New("no more streams")
	}
	s := g.streams[i]
	s.ctx = ctx
	return s, nil
}

// eofGrpc returns a new already-closed stream on every open. Recv and Send
// both yield io.EOF, which is the accept-then-immediate-close the reconnect
// loop has to back off.
type eofGrpc struct {
	Grpc

	opens atomic.Int64
}

func (g *eofGrpc) CreateReplicateStream(ctx context.Context, _ string) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
	s := newFakeReplicateStream()
	s.ctx = ctx
	s.CloseSend()
	g.opens.Add(1)
	return s, nil
}

// useObservedLogger installs an observer core as the global logger and puts
// the previous one back when the test ends. NewStreamClient copies the global
// logger, so this has to run first. Not parallel-safe: the global is process-wide.
func useObservedLogger(t *testing.T) *observer.ObservedLogs {
	t.Helper()

	prev := log.L()
	prevLevel := log.GetLevel()
	core, logs := observer.New(zapcore.DebugLevel)
	log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(prevLevel)})
	t.Cleanup(func() {
		log.ReplaceGlobals(prev, &log.ZapProperties{Level: zap.NewAtomicLevelAt(prevLevel)})
	})
	return logs
}

// buildImportMsg constructs a single-vchannel Import immutable suitable for Forward tests.
func buildImportMsg(t *testing.T, ts uint64, vchannel string) *commonpb.ImmutableMessage {
	t.Helper()

	body := &message.ImportMsg{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Import, Timestamp: ts},
		DbName:         "default",
		CollectionName: "t",
		CollectionID:   1,
	}
	builder := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(body).
		WithBroadcast([]string{vchannel})
	broadcast := builder.MustBuildBroadcast().WithBroadcastID(1)
	msgs := broadcast.SplitIntoMutableMessage()
	return msgs[0].
		WithTimeTick(ts).
		WithLastConfirmed(NewFakeMessageID(ts)).
		IntoImmutableMessage(NewFakeMessageID(ts)).
		IntoImmutableMessageProto()
}

// buildBroadcastDDL constructs a CreateCollection broadcast that targets both a
// data vchannel and a control vchannel — used by the broadcast-collision test.
func buildBroadcastDDL(dataVch, ctrlVch string) []message.MutableMessage {
	req := &message.CreateCollectionRequest{
		Base:                &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
		DbName:              "default",
		CollectionName:      "t",
		VirtualChannelNames: []string{dataVch},
	}
	builder := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(req).
		WithBroadcast([]string{dataVch, ctrlVch})
	broadcast := builder.MustBuildBroadcast().WithBroadcastID(1)
	return broadcast.SplitIntoMutableMessage()
}

// Realistic channel naming — funcutil.ToPhysicalChannel strips the last "_*v*"
// suffix, so "pch0_1v0" → "pch0", "pch0_vcchan" → "pch0".
const (
	testPch      = "pch0"
	testDataVch  = "pch0_1v0"
	testCtrlVch  = "pch0_vcchan"
	testPch2     = "pch1"
	testDataVch2 = "pch1_1v0"
)

func TestStreamClient_SendAndConfirm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeReplicateStream()
		cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(fs))
		defer cli.Close()

		synctest.Wait() // runOneConnection establishes

		err := cli.Send(context.Background(), func(ts uint64) []message.MutableMessage {
			body := &message.ImportMsg{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Import, Timestamp: ts},
				DbName:         "default",
				CollectionName: "t",
				CollectionID:   1,
			}
			builder := message.NewImportMessageBuilderV1().
				WithHeader(&message.ImportMessageHeader{}).
				WithBody(body).
				WithBroadcast([]string{testDataVch})
			return builder.MustBuildBroadcast().WithBroadcastID(1).SplitIntoMutableMessage()
		})
		assert.NoError(t, err)

		synctest.Wait()

		sent := fs.sentMsgs()
		assert.Len(t, sent, 1)
		tt, err := GetTT(sent[0])
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), tt)

		fs.pushConfirm(tt)

		done := make(chan struct{})
		go func() { cli.WaitConfirm(); close(done) }()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("WaitConfirm did not return after confirm")
		}
	})
}

func TestStreamClient_ForwardPreservesSourceTT(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeReplicateStream()
		cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(fs))
		defer cli.Close()

		synctest.Wait()

		imm := buildImportMsg(t, 99, testDataVch)
		assert.NoError(t, cli.Forward(context.Background(), imm))

		synctest.Wait()

		sent := fs.sentMsgs()
		assert.Len(t, sent, 1)
		tt, _ := GetTT(sent[0])
		assert.Equal(t, uint64(99), tt, "forward must preserve the source tt")
	})
}

// TestStreamClient_BroadcastCollidingPchGetsDistinctTT is the regression test
// for the broadcast-collision bug: when one broadcast routes multiple messages
// to the same pchannel (data vchannel + control vchannel), each message must
// receive a fresh wire tt or the broker's (pch, tt) dedup silently drops the
// second one.
func TestStreamClient_BroadcastCollidingPchGetsDistinctTT(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeReplicateStream()
		cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(fs))
		defer cli.Close()

		synctest.Wait()

		err := cli.Send(context.Background(), func(uint64) []message.MutableMessage {
			return buildBroadcastDDL(testDataVch, testCtrlVch)
		})
		assert.NoError(t, err)

		synctest.Wait()

		sent := fs.sentMsgs()
		assert.Len(t, sent, 2)

		tt0, err := GetTT(sent[0])
		assert.NoError(t, err)
		tt1, err := GetTT(sent[1])
		assert.NoError(t, err)
		assert.NotEqual(t, tt0, tt1, "broadcast msgs on same pch must carry distinct wire tts")
		assert.Less(t, tt0, tt1, "wire tts must be monotonic on the pch")
	})
}

// TestStreamClient_ReplayOnReconnect verifies that messages sent on a stream
// that breaks before confirm are re-shipped on the replacement stream.
func TestStreamClient_ReplayOnReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := newFakeReplicateStream()
		second := newFakeReplicateStream()
		cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(first, second))
		defer cli.Close()

		synctest.Wait()

		assert.NoError(t, cli.Forward(context.Background(),
			buildImportMsg(t, 10, testDataVch),
			buildImportMsg(t, 11, testDataVch)))

		synctest.Wait()
		assert.Len(t, first.sentMsgs(), 2, "both msgs ship on the first stream")

		// Break the first stream before any confirm and without advancing the
		// bubble clock. The attempt is not established, so runForever parks on
		// the initial backoff. Wait returns here without moving time.
		first.failRecv(errors.New("boom"))
		synctest.Wait()
		assert.Empty(t, second.sentMsgs(), "replacement stays parked on the initial backoff")

		time.Sleep(pchReconnectInitialBackoff)
		synctest.Wait()

		secondSent := second.sentMsgs()
		assert.Len(t, secondSent, 2, "both msgs replayed on the replacement stream")
		tt0, _ := GetTT(secondSent[0])
		tt1, _ := GetTT(secondSent[1])
		assert.Equal(t, uint64(10), tt0)
		assert.Equal(t, uint64(11), tt1)

		second.pushConfirm(11)
		done := make(chan struct{})
		go func() { cli.WaitConfirm(); close(done) }()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("WaitConfirm did not return after confirm on replacement stream")
		}
	})
}

// TestStreamClient_ConfirmDropsPrefix exercises sendLoop + recvLoop +
// queue.Confirm as a whole: after confirm for tt=N, messages with tt ≤ N
// leave the queue and WaitConfirm stays blocked until the remainder is
// confirmed.
func TestStreamClient_ConfirmDropsPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeReplicateStream()
		cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(fs))
		defer cli.Close()

		synctest.Wait()

		assert.NoError(t, cli.Forward(context.Background(),
			buildImportMsg(t, 1, testDataVch),
			buildImportMsg(t, 2, testDataVch),
			buildImportMsg(t, 3, testDataVch)))

		synctest.Wait()
		assert.Len(t, fs.sentMsgs(), 3)

		// Confirm covers only the first two.
		fs.pushConfirm(2)
		synctest.Wait()

		done := make(chan struct{})
		go func() { cli.WaitConfirm(); close(done) }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("WaitConfirm returned while tt=3 is unconfirmed")
		default:
		}

		fs.pushConfirm(3)
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("WaitConfirm did not return after final confirm")
		}
	})
}

// TestStreamClient_RoutesByPch verifies that Forward dispatches each message
// to the pchannel derived from its vchannel. With one stream per pchannel,
// each stream should only see the message addressed to it.
func TestStreamClient_RoutesByPch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsA := newFakeReplicateStream()
		fsB := newFakeReplicateStream()
		// Order matters: newPchClient registers pchs in map iteration order,
		// but each pchClient starts its own goroutine which calls
		// CreateReplicateStream independently. The fakeGrpc hands streams out
		// in call order — we verify routing by total-count per stream, which
		// holds regardless of which pch got which stream.
		cli := NewStreamClient("src", "task", []string{testPch, testPch2}, newFakeGrpc(fsA, fsB))
		defer cli.Close()

		synctest.Wait()

		assert.NoError(t, cli.Forward(context.Background(),
			buildImportMsg(t, 1, testDataVch),
			buildImportMsg(t, 2, testDataVch2)))

		synctest.Wait()

		total := len(fsA.sentMsgs()) + len(fsB.sentMsgs())
		assert.Equal(t, 2, total)
		assert.Equal(t, 1, len(fsA.sentMsgs()), "each pch receives exactly one msg")
		assert.Equal(t, 1, len(fsB.sentMsgs()), "each pch receives exactly one msg")
	})
}

func TestStreamClient_UnknownPchRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeReplicateStream()
		cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(fs))
		defer cli.Close()

		synctest.Wait()

		// Vchannel maps to a pch that has no client.
		err := cli.Forward(context.Background(), buildImportMsg(t, 1, "other-pch_1v0"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no pch client")
	})
}

// TestStreamClient_ImmediateEOFBacksOff is the accept-then-close regression:
// every open returns an already-closed stream, so the reconnect loop must take
// the backoff path. A tight loop is not durably blocked and this sleep would
// hang — that hang is the failure, not a reason to drop the timer.
//
// Opens land at t=0, 100ms, 300ms, 700ms, and 1500ms (backoff 100, 200, 400,
// 800). The next wait is 1600ms and would land at 3100ms, past this window.
func TestStreamClient_ImmediateEOFBacksOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := useObservedLogger(t)
		g := &eofGrpc{}
		cli := NewStreamClient("src", "task", []string{testPch}, g)
		defer cli.Close()

		imm := buildImportMsg(t, 77, testDataVch)
		assert.NoError(t, cli.Forward(context.Background(), imm))
		time.Sleep(2 * time.Second)
		synctest.Wait()

		assert.Equal(t, int64(5), g.opens.Load())
		assert.Empty(t, logs.FilterLevelExact(zap.InfoLevel).FilterMessage("replicate stream connected").All())

		warns := logs.FilterLevelExact(zap.WarnLevel).FilterMessage("replicate stream failing").All()
		require.Len(t, warns, 1)
		fields := warns[0].ContextMap()
		assert.Equal(t, int64(0), fields["confirmed"])
		consecutive, ok := fields["consecutive_failures"].(int64)
		require.True(t, ok)
		assert.GreaterOrEqual(t, consecutive, int64(2))
		head, ok := fields["head"].(map[string]any)
		require.True(t, ok, "head field missing: %v", fields)
		assert.Equal(t, uint64(77), head["timetick"])
	})
}

// TestStreamClient_ConfirmedDisconnectReconnectsImmediately checks the branch
// that must keep resetting backoff. Confirming the lower timetick marks the
// connection established and leaves the higher one queued; the replacement
// already has that replay with no backoff sleep.
func TestStreamClient_ConfirmedDisconnectReconnectsImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := newFakeReplicateStream()
		second := newFakeReplicateStream()
		cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(first, second))
		defer cli.Close()

		synctest.Wait()

		assert.NoError(t, cli.Forward(context.Background(),
			buildImportMsg(t, 10, testDataVch),
			buildImportMsg(t, 11, testDataVch)))
		synctest.Wait()
		assert.Len(t, first.sentMsgs(), 2)

		first.pushConfirm(10)
		synctest.Wait()

		first.failRecv(errors.New("boom"))
		synctest.Wait()

		sent := second.sentMsgs()
		require.Len(t, sent, 1, "replacement already has the unconfirmed replay")
		tt, err := GetTT(sent[0])
		require.NoError(t, err)
		assert.Equal(t, uint64(11), tt)
	})
}

// TestStreamClient_LifetimeWithoutConfirmCountsAsEstablished covers the other
// half of the established predicate: staying open for pchMinEstablishedLifetime
// resets backoff even when nothing was confirmed. Half of that does not.
func TestStreamClient_LifetimeWithoutConfirmCountsAsEstablished(t *testing.T) {
	t.Run("FullLifetime", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			first := newFakeReplicateStream()
			second := newFakeReplicateStream()
			cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(first, second))
			defer cli.Close()

			synctest.Wait()
			assert.NoError(t, cli.Forward(context.Background(), buildImportMsg(t, 7, testDataVch)))
			synctest.Wait()
			assert.Len(t, first.sentMsgs(), 1)

			time.Sleep(pchMinEstablishedLifetime)
			first.failRecv(errors.New("boom"))
			synctest.Wait()

			sent := second.sentMsgs()
			require.Len(t, sent, 1, "full lifetime reconnects without backoff")
			tt, err := GetTT(sent[0])
			require.NoError(t, err)
			assert.Equal(t, uint64(7), tt)
		})
	})

	t.Run("HalfLifetime", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			first := newFakeReplicateStream()
			second := newFakeReplicateStream()
			cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(first, second))
			defer cli.Close()

			synctest.Wait()
			assert.NoError(t, cli.Forward(context.Background(), buildImportMsg(t, 7, testDataVch)))
			synctest.Wait()
			assert.Len(t, first.sentMsgs(), 1)

			time.Sleep(pchMinEstablishedLifetime / 2)
			first.failRecv(errors.New("boom"))
			synctest.Wait()
			assert.Empty(t, second.sentMsgs(), "half lifetime stays parked on the initial backoff")

			time.Sleep(pchReconnectInitialBackoff)
			synctest.Wait()
			sent := second.sentMsgs()
			require.Len(t, sent, 1)
			tt, err := GetTT(sent[0])
			require.NoError(t, err)
			assert.Equal(t, uint64(7), tt)
		})
	})
}

// TestStreamClient_WaitConfirmNoProgress warns while a live stream never
// acks, then still returns once the broker confirms. Cancel unblocks the same
// wait without counting it as a channel failure.
func TestStreamClient_WaitConfirmNoProgress(t *testing.T) {
	t.Run("WarnsThenCompletes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logs := useObservedLogger(t)
			fs := newFakeReplicateStream()
			cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(fs))
			defer cli.Close()

			synctest.Wait()
			assert.NoError(t, cli.Forward(context.Background(), buildImportMsg(t, 42, testDataVch)))

			done := make(chan struct{})
			go func() { cli.WaitConfirm(); close(done) }()
			synctest.Wait()

			time.Sleep(pchNoProgressWarnAfter - time.Nanosecond)
			synctest.Wait()
			select {
			case <-done:
				t.Fatal("WaitConfirm returned before any confirm")
			default:
			}
			assert.Empty(t, logs.FilterMessage("replicate stream made no progress").All())

			time.Sleep(time.Nanosecond)
			synctest.Wait()
			warns := logs.FilterLevelExact(zap.WarnLevel).FilterMessage("replicate stream made no progress").All()
			require.Len(t, warns, 1)
			fields := warns[0].ContextMap()
			assert.Equal(t, int64(0), fields["confirmed"])
			queued, ok := fields["queued"].(int64)
			require.True(t, ok)
			assert.Greater(t, queued, int64(0))
			head, ok := fields["head"].(map[string]any)
			require.True(t, ok, "head field missing: %v", fields)
			assert.Equal(t, uint64(42), head["timetick"])

			fs.pushConfirm(42)
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Fatal("WaitConfirm did not return after confirm")
			}
		})
	})

	t.Run("CancelAbortsWithoutFailureStreak", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logs := useObservedLogger(t)
			fs := newFakeReplicateStream()
			cli := NewStreamClient("src", "task", []string{testPch}, newFakeGrpc(fs))

			synctest.Wait()
			assert.NoError(t, cli.Forward(context.Background(), buildImportMsg(t, 42, testDataVch)))

			done := make(chan struct{})
			go func() { cli.WaitConfirm(); close(done) }()
			synctest.Wait()

			cli.Close()
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Fatal("WaitConfirm did not return after cancel")
			}

			aborted := logs.FilterLevelExact(zap.WarnLevel).FilterMessage("wait confirm aborted").All()
			assert.NotEmpty(t, aborted)
			assert.Empty(t, logs.FilterMessage("replicate stream failing").All())
		})
	})
}
