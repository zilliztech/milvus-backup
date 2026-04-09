package secondary

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// recordingStream is an in-memory fake of milvus.Stream that records every
// message it accepts. It mimics the production StreamClient by allocating a
// monotonic time-tick under a mutex and converting the mutable message into an
// immutable proto, so the per-pch time-tick ordering invariants exercised by
// the secondary tasks still surface in the recorded output.
type recordingStream struct {
	mu   sync.Mutex
	ts   atomic.Uint64
	msgs []*commonpb.ImmutableMessage
}

func (s *recordingStream) Alloc() uint64 {
	return s.ts.Add(1)
}

func (s *recordingStream) Send(_ context.Context, msg message.MutableMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tt := s.ts.Add(1)
	id := newRecordingMessageID(tt)
	immutable := msg.
		WithTimeTick(tt).
		WithLastConfirmed(id).
		IntoImmutableMessage(id).
		IntoImmutableMessageProto()

	s.msgs = append(s.msgs, immutable)
	return nil
}

func (s *recordingStream) Forward(_ context.Context, msg *commonpb.ImmutableMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *recordingStream) WaitConfirm(_ context.Context) error { return nil }

func (s *recordingStream) Close() {}

// timestampsByPch returns the time-ticks grouped by physical channel, in send
// order.
func (s *recordingStream) timestampsByPch() map[string][]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string][]uint64)
	for _, msg := range s.msgs {
		pch := milvus.GetPch(msg)
		tt, _ := milvus.GetTT(msg)
		result[pch] = append(result[pch], tt)
	}
	return result
}

// recordingMessageID is a synthetic message id used by recordingStream to
// satisfy the MutableMessage -> ImmutableMessage conversion contract.
type recordingMessageID struct{ tt uint64 }

func newRecordingMessageID(tt uint64) *recordingMessageID { return &recordingMessageID{tt: tt} }

func (r *recordingMessageID) WALName() message.WALName { return message.WALNameKafka }
func (r *recordingMessageID) LT(id message.MessageID) bool {
	return r.tt < id.(*recordingMessageID).tt
}

func (r *recordingMessageID) LTE(id message.MessageID) bool {
	return r.tt <= id.(*recordingMessageID).tt
}
func (r *recordingMessageID) EQ(id message.MessageID) bool {
	return r.tt == id.(*recordingMessageID).tt
}
func (r *recordingMessageID) Marshal() string { return fmt.Sprintf("%d", r.tt) }
func (r *recordingMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{Id: message.EncodeUint64(r.tt), WALName: commonpb.WALName_Kafka}
}
func (r *recordingMessageID) String() string { return fmt.Sprintf("%d", r.tt) }

// completedRestful always returns ImportStateCompleted immediately.
type completedRestful struct{}

func (r *completedRestful) BulkInsert(context.Context, milvus.BulkInsertV2Input) (string, error) {
	return "", nil
}

func (r *completedRestful) GetBulkInsertState(context.Context, string, string) (*milvus.GetProcessResp, error) {
	resp := &milvus.GetProcessResp{}
	resp.Data.State = string(milvus.ImportStateCompleted)
	return resp, nil
}

func (r *completedRestful) GetSegmentInfo(context.Context, string, int64, int64) (*milvus.SegmentInfo, error) {
	return nil, nil
}

func newTestVChannels() []string {
	return []string{
		"rootcoord-dml_0_123v0",
		"rootcoord-dml_1_123v0",
	}
}

func newTestPchTS(vchannels []string) map[string]uint64 {
	pchTS := make(map[string]uint64)
	for _, vch := range vchannels {
		pch := funcutil.ToPhysicalChannel(vch)
		pchTS[pch] = 100
	}
	return pchTS
}

func newTestSchema() *backuppb.CollectionSchema {
	return &backuppb.CollectionSchema{
		Name: "test_coll",
		Fields: []*backuppb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     backuppb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: backuppb.DataType_FloatVector,
				TypeParams: []*backuppb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
		},
	}
}

func newTestCollBackup(vchannels []string, partitions []*backuppb.PartitionBackupInfo, l0Segments []*backuppb.SegmentBackupInfo) *backuppb.CollectionBackupInfo {
	return &backuppb.CollectionBackupInfo{
		CollectionId:        123,
		DbName:              "default",
		CollectionName:      "test_coll",
		Schema:              newTestSchema(),
		VirtualChannelNames: vchannels,
		PartitionBackups:    partitions,
		L0Segments:          l0Segments,
	}
}

func newTestDMLTask(t *testing.T, collBackup *backuppb.CollectionBackupInfo, stream *recordingStream) *collDMLTask {
	vchannels := collBackup.GetVirtualChannelNames()

	storageMock := storage.NewMockClient(t)
	storageMock.EXPECT().
		ListPrefix(mock.Anything, mock.Anything, false).
		Return(storage.NewMockObjectIterator(nil), nil).
		Maybe()

	return &collDMLTask{
		pchTS:         newTestPchTS(vchannels),
		collBackup:    collBackup,
		backupStorage: storageMock,
		backupDir:     "/backup",
		streamCli:     stream,
		restfulCli:    &completedRestful{},
		logger:        zap.NewNop(),
	}
}

func TestExecute_TimestampOrderingPerPch(t *testing.T) {
	vchannels := newTestVChannels()

	partitions := []*backuppb.PartitionBackupInfo{
		{
			PartitionId:   1,
			PartitionName: "part1",
			SegmentBackups: []*backuppb.SegmentBackupInfo{
				{PartitionId: 1, VChannel: vchannels[0], GroupId: 1, Size: 100, StorageVersion: 2},
				{PartitionId: 1, VChannel: vchannels[1], GroupId: 2, Size: 100, StorageVersion: 2},
				{PartitionId: 1, VChannel: vchannels[0], GroupId: 3, Size: 100, IsL0: true, StorageVersion: 2},
			},
		},
		{
			PartitionId:   2,
			PartitionName: "part2",
			SegmentBackups: []*backuppb.SegmentBackupInfo{
				{PartitionId: 2, VChannel: vchannels[0], GroupId: 4, Size: 100, StorageVersion: 2},
				{PartitionId: 2, VChannel: vchannels[1], GroupId: 5, Size: 100, StorageVersion: 2},
				{PartitionId: 2, VChannel: vchannels[1], GroupId: 6, Size: 100, IsL0: true, StorageVersion: 2},
			},
		},
	}

	collBackup := newTestCollBackup(vchannels, partitions, nil)
	stream := &recordingStream{}
	task := newTestDMLTask(t, collBackup, stream)

	err := task.Execute(context.Background())
	assert.NoError(t, err)

	// Verify: per physical channel, timestamps must be strictly increasing.
	for pch, tss := range stream.timestampsByPch() {
		for i := 1; i < len(tss); i++ {
			assert.Greater(t, tss[i], tss[i-1],
				"timestamps on pch %s should be strictly increasing, got %v", pch, tss)
		}
	}
}

func TestExecute_NonL0BeforeL0(t *testing.T) {
	vchannels := newTestVChannels()

	partitions := []*backuppb.PartitionBackupInfo{
		{
			PartitionId:   1,
			PartitionName: "part1",
			SegmentBackups: []*backuppb.SegmentBackupInfo{
				{PartitionId: 1, VChannel: vchannels[0], GroupId: 1, Size: 100, StorageVersion: 2},
				{PartitionId: 1, VChannel: vchannels[0], GroupId: 2, Size: 100, IsL0: true, StorageVersion: 2},
			},
		},
	}

	allPartL0 := []*backuppb.SegmentBackupInfo{
		{PartitionId: 1, VChannel: vchannels[0], GroupId: 10, Size: 100, IsL0: true, StorageVersion: 2},
	}

	collBackup := newTestCollBackup(vchannels, partitions, allPartL0)
	stream := &recordingStream{}
	task := newTestDMLTask(t, collBackup, stream)

	err := task.Execute(context.Background())
	assert.NoError(t, err)

	// With phased execution, timestamps per pch must be strictly increasing,
	// which implies non-L0 (sent first) has lower ts than L0 (sent later),
	// and all-partition L0 (sent last) has the highest ts.
	for pch, tss := range stream.timestampsByPch() {
		assert.True(t, len(tss) >= 2,
			"pch %s should have messages from multiple phases", pch)
		for i := 1; i < len(tss); i++ {
			assert.Greater(t, tss[i], tss[i-1],
				"timestamps on pch %s should be strictly increasing, got %v", pch, tss)
		}
	}
}

func TestExecute_EmptyPartitions(t *testing.T) {
	vchannels := newTestVChannels()

	partitions := []*backuppb.PartitionBackupInfo{
		{
			PartitionId:   1,
			PartitionName: "empty_part",
		},
	}

	collBackup := newTestCollBackup(vchannels, partitions, nil)
	stream := &recordingStream{}
	task := newTestDMLTask(t, collBackup, stream)

	err := task.Execute(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, stream.msgs, "no messages should be sent for empty partitions")
}

// TestConcurrentCollections_TimestampOrderingPerPch verifies that when multiple
// collections are restored concurrently and share the same physical channels,
// the stream client's internal serialization keeps per-pch time-ticks
// monotonically increasing.
func TestConcurrentCollections_TimestampOrderingPerPch(t *testing.T) {
	// Shared stream — simulating what Task provides to all collection tasks.
	stream := &recordingStream{}

	storageMock := storage.NewMockClient(t)
	storageMock.EXPECT().
		ListPrefix(mock.Anything, mock.Anything, false).
		Return(storage.NewMockObjectIterator(nil), nil).
		Maybe()

	// Create 4 collections with different vchannels mapping to the same 2 pchs.
	numColls := 4
	tasks := make([]*collDMLTask, numColls)
	for i := range numColls {
		collID := int64(100 + i)
		vchannels := []string{
			fmt.Sprintf("rootcoord-dml_0_%dv0", collID),
			fmt.Sprintf("rootcoord-dml_1_%dv0", collID),
		}

		partitions := []*backuppb.PartitionBackupInfo{
			{
				PartitionId:   collID*10 + 1,
				PartitionName: fmt.Sprintf("part_%d_1", collID),
				SegmentBackups: []*backuppb.SegmentBackupInfo{
					{PartitionId: collID*10 + 1, VChannel: vchannels[0], GroupId: collID*100 + 1, Size: 100, StorageVersion: 2},
					{PartitionId: collID*10 + 1, VChannel: vchannels[1], GroupId: collID*100 + 2, Size: 100, StorageVersion: 2},
					{PartitionId: collID*10 + 1, VChannel: vchannels[0], GroupId: collID*100 + 3, Size: 100, IsL0: true, StorageVersion: 2},
				},
			},
			{
				PartitionId:   collID*10 + 2,
				PartitionName: fmt.Sprintf("part_%d_2", collID),
				SegmentBackups: []*backuppb.SegmentBackupInfo{
					{PartitionId: collID*10 + 2, VChannel: vchannels[0], GroupId: collID*100 + 4, Size: 100, StorageVersion: 2},
					{PartitionId: collID*10 + 2, VChannel: vchannels[1], GroupId: collID*100 + 5, Size: 100, StorageVersion: 2},
				},
			},
		}

		collBackup := &backuppb.CollectionBackupInfo{
			CollectionId:        collID,
			DbName:              "default",
			CollectionName:      fmt.Sprintf("coll_%d", collID),
			Schema:              newTestSchema(),
			VirtualChannelNames: vchannels,
			PartitionBackups:    partitions,
		}

		tasks[i] = &collDMLTask{
			pchTS:         newTestPchTS(vchannels),
			collBackup:    collBackup,
			backupStorage: storageMock,
			backupDir:     "/backup",
			streamCli:     stream,
			restfulCli:    &completedRestful{},
			logger:        zap.NewNop(),
		}
	}

	// Run all collection DML tasks concurrently.
	g, ctx := errgroup.WithContext(context.Background())
	for _, task := range tasks {
		g.Go(func() error {
			return task.Execute(ctx)
		})
	}
	assert.NoError(t, g.Wait())

	// Verify: per physical channel, timestamps must be strictly increasing.
	for pch, tss := range stream.timestampsByPch() {
		for i := 1; i < len(tss); i++ {
			assert.Greater(t, tss[i], tss[i-1],
				"timestamps on pch %s should be strictly increasing, got %v", pch, tss)
		}
	}
}

func TestSendBatches(t *testing.T) {
	vchannels := newTestVChannels()
	collBackup := newTestCollBackup(vchannels, nil, nil)
	stream := &recordingStream{}
	task := newTestDMLTask(t, collBackup, stream)

	batches := []batch{
		{timestamp: 100, partitionDirs: []partitionDir{{insertLogDir: "/a"}}, storageVersion: 2},
		{timestamp: 200, partitionDirs: []partitionDir{{insertLogDir: "/b"}}, storageVersion: 2},
	}

	jobIDs, err := task.sendBatches(context.Background(), 1, batches)
	assert.NoError(t, err)
	assert.Len(t, jobIDs, 2)

	// Each batch broadcasts to all vchannels, so total messages = 2 batches * 2 vchannels.
	assert.Len(t, stream.msgs, 4)
}
