package secondary

import (
	"context"
	"sync"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// recordingStream records all messages sent in order.
type recordingStream struct {
	mu   sync.Mutex
	msgs []*commonpb.ImmutableMessage
}

func (s *recordingStream) Send(_ context.Context, msg *commonpb.ImmutableMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *recordingStream) WaitConfirm() {}

// timestampsByPch returns the timestamps grouped by physical channel, in send order.
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
		tsAlloc:       newTTAlloc(),
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
