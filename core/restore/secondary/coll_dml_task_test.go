package secondary

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// recordingStream records all messages sent in order. Send runs the caller's
// build callback under s.mu and stamps every returned message with the same
// wire time-tick, mirroring the real StreamClient.
type recordingStream struct {
	mu    sync.Mutex
	ttCtr uint64 // guarded by mu
	msgs  []*commonpb.ImmutableMessage
}

func (s *recordingStream) Send(_ context.Context, build func(ts uint64) []message.MutableMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ttCtr++
	bodyTS := s.ttCtr
	msgs := build(bodyTS)
	for i, msg := range msgs {
		wireTS := bodyTS
		if i > 0 {
			s.ttCtr++
			wireTS = s.ttCtr
		}
		imm := msg.WithTimeTick(wireTS).
			WithLastConfirmed(milvus.NewFakeMessageID(wireTS)).
			IntoImmutableMessage(milvus.NewFakeMessageID(wireTS)).
			IntoImmutableMessageProto()
		s.msgs = append(s.msgs, imm)
	}
	return nil
}

func (s *recordingStream) Forward(_ context.Context, msgs ...*commonpb.ImmutableMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msgs...)
	return nil
}

func (s *recordingStream) WaitConfirm() {}
func (s *recordingStream) Close()       {}

func (s *recordingStream) messagesByType(messageType string) []*commonpb.ImmutableMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*commonpb.ImmutableMessage
	for _, msg := range s.msgs {
		if msg.GetProperties()["_t"] == messageType {
			result = append(result, msg)
		}
	}
	return result
}

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

type sequencedRestful struct {
	mu     sync.Mutex
	states []milvus.ImportState
	calls  int
}

func (r *sequencedRestful) BulkInsert(context.Context, milvus.BulkInsertV2Input) (string, error) {
	return "", nil
}

func (r *sequencedRestful) GetBulkInsertState(context.Context, string, string) (*milvus.GetProcessResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stateIndex := min(r.calls, len(r.states)-1)
	r.calls++
	resp := &milvus.GetProcessResp{}
	resp.Data.State = string(r.states[stateIndex])
	return resp, nil
}

func (r *sequencedRestful) GetSegmentInfo(context.Context, string, int64, int64) (*milvus.SegmentInfo, error) {
	return nil, nil
}

type perJobSequencedRestful struct {
	mu    sync.Mutex
	calls map[string]int
}

func (r *perJobSequencedRestful) BulkInsert(context.Context, milvus.BulkInsertV2Input) (string, error) {
	return "", nil
}

func (r *perJobSequencedRestful) GetBulkInsertState(_ context.Context, _, jobID string) (*milvus.GetProcessResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.calls == nil {
		r.calls = make(map[string]int)
	}
	state := milvus.ImportStateUncommitted
	if r.calls[jobID] > 0 {
		state = milvus.ImportStateCompleted
	}
	r.calls[jobID]++

	resp := &milvus.GetProcessResp{}
	resp.Data.State = string(state)
	return resp, nil
}

func (r *perJobSequencedRestful) GetSegmentInfo(context.Context, string, int64, int64) (*milvus.SegmentInfo, error) {
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

func TestPartitionDirToPaths(t *testing.T) {
	tests := []struct {
		name string
		dir  partitionDir
		want []string
	}{
		{
			name: "insert and delta",
			dir:  partitionDir{insertLogDir: "insert", deltaLogDir: "delta"},
			want: []string{"insert", "delta"},
		},
		{
			name: "insert only",
			dir:  partitionDir{insertLogDir: "insert"},
			want: []string{"insert"},
		},
		{
			name: "delta only",
			dir:  partitionDir{deltaLogDir: "delta"},
			want: []string{"delta"},
		},
		{
			name: "empty",
			dir:  partitionDir{},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.dir.toPaths())
		})
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
// StreamClient's internal dispatch lock keeps per-pch timestamps strictly
// increasing without any external synchronization.
func TestConcurrentCollections_TimestampOrderingPerPch(t *testing.T) {
	// Shared resources — simulating what Task provides to all collection tasks.
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

func TestCheckBulkInsertJob_Import2PCStates(t *testing.T) {
	vchannels := newTestVChannels()
	collBackup := newTestCollBackup(vchannels, nil, nil)

	t.Run("LegacyCompletedSendsNoCommit", func(t *testing.T) {
		stream := &recordingStream{}
		task := newTestDMLTask(t, collBackup, stream)
		task.restfulCli = &sequencedRestful{states: []milvus.ImportState{milvus.ImportStateCompleted}}

		err := task.checkBulkInsertJob(context.Background(), "101")
		assert.NoError(t, err)
		assert.Empty(t, stream.messagesByType(strconv.Itoa(_commitImportMessageType)))
	})

	t.Run("UncommittedSendsOneCommitAndWaits", func(t *testing.T) {
		stream := &recordingStream{}
		restful := &sequencedRestful{states: []milvus.ImportState{
			milvus.ImportStateUncommitted,
			milvus.ImportStateCompleted,
		}}
		task := newTestDMLTask(t, collBackup, stream)
		task.restfulCli = restful

		err := task.checkBulkInsertJob(context.Background(), "102")
		assert.NoError(t, err)
		assert.Equal(t, 2, restful.calls)
		assert.Len(t, stream.messagesByType(strconv.Itoa(_commitImportMessageType)), len(vchannels))
	})

	t.Run("CommittingOnlyWaits", func(t *testing.T) {
		stream := &recordingStream{}
		restful := &sequencedRestful{states: []milvus.ImportState{
			milvus.ImportStateCommitting,
			milvus.ImportStateCompleted,
		}}
		task := newTestDMLTask(t, collBackup, stream)
		task.restfulCli = restful

		err := task.checkBulkInsertJob(context.Background(), "103")
		assert.NoError(t, err)
		assert.Equal(t, 2, restful.calls)
		assert.Empty(t, stream.messagesByType(strconv.Itoa(_commitImportMessageType)))
	})
}

func TestSendImportMsg_DisablesAutoCommit(t *testing.T) {
	vchannels := newTestVChannels()
	collBackup := newTestCollBackup(vchannels, nil, nil)
	stream := &recordingStream{}
	task := newTestDMLTask(t, collBackup, stream)

	_, err := task.sendImportMsg(context.Background(), 1, batch{
		timestamp:      100,
		storageVersion: 2,
		partitionDirs:  []partitionDir{{insertLogDir: "/insert"}},
	})
	assert.NoError(t, err)

	importMessages := stream.messagesByType(strconv.Itoa(int(message.MessageTypeImport)))
	assert.NotEmpty(t, importMessages)
	importBody := &msgpb.ImportMsg{}
	assert.NoError(t, proto.Unmarshal(importMessages[0].GetPayload(), importBody))
	assert.Equal(t, "false", importBody.GetOptions()["auto_commit"])
}

func TestSendCommitImportMsg_WireCompatibilityAndFanout(t *testing.T) {
	vchannels := newTestVChannels()
	collBackup := newTestCollBackup(vchannels, nil, nil)
	stream := &recordingStream{}
	task := newTestDMLTask(t, collBackup, stream)
	jobID := int64(456)

	err := task.sendCommitImportMsg(context.Background(), jobID)
	assert.NoError(t, err)

	commitMessages := stream.messagesByType(strconv.Itoa(_commitImportMessageType))
	assert.Len(t, commitMessages, len(vchannels))
	for i, msg := range commitMessages {
		properties := msg.GetProperties()
		assert.Equal(t, strconv.Itoa(_commitImportMessageType), properties["_t"])
		assert.Equal(t, strconv.Itoa(_commitImportMessageVersion), properties["_v"])
		assert.NotEmpty(t, properties["_h"])
		assert.NotEmpty(t, properties["_bh"])
		assert.Empty(t, msg.GetPayload())
		assert.Equal(t, vchannels[i], milvus.GetVch(msg))

		header, decodeErr := base64.StdEncoding.DecodeString(properties["_h"])
		assert.NoError(t, decodeErr)
		fieldNumber, wireType, consumed := protowire.ConsumeTag(header)
		assert.Equal(t, protowire.Number(1), fieldNumber)
		assert.Equal(t, protowire.VarintType, wireType)
		collectionID, valueBytes := protowire.ConsumeVarint(header[consumed:])
		assert.Greater(t, valueBytes, 0)
		header = header[consumed+valueBytes:]
		fieldNumber, wireType, consumed = protowire.ConsumeTag(header)
		assert.Equal(t, protowire.Number(2), fieldNumber)
		assert.Equal(t, protowire.VarintType, wireType)
		wireJobID, valueBytes := protowire.ConsumeVarint(header[consumed:])
		assert.Greater(t, valueBytes, 0)
		assert.Empty(t, header[consumed+valueBytes:])
		assert.Equal(t, uint64(collBackup.GetCollectionId()), collectionID)
		assert.Equal(t, uint64(jobID), wireJobID)

		broadcastHeader := &messagespb.BroadcastHeader{}
		assert.NoError(t, message.DecodeProto(properties["_bh"], broadcastHeader))
		assert.NotZero(t, broadcastHeader.GetBroadcastId())
		assert.Equal(t, vchannels, broadcastHeader.GetVchannels())
	}
}

func TestExecute_Import2PCPreservesPhaseAndTimestampOrdering(t *testing.T) {
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
	allPartitionL0 := []*backuppb.SegmentBackupInfo{
		{PartitionId: 1, VChannel: vchannels[0], GroupId: 3, Size: 100, IsL0: true, StorageVersion: 2},
	}
	collBackup := newTestCollBackup(vchannels, partitions, allPartitionL0)
	stream := &recordingStream{}
	task := newTestDMLTask(t, collBackup, stream)
	task.restfulCli = &perJobSequencedRestful{}

	err := task.Execute(context.Background())
	assert.NoError(t, err)

	var messageTypes []string
	for _, msg := range stream.msgs {
		messageTypes = append(messageTypes, msg.GetProperties()["_t"])
	}
	importType := strconv.Itoa(int(message.MessageTypeImport))
	commitType := strconv.Itoa(_commitImportMessageType)
	assert.Equal(t, []string{
		importType, importType, commitType, commitType,
		importType, importType, commitType, commitType,
		importType, importType, commitType, commitType,
	}, messageTypes)

	for pch, timestamps := range stream.timestampsByPch() {
		for i := 1; i < len(timestamps); i++ {
			assert.Greater(t, timestamps[i], timestamps[i-1],
				"timestamps on pch %s should be strictly increasing, got %v", pch, timestamps)
		}
	}
}
