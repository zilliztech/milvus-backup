package secondary

import (
	"context"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
)

func newTestSnapDMLTask(t *testing.T, collBackup *backuppb.CollectionBackupInfo, stream milvus.Stream) *collSnapDMLTask {
	t.Helper()

	args := snapDMLTaskArgs{
		TaskID:    "task-1",
		PchTS:     newTestPchTS(collBackup.GetVirtualChannelNames()),
		StreamCli: stream, RestfulCli: &completedRestful{},
	}
	task, err := newCollSnapDMLTask(args, collBackup,
		"s3://backup-bucket/backup/mybackup/bundle/snapshots/123/metadata/456.json",
		`{"extfs":{"access_key_id":"ak"}}`,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained)
	require.NoError(t, err)
	task.logger = zap.NewNop()
	return task
}

func importBodies(t *testing.T, stream *recordingStream) []*msgpb.ImportMsg {
	t.Helper()

	msgs := stream.messagesByType(strconv.Itoa(int(message.MessageTypeImport)))
	bodies := make([]*msgpb.ImportMsg, 0, len(msgs))
	for _, msg := range msgs {
		body := &msgpb.ImportMsg{}
		require.NoError(t, proto.Unmarshal(msg.GetPayload(), body))
		bodies = append(bodies, body)
	}
	return bodies
}

func TestSnapshotPartitionRouting(t *testing.T) {
	// The server pairs the message's partition ids with the mapping's
	// destinations in sorted-name order, so the ids follow the names however
	// the backup meta lists them.
	t.Run("IdentityMappingInSortedNameOrder", func(t *testing.T) {
		collBackup := newTestCollBackup(newTestVChannels(), []*backuppb.PartitionBackupInfo{
			{PartitionId: 30, PartitionName: "p_c"},
			{PartitionId: 10, PartitionName: "_default"},
			{PartitionId: 20, PartitionName: "p_b"},
		}, nil)

		mapping, ids, err := snapshotPartitionRouting(collBackup)
		require.NoError(t, err)
		assert.JSONEq(t, `{"_default":"_default","p_b":"p_b","p_c":"p_c"}`, mapping)
		assert.Equal(t, []int64{10, 20, 30}, ids)
	})

	// A partition-key collection takes no mapping, and the message names every
	// partition in the order the proxy arranges them: by the index in the name.
	t.Run("PartitionKeyArrangesByIndex", func(t *testing.T) {
		collBackup := newTestCollBackup(newTestVChannels(), []*backuppb.PartitionBackupInfo{
			{PartitionId: 12, PartitionName: "_default_2"},
			{PartitionId: 10, PartitionName: "_default_0"},
			{PartitionId: 11, PartitionName: "_default_1"},
		}, nil)
		collBackup.Schema.Fields = append(collBackup.Schema.Fields, &backuppb.FieldSchema{
			FieldID: 102, Name: "tenant", DataType: backuppb.DataType_Int64, IsPartitionKey: true,
		})

		mapping, ids, err := snapshotPartitionRouting(collBackup)
		require.NoError(t, err)
		assert.Empty(t, mapping)
		assert.Equal(t, []int64{10, 11, 12}, ids)
	})

	t.Run("NoPartitions", func(t *testing.T) {
		mapping, ids, err := snapshotPartitionRouting(newTestCollBackup(newTestVChannels(), nil, nil))
		require.NoError(t, err)
		assert.Empty(t, mapping)
		assert.Nil(t, ids)
	})
}

func TestSnapshotLayoutOption(t *testing.T) {
	assert.Equal(t, "self-contained", snapshotLayoutOption(datapb.SnapshotLayout_SnapshotLayoutSelfContained))
	assert.Equal(t, "referenced", snapshotLayoutOption(datapb.SnapshotLayout_SnapshotLayoutReferenced))
	// A bundle written before layouts were recorded is the referenced layout.
	assert.Equal(t, "referenced", snapshotLayoutOption(datapb.SnapshotLayout_SnapshotLayoutUnknown))
}

// The import message has to be the one a coordinator would have broadcast for
// a snapshot-sourced import: the bundle named in the reference options, one
// empty file standing for it, and none of the binlog contract's knobs.
func TestCollSnapDMLTask_SendImportMsg(t *testing.T) {
	vchannels := newTestVChannels()
	collBackup := newTestCollBackup(vchannels, []*backuppb.PartitionBackupInfo{
		{PartitionId: 20, PartitionName: "p_b"},
		{PartitionId: 10, PartitionName: "_default"},
	}, nil)
	stream := &recordingStream{}
	task := newTestSnapDMLTask(t, collBackup, stream)

	jobID, err := task.sendImportMsg(context.Background())
	require.NoError(t, err)
	assert.NotZero(t, jobID)

	bodies := importBodies(t, stream)
	// One broadcast, split onto every vchannel of the collection.
	require.Len(t, bodies, len(vchannels))
	for _, body := range bodies {
		assert.Equal(t, jobID, body.GetJobID())
		assert.Equal(t, collBackup.GetCollectionId(), body.GetCollectionID())
		assert.Equal(t, "default", body.GetDbName())
		assert.Equal(t, "test_coll", body.GetCollectionName())
		assert.Equal(t, []int64{10, 20}, body.GetPartitionIDs())

		require.Len(t, body.GetFiles(), 1)
		assert.Empty(t, body.GetFiles()[0].GetPaths())

		opts := body.GetOptions()
		assert.Equal(t, "snapshot", opts["source_type"])
		assert.Equal(t, "true", opts["backup"])
		assert.Equal(t, "false", opts["auto_commit"])
		assert.Equal(t, "true", opts["skip_disk_quota_check"])
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle/snapshots/123/metadata/456.json", opts["_snapshot_source_uri"])
		assert.Equal(t, "self-contained", opts["_snapshot_layout"])
		assert.Equal(t, `{"extfs":{"access_key_id":"ak"}}`, opts["external_spec"])
		assert.JSONEq(t, `{"_default":"_default","p_b":"p_b"}`, opts["partition_mapping"])
		// The cut is per PChannel, so it travels as its own map; the scalar
		// end_ts, the L0 flag and the storage version stay the snapshot's own.
		assert.JSONEq(t, `{"rootcoord-dml_0":"100","rootcoord-dml_1":"100"}`, opts["snapshot_pch_end_ts"])
		assert.NotContains(t, opts, "_snapshot_end_ts")
		assert.NotContains(t, opts, "l0_import")
		assert.NotContains(t, opts, "end_ts")
		assert.NotContains(t, opts, "storage_version")
		assert.NotContains(t, opts, "ezk")

		// The schema travels with the system fields appended, as every other
		// replayed message carries it.
		fieldNames := make([]string, 0, len(body.GetSchema().GetFields()))
		for _, field := range body.GetSchema().GetFields() {
			fieldNames = append(fieldNames, field.GetName())
		}
		assert.Contains(t, fieldNames, "RowID")
		assert.Contains(t, fieldNames, "Timestamp")
	}
}

// Without an external spec the target reads the bundle with its own
// credential, so the option is left out rather than sent empty — the server
// rejects an empty spec.
func TestCollSnapDMLTask_OptionsWithoutExternalSpec(t *testing.T) {
	vchannels := newTestVChannels()
	collBackup := newTestCollBackup(vchannels, nil, nil)
	task, err := newCollSnapDMLTask(
		snapDMLTaskArgs{TaskID: "task-1", PchTS: newTestPchTS(vchannels)}, collBackup,
		"s3://bucket/dir/bundle/snapshots/1/metadata/2.json", "", datapb.SnapshotLayout_SnapshotLayoutReferenced)
	require.NoError(t, err)

	opts := task.options()
	assert.NotContains(t, opts, "external_spec")
	assert.NotContains(t, opts, "partition_mapping")
	assert.Equal(t, "referenced", opts["_snapshot_layout"])
}

// The job lifecycle is the binlog path's: submitted with auto_commit off, the
// job is committed over the stream once the target holds it at Uncommitted,
// then waited to Completed.
func TestCollSnapDMLTask_Execute_CommitsOverTheStream(t *testing.T) {
	vchannels := newTestVChannels()
	collBackup := newTestCollBackup(vchannels, []*backuppb.PartitionBackupInfo{
		{PartitionId: 10, PartitionName: "_default"},
	}, nil)
	stream := &recordingStream{}
	task := newTestSnapDMLTask(t, collBackup, stream)
	restful := &sequencedRestful{states: []milvus.ImportState{
		milvus.ImportStateUncommitted,
		milvus.ImportStateCompleted,
	}}
	task.restfulCli = restful

	require.NoError(t, task.Execute(context.Background()))

	var messageTypes []string
	for _, msg := range stream.msgs {
		messageTypes = append(messageTypes, msg.GetProperties()["_t"])
	}
	importType := strconv.Itoa(int(message.MessageTypeImport))
	commitType := strconv.Itoa(_commitImportMessageType)
	assert.Equal(t, []string{importType, importType, commitType, commitType}, messageTypes)
	assert.Equal(t, 2, restful.calls)
}

func TestCollSnapDMLTask_Execute_FailedJob(t *testing.T) {
	collBackup := newTestCollBackup(newTestVChannels(), nil, nil)
	stream := &recordingStream{}
	task := newTestSnapDMLTask(t, collBackup, stream)
	task.restfulCli = &sequencedRestful{states: []milvus.ImportState{milvus.ImportStateFailed}}

	err := task.Execute(context.Background())
	assert.ErrorContains(t, err, "bulk insert failed")
	assert.Empty(t, stream.messagesByType(strconv.Itoa(_commitImportMessageType)))
}

// The bundle is cut per PChannel: every bound is the flush boundary of the
// PChannel those rows live on, so a collection spread over two PChannels keeps
// two bounds and neither is flattened into the other.
func TestSnapshotPChannelEndTS(t *testing.T) {
	t.Run("EveryPChannelBound", func(t *testing.T) {
		collBackup := newTestCollBackup(newTestVChannels(), nil, nil)

		got, err := snapshotPChannelEndTS(collBackup, map[string]uint64{
			"rootcoord-dml_0": 100,
			"rootcoord-dml_1": 200,
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"rootcoord-dml_0":"100","rootcoord-dml_1":"200"}`, got)
	})

	// An unresolvable boundary is refused rather than defaulted: importing that
	// channel uncut is what makes the CDC replay its tail a second time.
	t.Run("MissingFlushBoundaryFails", func(t *testing.T) {
		collBackup := newTestCollBackup(newTestVChannels(), nil, nil)

		_, err := snapshotPChannelEndTS(collBackup, map[string]uint64{"rootcoord-dml_0": 100})
		assert.ErrorContains(t, err, "no flush all ts for pch rootcoord-dml_1")
	})

	t.Run("NoChannelsFails", func(t *testing.T) {
		_, err := snapshotPChannelEndTS(newTestCollBackup(nil, nil, nil), nil)
		assert.ErrorContains(t, err, "carries no virtual channels")
	})

	// Two VChannels sharing one PChannel share its single boundary.
	t.Run("VChannelsSharePChannelBoundary", func(t *testing.T) {
		collBackup := newTestCollBackup([]string{"rootcoord-dml_0_123v0", "rootcoord-dml_0_123v1"}, nil, nil)

		got, err := snapshotPChannelEndTS(collBackup, map[string]uint64{"rootcoord-dml_0": 300})
		require.NoError(t, err)
		assert.JSONEq(t, `{"rootcoord-dml_0":"300"}`, got)
	})
}
