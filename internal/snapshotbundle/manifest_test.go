package snapshotbundle

import (
	"fmt"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestSegmentManifestRoundTrip(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId:    1001,
		PartitionId:  2001,
		SegmentLevel: datapb.SegmentLevel_L1,
		ChannelName:  "by-dev-rootcoord-dml_0",
		NumOfRows:    3001,
		Binlogs: []*datapb.FieldBinlog{
			fieldBinlog(101, 1, "insert-log"),
		},
		Deltalogs: []*datapb.FieldBinlog{
			fieldBinlog(102, 2, "delta-log"),
		},
		Statslogs: []*datapb.FieldBinlog{
			fieldBinlog(103, 3, "stats-log"),
		},
		Bm25Statslogs: []*datapb.FieldBinlog{
			fieldBinlog(104, 4, "bm25-log"),
		},
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				SegmentID:                 1001,
				FieldID:                   101,
				IndexID:                   201,
				BuildID:                   301,
				IndexName:                 "vec_idx",
				IndexParams:               []*commonpb.KeyValuePair{{Key: "metric_type", Value: "L2"}},
				IndexFilePaths:            []string{"indexes/301/1"},
				SerializedSize:            4096,
				IndexVersion:              5,
				NumRows:                   3001,
				CurrentIndexVersion:       7,
				CurrentScalarIndexVersion: 8,
				MemSize:                   8192,
				IndexStorePathVersion:     indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			},
		},
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			101: {
				FieldID:    101,
				Version:    11,
				Files:      []string{"text/1"},
				LogSize:    12,
				MemorySize: 13,
				BuildID:    14,
			},
		},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			102: {
				FieldID:                102,
				Version:                21,
				Files:                  []string{"json/1"},
				LogSize:                22,
				MemorySize:             23,
				BuildID:                24,
				JsonKeyStatsDataFormat: 25,
			},
		},
		StartPosition:   &msgpb.MsgPosition{ChannelName: "start", MsgID: []byte{1, 2}, MsgGroup: "g1", Timestamp: 100},
		DmlPosition:     &msgpb.MsgPosition{ChannelName: "dml", MsgID: []byte{3, 4}, MsgGroup: "g2", Timestamp: 200},
		StorageVersion:  2,
		IsSorted:        true,
		CommitTimestamp: 999,
	}

	entry := SegmentToManifestEntry(segment)
	restored := ManifestEntryToSegment(entry)
	assert.True(t, proto.Equal(segment, restored))

	data, err := MarshalSegmentManifest(segment, SnapshotFormatVersion)
	require.NoError(t, err)
	parsed, err := ParseSegmentManifest(data, SnapshotFormatVersion)
	require.NoError(t, err)
	assert.Equal(t, segment.GetSegmentId(), parsed.GetSegmentId())
	assert.Equal(t, segment.GetPartitionId(), parsed.GetPartitionId())
	assert.Equal(t, segment.GetSegmentLevel(), parsed.GetSegmentLevel())
	assert.Equal(t, segment.GetChannelName(), parsed.GetChannelName())
	assert.Equal(t, segment.GetNumOfRows(), parsed.GetNumOfRows())
	assert.Equal(t, segment.GetStorageVersion(), parsed.GetStorageVersion())
	assert.Equal(t, segment.GetIsSorted(), parsed.GetIsSorted())
	assert.Equal(t, segment.GetCommitTimestamp(), parsed.GetCommitTimestamp())
	require.Len(t, parsed.GetBinlogs(), 1)
	assert.Equal(t, segment.GetBinlogs()[0].GetBinlogs()[0].GetLogPath(), parsed.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	require.Len(t, parsed.GetDeltalogs(), 1)
	assert.Equal(t, segment.GetDeltalogs()[0].GetBinlogs()[0].GetLogID(), parsed.GetDeltalogs()[0].GetBinlogs()[0].GetLogID())
	require.Len(t, parsed.GetIndexFiles(), 1)
	assert.Equal(t, segment.GetIndexFiles()[0].GetIndexStorePathVersion(), parsed.GetIndexFiles()[0].GetIndexStorePathVersion())
	assert.Equal(t, segment.GetStartPosition().GetTimestamp(), parsed.GetStartPosition().GetTimestamp())
	assert.Equal(t, segment.GetDmlPosition().GetMsgGroup(), parsed.GetDmlPosition().GetMsgGroup())
	assert.Equal(t, segment.GetTextIndexFiles()[101].GetFiles(), parsed.GetTextIndexFiles()[101].GetFiles())
	assert.Equal(t, segment.GetJsonKeyIndexFiles()[102].GetJsonKeyStatsDataFormat(), parsed.GetJsonKeyIndexFiles()[102].GetJsonKeyStatsDataFormat())

	_, err = ParseSegmentManifest(data, SnapshotFormatVersion+1)
	require.Error(t, err)

	_, err = ParseSegmentManifest([]byte("not-avro"), SnapshotFormatVersion)
	require.Error(t, err)
}

func TestParseSegmentManifestEverySchemaVersion(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId:      1001,
		PartitionId:    2001,
		StorageVersion: 2,
		Binlogs: []*datapb.FieldBinlog{
			fieldBinlog(101, 1, "insert-log"),
		},
	}
	for _, version := range []int{0, 1, 2, 3, 4, 5} {
		t.Run(fmt.Sprintf("Version%d", version), func(t *testing.T) {
			schema, err := ManifestSchemaByVersion(version)
			require.NoError(t, err)
			data, err := avro.Marshal(schema, SegmentToManifestEntry(segment))
			require.NoError(t, err)

			parsed, err := ParseSegmentManifest(data, version)
			require.NoError(t, err)
			assert.Equal(t, segment.GetSegmentId(), parsed.GetSegmentId())
			require.Len(t, parsed.GetBinlogs(), 1)
			assert.Equal(t, "insert-log", parsed.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
		})
	}
}

func TestParseSegmentManifestV2DefaultsCommitTimestamp(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId:      1001,
		PartitionId:    2001,
		SegmentLevel:   datapb.SegmentLevel_L1,
		StorageVersion: 2,
		IsSorted:       true,
	}
	schema, err := ManifestSchemaV2()
	require.NoError(t, err)
	data, err := avro.Marshal(schema, SegmentToManifestEntry(segment))
	require.NoError(t, err)

	parsed, err := ParseSegmentManifest(data, 2)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), parsed.GetCommitTimestamp())
}

func TestOptionalManifestConversions(t *testing.T) {
	assert.Nil(t, AvroToMsgPosition(nil))
	assert.Equal(t, &AvroMsgPosition{MsgID: []byte{}}, MsgPositionToAvro(nil))
	assert.Nil(t, TextIndexStatsToAvro(nil))
	assert.Nil(t, AvroToTextIndexStats(nil))
	assert.Nil(t, JSONKeyStatsToAvro(nil))
	assert.Nil(t, AvroToJSONKeyStats(nil))
	assert.Empty(t, TextIndexMapToAvro(nil))
	assert.Empty(t, AvroToTextIndexMap(nil))
	assert.Empty(t, JSONKeyIndexMapToAvro(nil))
	assert.Empty(t, AvroToJSONKeyIndexMap(nil))
}

func TestParseSnapshotMetadataVersionGate(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{FormatVersion: SnapshotFormatVersion + 1}
	data, err := protojson.Marshal(metadata)
	require.NoError(t, err)

	_, err = ParseSnapshotMetadata(data)
	assert.NoError(t, err)
	_, err = ParseSnapshotMetadataWithVersionCheck(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too new")

	parsed, err := ParseSnapshotMetadataWithVersionCheck([]byte(`{"formatVersion":` + fmt.Sprint(SnapshotFormatVersion) + `}`))
	require.NoError(t, err)
	assert.EqualValues(t, SnapshotFormatVersion, parsed.GetFormatVersion())
}

func fieldBinlog(fieldID int64, logID int64, logPath string) *datapb.FieldBinlog {
	return &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: []*datapb.Binlog{
			{
				EntriesNum:    10,
				TimestampFrom: 100,
				TimestampTo:   200,
				LogPath:       logPath,
				LogSize:       300,
				LogID:         logID,
				MemorySize:    400,
			},
		},
	}
}
