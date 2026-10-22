package snapshotbundle

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.True(t, bytes.HasPrefix(data, []byte(ocfMagic)), "v6 manifests are avro containers")
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

	// A future version remains readable: OCF embeds the writer schema and the
	// reader skips fields it does not know.
	future, err := ParseSegmentManifest(data, SnapshotFormatVersion+1)
	require.NoError(t, err)
	assert.Equal(t, segment.GetSegmentId(), future.GetSegmentId())

	_, err = ParseSegmentManifest([]byte("not-avro"), SnapshotFormatVersion)
	require.Error(t, err)
}

func TestSegmentManifestEncodingByVersion(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId:      1001,
		PartitionId:    2001,
		StorageVersion: 2,
		Binlogs: []*datapb.FieldBinlog{
			fieldBinlog(101, 1, "insert-log"),
		},
	}

	t.Run("V5StaysRawBinary", func(t *testing.T) {
		data, err := MarshalSegmentManifest(segment, 5)
		require.NoError(t, err)
		assert.False(t, bytes.HasPrefix(data, []byte(ocfMagic)))

		parsed, err := ParseSegmentManifest(data, 5)
		require.NoError(t, err)
		assert.Equal(t, segment.GetSegmentId(), parsed.GetSegmentId())
	})

	t.Run("V6IsOCF", func(t *testing.T) {
		data, err := MarshalSegmentManifest(segment, 6)
		require.NoError(t, err)
		assert.True(t, bytes.HasPrefix(data, []byte(ocfMagic)))
	})

	t.Run("RawBinaryRejectedAtV6", func(t *testing.T) {
		data, err := MarshalSegmentManifest(segment, 5)
		require.NoError(t, err)

		_, err = ParseSegmentManifest(data, 6)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires an avro container")
	})

	t.Run("OCFRejectedAtV5", func(t *testing.T) {
		data, err := MarshalSegmentManifest(segment, 6)
		require.NoError(t, err)

		_, err = ParseSegmentManifest(data, 5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires a raw binary manifest")
	})

	t.Run("NegativeVersionRejected", func(t *testing.T) {
		_, err := MarshalSegmentManifest(segment, -1)
		require.Error(t, err)

		_, err = ParseSegmentManifest([]byte("not-avro"), -1)
		require.Error(t, err)
	})
}

func TestSegmentManifestIndexMarkerPresence(t *testing.T) {
	t.Run("ExplicitFalseSurvivesV6", func(t *testing.T) {
		segment := &datapb.SegmentDescription{SegmentId: 1001, ManifestHasIndex: proto.Bool(false)}
		data, err := MarshalSegmentManifest(segment, 6)
		require.NoError(t, err)

		parsed, err := ParseSegmentManifest(data, 6)
		require.NoError(t, err)
		require.NotNil(t, parsed.ManifestHasIndex)
		assert.False(t, parsed.GetManifestHasIndex())
	})

	t.Run("PresenceFollowsEmbeddedSchema", func(t *testing.T) {
		// A V4 OCF carries no index marker field. Its presence must follow the
		// embedded schema, not the outer V6 metadata version.
		schema, err := ManifestSchemaV4()
		require.NoError(t, err)
		var buf bytes.Buffer
		encoder, err := ocf.NewEncoderWithSchema(schema, &buf, ocf.WithSchemaMarshaler(ocf.FullSchemaMarshaler))
		require.NoError(t, err)
		require.NoError(t, encoder.Encode(SegmentToManifestEntry(&datapb.SegmentDescription{SegmentId: 1001})))
		require.NoError(t, encoder.Close())

		parsed, err := ParseSegmentManifest(buf.Bytes(), 6)
		require.NoError(t, err)
		assert.Nil(t, parsed.ManifestHasIndex)
	})
}

func TestValidateOCFBlockSizes(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId: 1001,
		Binlogs:   []*datapb.FieldBinlog{fieldBinlog(101, 1, "insert-log")},
	}
	data, err := MarshalSegmentManifest(segment, SnapshotFormatVersion)
	require.NoError(t, err)
	require.NoError(t, validateOCFBlockSizes(data))

	assert.Error(t, validateOCFBlockSizes(data[:len(data)-1]))
	assert.Error(t, validateOCFBlockSizes([]byte(ocfMagic)))
	assert.Error(t, validateOCFBlockSizes([]byte("not-ocf")))
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

func TestParseSnapshotMetadataVersionValidation(t *testing.T) {
	parsed, err := ParseSnapshotMetadata([]byte(`{"formatVersion":` + fmt.Sprint(SnapshotFormatVersion) + `}`))
	require.NoError(t, err)
	assert.EqualValues(t, SnapshotFormatVersion, parsed.GetFormatVersion())

	// There is no reader ceiling: OCF embeds the writer schema, so a newer
	// snapshot stays readable when its unknown fields can be skipped.
	parsed, err = ParseSnapshotMetadata([]byte(`{"formatVersion":` + fmt.Sprint(SnapshotFormatVersion+1) + `}`))
	require.NoError(t, err)
	assert.EqualValues(t, SnapshotFormatVersion+1, parsed.GetFormatVersion())

	_, err = ParseSnapshotMetadata([]byte(`{"formatVersion":-1}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid snapshot format version")
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
