package snapshotbundle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func rewriteTestSnapshot(t *testing.T) *SnapshotData {
	t.Helper()
	return &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:            123,
			CollectionId:  457,
			Name:          "snap",
			S3Location:    "files/snapshots/457/metadata/123.json",
			PinIds:        []int64{9},
			PinExpireAtMs: map[int64]int64{9: 1},
		},
		Collection: &datapb.CollectionDescription{NumShards: 2},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:      1001,
				PartitionId:    2001,
				StorageVersion: 2,
				Binlogs: []*datapb.FieldBinlog{
					fieldBinlog(101, 1, "files/insert_log/457/2001/1001/101/1.parquet"),
				},
				Statslogs: []*datapb.FieldBinlog{
					fieldBinlog(101, 2, "files/stats_log/457/2001/1001/101/2.parquet"),
				},
				ManifestPath: MarshalManifestPath("files/insert_log/457/2001/1001", 2),
			},
			{
				SegmentId:      1002,
				PartitionId:    2001,
				StorageVersion: 3,
				Binlogs: []*datapb.FieldBinlog{
					fieldBinlog(101, 3, "files/insert_log/457/2001/1002/101/1.parquet"),
				},
				ManifestPath: MarshalManifestPath("files/insert_log/457/2001/1002", 3),
			},
		},
		Layout:     datapb.SnapshotLayout_SnapshotLayoutReferenced,
		SegmentIDs: []int64{1001, 1002},
	}
}

func rewriteTestMappings() map[string]string {
	// Storage v2 manifest paths are mapped as opaque strings — restore maps the
	// whole reference back without unmarshaling it. Only v3 manifest roots go
	// through base-path rewriting.
	v2Manifest := MarshalManifestPath("files/insert_log/457/2001/1001", 2)
	return map[string]string{
		v2Manifest:                       "bundle/files/" + v2Manifest,
		"files/insert_log/457/2001/1001": "bundle/files/insert_log/457/2001/1001",
		"files/insert_log/457/2001/1001/101/1.parquet": "bundle/files/insert_log/457/2001/1001/101/1.parquet",
		"files/stats_log/457/2001/1001/101/2.parquet":  "bundle/files/stats_log/457/2001/1001/101/2.parquet",
		"files/insert_log/457/2001/1002":               "bundle/files/insert_log/457/2001/1002",
	}
}

func TestRewriteSnapshotWithMapping(t *testing.T) {
	source := rewriteTestSnapshot(t)
	source.Segments[0].StartPosition = &msgpb.MsgPosition{ChannelName: "ch", Timestamp: 5}

	exported, err := RewriteSnapshotWithMapping(
		source,
		rewriteTestMappings(),
		"bundle",
		"bundle/snapshots/457/metadata/123.json",
	)
	require.NoError(t, err)

	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutSelfContained, exported.Layout)
	assert.Equal(t, "bundle/snapshots/457/metadata/123.json", exported.SnapshotInfo.GetS3Location())
	assert.Empty(t, exported.SnapshotInfo.GetPinIds())
	assert.Empty(t, exported.SnapshotInfo.GetPinExpireAtMs())

	v2 := exported.Segments[0]
	require.Len(t, v2.GetBinlogs(), 1)
	assert.Equal(t, "bundle/files/insert_log/457/2001/1001/101/1.parquet", v2.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	require.Len(t, v2.GetStatslogs(), 1)
	assert.Equal(t, "bundle/files/stats_log/457/2001/1001/101/2.parquet", v2.GetStatslogs()[0].GetBinlogs()[0].GetLogPath())
	// The v2 manifest reference moves as one opaque string under the bundle's
	// files subtree, mirroring how restore maps it back.
	assert.Equal(t, "bundle/files/"+MarshalManifestPath("files/insert_log/457/2001/1001", 2), v2.GetManifestPath())

	v3 := exported.Segments[1]
	// Storage v3 insert files are manifest-owned; the legacy protobuf list is
	// dropped from the export rather than copied twice.
	assert.Empty(t, v3.GetBinlogs())
	basePath, version, err := UnmarshalManifestPath(v3.GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, "bundle/files/insert_log/457/2001/1002", basePath)
	assert.EqualValues(t, 3, version)

	// The in-memory source is untouched by the rewrite.
	assert.Equal(t, "files/snapshots/457/metadata/123.json", source.SnapshotInfo.GetS3Location())
	assert.Len(t, source.SnapshotInfo.GetPinIds(), 1)
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, source.Layout)
	require.Len(t, source.Segments[1].GetBinlogs(), 1)
	assert.True(t, proto.Equal(source.Segments[0].GetStartPosition(), exported.Segments[0].GetStartPosition()))
}

func TestRewriteSnapshotWithMappingMissingMapping(t *testing.T) {
	mappings := rewriteTestMappings()
	delete(mappings, "files/stats_log/457/2001/1001/101/2.parquet")

	_, err := RewriteSnapshotWithMapping(rewriteTestSnapshot(t), mappings, "bundle", "bundle/snapshots/457/metadata/123.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing snapshot file mapping")
}

func TestRewriteSnapshotWithMappingValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(snapshot *SnapshotData)
		message string
	}{
		{"NilSnapshot", func(s *SnapshotData) { s.SnapshotInfo = nil; s.Collection = nil }, "snapshot info cannot be nil"},
		{"NilCollection", func(s *SnapshotData) { s.Collection = nil }, "collection description cannot be nil"},
		{"NilSegment", func(s *SnapshotData) { s.Segments = []*datapb.SegmentDescription{nil} }, "segment at index 0 cannot be nil"},
		{"NilIndex", func(s *SnapshotData) { s.Indexes = []*indexpb.IndexInfo{nil} }, "snapshot index at index 0 cannot be nil"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := rewriteTestSnapshot(t)
			tt.mutate(snapshot)
			_, err := RewriteSnapshotWithMapping(snapshot, rewriteTestMappings(), "bundle", "bundle/snapshots/457/metadata/123.json")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.message)
		})
	}

	_, err := RewriteSnapshotWithMapping(rewriteTestSnapshot(t), rewriteTestMappings(), "", "uri")
	assert.Error(t, err)
	_, err = RewriteSnapshotWithMapping(rewriteTestSnapshot(t), rewriteTestMappings(), "bundle", "")
	assert.Error(t, err)
}

func TestRewriteStorageV3ManifestRequired(t *testing.T) {
	snapshot := rewriteTestSnapshot(t)
	snapshot.Segments[1].ManifestPath = ""

	_, err := RewriteSnapshotWithMapping(snapshot, rewriteTestMappings(), "bundle", "bundle/snapshots/457/metadata/123.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires manifest path")
}
