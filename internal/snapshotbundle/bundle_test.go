package snapshotbundle

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type mapStore struct {
	objects map[string][]byte
}

func newMapStore() *mapStore {
	return &mapStore{objects: make(map[string][]byte)}
}

func (s *mapStore) Read(ctx context.Context, key string) ([]byte, error) {
	data, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	return data, nil
}

func (s *mapStore) Write(ctx context.Context, key string, data []byte) error {
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *mapStore) Remove(ctx context.Context, key string) error {
	delete(s.objects, key)
	return nil
}

func preparedBundle(t *testing.T, store *mapStore) (metadataPath string, metadataURI string) {
	t.Helper()
	source := rewriteTestSnapshot(t)
	source.SegmentIDs = []int64{1001, 1002}
	exported, err := RewriteSnapshotWithMapping(
		source,
		rewriteTestMappings(),
		"bundle",
		"bundle/snapshots/457/metadata/123.json",
	)
	require.NoError(t, err)

	metadataPath, totalBytes, err := PrepareBundleStaged(
		context.Background(),
		store,
		exported,
		"bundle",
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		GetSnapshotStagingMetadataPath("bundle"),
	)
	require.NoError(t, err)
	assert.Equal(t, "bundle/snapshots/457/metadata/123.json", metadataPath)
	assert.Greater(t, totalBytes, int64(0))
	assert.NotEmpty(t, store.objects[GetSnapshotStagingMetadataPath("bundle")])
	assert.NotEmpty(t, store.objects["bundle/snapshots/457/manifests/123/1001.avro"])
	assert.NotEmpty(t, store.objects["bundle/snapshots/457/manifests/123/1002.avro"])
	// An unset source version means the current format, whose manifests are OCF.
	assert.True(t, bytes.HasPrefix(store.objects["bundle/snapshots/457/manifests/123/1001.avro"], []byte(ocfMagic)))
	return metadataPath, "bundle/snapshots/457/metadata/123.json"
}

func TestPrepareAndCommitBundleRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newMapStore()
	metadataPath, metadataURI := preparedBundle(t, store)

	written, err := CommitBundleMetadata(ctx, store, GetSnapshotStagingMetadataPath("bundle"), metadataPath, metadataURI)
	require.NoError(t, err)
	assert.Greater(t, written, int64(0))

	snapshot, err := LoadSnapshotData(ctx, store, metadataPath, true)
	require.NoError(t, err)
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutSelfContained, snapshot.Layout)
	assert.Equal(t, metadataURI, snapshot.SnapshotInfo.GetS3Location())
	require.Len(t, snapshot.Segments, 2)
	// The storage v2 manifest reference survived as an opaque string.
	assert.Equal(t, "bundle/files/"+MarshalManifestPath("files/insert_log/457/2001/1001", 2), snapshot.Segments[0].GetManifestPath())
	basePath, version, err := UnmarshalManifestPath(snapshot.Segments[1].GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, "bundle/files/insert_log/457/2001/1002", basePath)
	assert.EqualValues(t, 3, version)

	// Publishing again proves idempotence and changes nothing.
	republished, err := CommitBundleMetadata(ctx, store, GetSnapshotStagingMetadataPath("bundle"), metadataPath, metadataURI)
	require.NoError(t, err)
	assert.Equal(t, written, republished)

	require.NoError(t, store.Remove(ctx, GetSnapshotStagingMetadataPath("bundle")))
	assert.NotContains(t, store.objects, GetSnapshotStagingMetadataPath("bundle"))
	// The published metadata outlives its staging object.
	assert.Contains(t, store.objects, metadataPath)
}

func TestCommitBundleMetadataValidatesURI(t *testing.T) {
	store := newMapStore()
	metadataPath, _ := preparedBundle(t, store)

	_, err := CommitBundleMetadata(context.Background(), store, GetSnapshotStagingMetadataPath("bundle"), metadataPath, "bundle/snapshots/457/metadata/other.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match the export job")
}

func TestCommitBundleMetadataMissingStaged(t *testing.T) {
	_, err := CommitBundleMetadata(
		context.Background(),
		newMapStore(),
		"bundle/_staging/metadata.json",
		"bundle/snapshots/457/metadata/123.json",
		"bundle/snapshots/457/metadata/123.json",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "staged snapshot metadata object is missing")
}

func TestLoadSnapshotDataSegmentIDMismatch(t *testing.T) {
	ctx := context.Background()
	store := newMapStore()
	source := rewriteTestSnapshot(t)
	source.SegmentIDs = []int64{1001}
	exported, err := RewriteSnapshotWithMapping(
		source,
		rewriteTestMappings(),
		"bundle",
		"bundle/snapshots/457/metadata/123.json",
	)
	require.NoError(t, err)

	metadataPath, _, err := PrepareBundleStaged(
		ctx,
		store,
		exported,
		"bundle",
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		GetSnapshotStagingMetadataPath("bundle"),
	)
	require.NoError(t, err)
	_, err = CommitBundleMetadata(ctx, store, GetSnapshotStagingMetadataPath("bundle"), metadataPath, "bundle/snapshots/457/metadata/123.json")
	require.NoError(t, err)

	_, err = LoadSnapshotData(ctx, store, metadataPath, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "do not match manifests")
}

func TestLoadSnapshotDataMissingMetadata(t *testing.T) {
	_, err := LoadSnapshotData(context.Background(), newMapStore(), "bundle/snapshots/457/metadata/123.json", false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

// A bundle never upgrades the source snapshot's format version: a v4 source
// writes a v4 bundle, which both v4 and v5 servers restore.
func TestPrepareBundleKeepsSourceFormatVersion(t *testing.T) {
	ctx := context.Background()
	store := newMapStore()
	source := rewriteTestSnapshot(t)
	source.FormatVersion = 4
	exported, err := RewriteSnapshotWithMapping(
		source,
		rewriteTestMappings(),
		"bundle",
		"bundle/snapshots/457/metadata/123.json",
	)
	require.NoError(t, err)

	metadataPath, _, err := PrepareBundleStaged(
		ctx, store, exported, "bundle",
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		GetSnapshotStagingMetadataPath("bundle"),
	)
	require.NoError(t, err)
	_, err = CommitBundleMetadata(ctx, store, GetSnapshotStagingMetadataPath("bundle"), metadataPath, "bundle/snapshots/457/metadata/123.json")
	require.NoError(t, err)

	snapshot, err := LoadSnapshotData(ctx, store, metadataPath, true)
	require.NoError(t, err)
	assert.Equal(t, 4, snapshot.FormatVersion)
	require.Len(t, snapshot.Segments, 2)
	// A V4 bundle keeps writing raw binary manifests: only V6+ uses OCF.
	assert.False(t, bytes.HasPrefix(store.objects["bundle/snapshots/457/manifests/123/1001.avro"], []byte(ocfMagic)))
}
