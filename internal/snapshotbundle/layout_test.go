package snapshotbundle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSnapshotPaths(t *testing.T) {
	manifestDir, metadataPath := GetSnapshotPaths("backup/inst", 457, 123)
	assert.Equal(t, "backup/inst/snapshots/457/manifests/123", manifestDir)
	assert.Equal(t, "backup/inst/snapshots/457/metadata/123.json", metadataPath)

	manifestDir, metadataPath = GetSnapshotPaths("", 457, 123)
	assert.Equal(t, "snapshots/457/manifests/123", manifestDir)
	assert.Equal(t, "snapshots/457/metadata/123.json", metadataPath)
}

func TestGetSnapshotStagingMetadataPath(t *testing.T) {
	assert.Equal(t, "backup/inst/_staging/metadata.json", GetSnapshotStagingMetadataPath("backup/inst"))
}

func TestGetBundleDataRoot(t *testing.T) {
	assert.Equal(t, "backup/inst/files", GetBundleDataRoot("backup/inst"))
}

func TestExportedSnapshotPath(t *testing.T) {
	assert.Equal(t,
		"bundle/files/insert_log/457/458/459/1.parquet",
		ExportedSnapshotPath("files", "files/insert_log/457/458/459/1.parquet", "bundle"))
	// A source key outside the source root is kept whole.
	assert.Equal(t,
		"bundle/files/snapshots/457/metadata/123.json",
		ExportedSnapshotPath("files", "snapshots/457/metadata/123.json", "bundle"))
	// A source key equal to the root collapses onto files/ itself.
	assert.Equal(t, "bundle/files", ExportedSnapshotPath("files", "files", "bundle"))
	// An empty source root keeps everything.
	assert.Equal(t, "bundle/files/a/b", ExportedSnapshotPath("", "a/b", "bundle"))
}

func TestNormalizeObjectPath(t *testing.T) {
	assert.Equal(t, "files/insert_log/a", NormalizeObjectPath("files/insert_log/a"))
	assert.Equal(t, "insert_log/a", NormalizeObjectPath("s3://bucket/insert_log/a"))
	assert.Equal(t, "insert_log/a", NormalizeObjectPath("gs://bucket/insert_log/a"))
	assert.Equal(t, "insert_log/a", NormalizeObjectPath("minio://host:9000/bucket/insert_log/a"))
	assert.Equal(t, "insert_log/a", NormalizeObjectPath("https://host:9000/bucket/insert_log/a"))
	assert.Equal(t, "insert_log/a", NormalizeObjectPath("az://account.blob.core.windows.net/container/insert_log/a"))
	// Endpoint-hosted provider URIs keep the bucket in the first path segment.
	assert.Equal(t, "insert_log/a", NormalizeObjectPath("s3://s3.us-east-1.amazonaws.com/bucket/insert_log/a"))
	// A reference that is not a valid supported URI comes back unchanged.
	assert.Equal(t, "http://host", NormalizeObjectPath("http://host"))
}

func TestDeriveBundleRoot(t *testing.T) {
	root, collID, snapID, ok := DeriveBundleRoot("backup/inst/snapshots/457/metadata/123.json")
	require.True(t, ok)
	assert.Equal(t, "backup/inst", root)
	assert.EqualValues(t, 457, collID)
	assert.EqualValues(t, 123, snapID)

	root, _, _, ok = DeriveBundleRoot("s3://bucket/backup/inst/snapshots/457/metadata/123.json")
	require.True(t, ok)
	assert.Equal(t, "backup/inst", root)

	_, _, _, ok = DeriveBundleRoot("backup/inst/snapshots/457/manifests/123")
	assert.False(t, ok)

	_, _, _, ok = DeriveBundleRoot("backup/inst/snapshots/457/metadata/notjson")
	assert.False(t, ok)

	_, _, _, ok = DeriveBundleRoot("short")
	assert.False(t, ok)
}

func TestManifestPathRoundTrip(t *testing.T) {
	encoded := MarshalManifestPath("files/insert_log/457/458/459", 3)
	basePath, version, err := UnmarshalManifestPath(encoded)
	require.NoError(t, err)
	assert.Equal(t, "files/insert_log/457/458/459", basePath)
	assert.EqualValues(t, 3, version)

	_, _, err = UnmarshalManifestPath("not-json")
	require.Error(t, err)
}
