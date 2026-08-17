package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestNewSnapshotSource(t *testing.T) {
	// Milvus falls back to its own credential when no spec is given, which is what to rely
	// on when both sides are the same backend — it keeps the key off the wire. The uri
	// omits the endpoint for the same reason: it is Milvus's own store, which it resolves
	// through its own storage config.
	t.Run("SameBackendSendsNoSpec", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"

		source, err := newSnapshotSource(milvusCfg, backupCfg, "backup/mybackup/")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", source.dirURI)
		assert.Empty(t, source.externalSpec)
	})

	t.Run("OtherBackendSendsSpec", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"}
		backupCfg := storage.Config{
			Provider: v2.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		source, err := newSnapshotSource(milvusCfg, backupCfg, "backup/mybackup/")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", source.dirURI)
		assert.Contains(t, source.externalSpec, `"access_key_id":"ak"`)
	})
}

func TestSnapshotSource_MetadataURI(t *testing.T) {
	source := snapshotSource{dirURI: "s3://backup-bucket/backup/mybackup"}

	// The meta records the path relative to the backup directory, so a backup that was
	// moved resolves against wherever it is being read from now.
	t.Run("RelativeToTheBackupDir", func(t *testing.T) {
		got, err := source.metadataURI("bundle/snapshots/449577/metadata/449580.json")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle/snapshots/449577/metadata/449580.json", got)
	})

	// A collection with no bundle recorded is a broken backup, not an empty restore.
	t.Run("NoPath", func(t *testing.T) {
		_, err := source.metadataURI("")
		assert.Error(t, err)
	})

	t.Run("AbsoluteURI", func(t *testing.T) {
		_, err := source.metadataURI("s3://other-bucket/snapshots/449577/metadata/449580.json")
		assert.ErrorContains(t, err, "relative")
	})
}
