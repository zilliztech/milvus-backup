package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestNewSnapshotTarget(t *testing.T) {
	// Milvus falls back to its own credential when no spec is given, which is what to
	// rely on when both sides are the same backend — it keeps the key off the wire.
	t.Run("SameBackendSendsNoSpec", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"

		target, err := newSnapshotTarget(milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "minio://minio:9000/backup-bucket/backup/mybackup/bundle", target.Path)
		assert.Equal(t, "bundle", target.Dir)
		assert.Empty(t, target.ExternalSpec)
	})

	t.Run("OtherBackendSendsSpec", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"}
		backupCfg := storage.Config{
			Provider: v2.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		target, err := newSnapshotTarget(milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle", target.Path)
		assert.Contains(t, target.ExternalSpec, `"access_key_id":"ak"`)
	})
}

func TestSnapshotTarget_MetadataPath(t *testing.T) {
	target := snapshotTarget{Path: "s3://backup-bucket/backup/mybackup/bundle", Dir: "bundle"}

	// What lands in the meta is relative to the backup directory, bundle prefix and
	// all, so restore can rebuild the uri without knowing this layout.
	t.Run("RelativeToTheBackupDir", func(t *testing.T) {
		got, err := target.metadataPath("s3://backup-bucket/backup/mybackup/bundle/snapshots/449577/metadata/449580.json")
		require.NoError(t, err)
		assert.Equal(t, "bundle/snapshots/449577/metadata/449580.json", got)
	})

	// Milvus negotiates a per-export directory below the requested bundle root and
	// may report the result using its transport scheme with the default port omitted.
	// This uses synthetic identifiers while preserving the shape of an AWS export.
	t.Run("MilvusCanonicalizedExportURI", func(t *testing.T) {
		target := snapshotTarget{
			Path: "minio://s3.us-west-2.amazonaws.com:443/backup-bucket/instance-a/backup-a/bundle",
			Dir:  "bundle",
		}
		got, err := target.metadataPath("https://s3.us-west-2.amazonaws.com/backup-bucket/instance-a/backup-a/bundle/exports/11111111-2222-3333-4444-555555555555/snapshots/1001/metadata/2002.json")
		require.NoError(t, err)
		assert.Equal(t, "bundle/exports/11111111-2222-3333-4444-555555555555/snapshots/1001/metadata/2002.json", got)
	})

	// Anything outside the target is not ours to record, and a path relative to the
	// backup directory could not describe it anyway.
	t.Run("OutsideTarget", func(t *testing.T) {
		_, err := target.metadataPath("s3://other-bucket/snapshots/449577/metadata/449580.json")
		assert.Error(t, err)
	})

	t.Run("SiblingPrefixIsOutsideTarget", func(t *testing.T) {
		_, err := target.metadataPath("s3://backup-bucket/backup/mybackup/bundle-other/snapshots/449577/metadata/449580.json")
		assert.Error(t, err)
	})

	t.Run("DifferentEndpointIsOutsideTarget", func(t *testing.T) {
		target := snapshotTarget{
			Path: "minio://s3.us-west-2.amazonaws.com:443/backup-bucket/backup/mybackup/bundle",
			Dir:  "bundle",
		}
		_, err := target.metadataPath("https://s3.us-east-1.amazonaws.com/backup-bucket/backup/mybackup/bundle/snapshots/449577/metadata/449580.json")
		assert.Error(t, err)
	})
}
