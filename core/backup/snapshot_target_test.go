package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func minioCfg(bucket string) storage.Config {
	return storage.Config{
		Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: bucket,
		Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
	}
}

func TestNewSnapshotTarget(t *testing.T) {
	// The bundle root is one key prefix in the backup storage; the uri names the
	// same prefix on the storage service the server resolves, so restore can
	// rebuild it from the recorded relative path.
	t.Run("SameBackend", func(t *testing.T) {
		milvusCfg := minioCfg("milvus-bucket")
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"

		target, err := newSnapshotTarget(milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "backup/mybackup/bundle", target.dir)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle", target.rootURI)
	})

	// A pinned milvus-view endpoint is written into the uri instead of being omitted.
	t.Run("SameBackendWithMilvusEndpointPinsTheURI", func(t *testing.T) {
		milvusCfg := minioCfg("milvus-bucket")
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"
		backupCfg.MilvusEndpoint = "milvus-minio:9000"

		target, err := newSnapshotTarget(milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "minio://milvus-minio:9000/backup-bucket/backup/mybackup/bundle", target.rootURI)
	})
}
