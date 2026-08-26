package restore

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// azureCfg builds an azure backend config for one storage account. The shared
// key has to be a base64-decodable 256-bit value for the SDK to sign with it.
func azureCfg(account, bucket string) storage.Config {
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	return storage.Config{
		Provider:   v2.ProviderAzure,
		Endpoint:   "core.windows.net:443",
		Bucket:     bucket,
		Credential: storage.Credential{Type: storage.Static, AK: account, SK: key, AzureAccountName: account},
	}
}

// extfs decodes the extfs object out of an external spec json, so assertions
// name fields instead of matching escaped json substrings.
func extfs(t *testing.T, spec string) map[string]string {
	t.Helper()

	var parsed struct {
		Extfs map[string]string `json:"extfs"`
	}
	require.NoError(t, json.Unmarshal([]byte(spec), &parsed))

	return parsed.Extfs
}

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

		source, err := newSnapshotSource(t.Context(), milvusCfg, backupCfg, "backup/mybackup/")
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

		source, err := newSnapshotSource(t.Context(), milvusCfg, backupCfg, "backup/mybackup/")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", source.dirURI)
		assert.Contains(t, source.externalSpec, `"access_key_id":"ak"`)
	})

	// A restore whose backup container lives in another storage account reads
	// the bundle blobs under a SAS minted from the backup account's key —
	// Milvus cannot read them with the backup credentials it is handed.
	t.Run("CrossAccountAzureMintsASourceSAS", func(t *testing.T) {
		milvusCfg := azureCfg("milvus-account", "milvus-bucket")
		backupCfg := azureCfg("backup-account", "backup-bucket")

		source, err := newSnapshotSource(t.Context(), milvusCfg, backupCfg, "backup/mybackup/")
		require.NoError(t, err)
		assert.Equal(t, "azure://backup-account.blob.core.windows.net/backup-bucket/backup/mybackup", source.dirURI)

		spec := extfs(t, source.externalSpec)
		assert.Equal(t, "backup-account", spec["access_key_id"])
		assert.NotEmpty(t, spec["source_sas_token"])
		assert.True(t, source.sourceSASSet)
	})

	// The explicit token stands in for a minted one, trimmed to the bare query
	// the extfs field expects.
	t.Run("CrossAccountAzureUsesTheExplicitSAS", func(t *testing.T) {
		milvusCfg := azureCfg("milvus-account", "milvus-bucket")
		backupCfg := azureCfg("backup-account", "backup-bucket")
		backupCfg.SourceSAS = "?sv=2024-08-04&sig=abc"

		source, err := newSnapshotSource(t.Context(), milvusCfg, backupCfg, "backup/mybackup/")
		require.NoError(t, err)
		assert.Equal(t, "sv=2024-08-04&sig=abc", extfs(t, source.externalSpec)["source_sas_token"])
	})

	// A SAS on a copy that stays within one account is a misconfiguration the
	// server rejects, so it fails here with the config named.
	t.Run("SameAccountWithSASFails", func(t *testing.T) {
		milvusCfg := azureCfg("account", "milvus-bucket")
		backupCfg := azureCfg("account", "backup-bucket")
		backupCfg.SourceSAS = "sv=2024-08-04&sig=abc"

		_, err := newSnapshotSource(t.Context(), milvusCfg, backupCfg, "backup/mybackup/")
		assert.ErrorContains(t, err, "crosses azure storage accounts")
	})

	t.Run("NonAzureMilvusWithSASFails", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := azureCfg("backup-account", "backup-bucket")
		backupCfg.SourceSAS = "sv=2024-08-04&sig=abc"

		_, err := newSnapshotSource(t.Context(), milvusCfg, backupCfg, "backup/mybackup/")
		assert.ErrorContains(t, err, "crosses azure storage accounts")
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
