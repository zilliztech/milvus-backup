package backup

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

func TestNewSnapshotTarget(t *testing.T) {
	// Milvus falls back to its own credential when no spec is given, which is what to
	// rely on when both sides are the same backend — it keeps the key off the wire. The
	// uri omits the endpoint for the same reason: it is Milvus's own store, which it
	// resolves through its own storage config.
	t.Run("SameBackendSendsNoSpec", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"

		target, err := newSnapshotTarget(t.Context(), milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle", target.Path)
		assert.Equal(t, "bundle", target.Dir)
		assert.Empty(t, target.ExternalSpec)
	})

	// A pinned Milvus-view endpoint is written into the uri instead of being omitted.
	t.Run("SameBackendWithMilvusEndpointPinsTheURI", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"
		backupCfg.MilvusEndpoint = "milvus-minio:9000"

		target, err := newSnapshotTarget(t.Context(), milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "minio://milvus-minio:9000/backup-bucket/backup/mybackup/bundle", target.Path)
		assert.Empty(t, target.ExternalSpec)
	})

	t.Run("OtherBackendSendsSpec", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"}
		backupCfg := storage.Config{
			Provider: v2.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		target, err := newSnapshotTarget(t.Context(), milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle", target.Path)
		assert.Contains(t, target.ExternalSpec, `"access_key_id":"ak"`)
	})

	// An export whose backup container lives in another storage account reads
	// the instance blobs under a SAS minted from the instance account's key —
	// Milvus cannot read them with the backup credentials it is handed.
	t.Run("CrossAccountAzureMintsASourceSAS", func(t *testing.T) {
		milvusCfg := azureCfg("milvus-account", "milvus-bucket")
		backupCfg := azureCfg("backup-account", "backup-bucket")

		target, err := newSnapshotTarget(t.Context(), milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "azure://backup-account.blob.core.windows.net/backup-bucket/backup/mybackup/bundle", target.Path)

		spec := extfs(t, target.ExternalSpec)
		assert.Equal(t, "backup-account", spec["access_key_id"])
		assert.NotEmpty(t, spec["source_sas_token"])
		assert.True(t, target.SourceSASSet)
	})

	// The explicit token stands in for a minted one, trimmed to the bare query
	// the extfs field expects.
	t.Run("CrossAccountAzureUsesTheExplicitSAS", func(t *testing.T) {
		milvusCfg := azureCfg("milvus-account", "milvus-bucket")
		milvusCfg.SourceSAS = "?sv=2024-08-04&sig=abc"
		backupCfg := azureCfg("backup-account", "backup-bucket")

		target, err := newSnapshotTarget(t.Context(), milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "sv=2024-08-04&sig=abc", extfs(t, target.ExternalSpec)["source_sas_token"])
	})

	// A SAS on a copy that stays within one account is a misconfiguration the
	// server rejects, so it fails here with the config named.
	t.Run("SameAccountWithSASFails", func(t *testing.T) {
		milvusCfg := azureCfg("account", "milvus-bucket")
		milvusCfg.SourceSAS = "sv=2024-08-04&sig=abc"
		backupCfg := azureCfg("account", "backup-bucket")

		_, err := newSnapshotTarget(t.Context(), milvusCfg, backupCfg, "backup/mybackup")
		assert.ErrorContains(t, err, "crosses azure storage accounts")
	})

	t.Run("NonAzureBackupWithSASFails", func(t *testing.T) {
		milvusCfg := azureCfg("milvus-account", "milvus-bucket")
		milvusCfg.SourceSAS = "sv=2024-08-04&sig=abc"
		backupCfg := storage.Config{
			Provider: v2.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		_, err := newSnapshotTarget(t.Context(), milvusCfg, backupCfg, "backup/mybackup")
		assert.ErrorContains(t, err, "crosses azure storage accounts")
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

	// The endpoint-less target names only a bucket; the export result spells the same
	// bucket back with the endpoint the server resolved from its own storage config.
	t.Run("EndpointlessTargetMatchesResolvedEndpoint", func(t *testing.T) {
		got, err := target.metadataPath("minio://minio:9000/backup-bucket/backup/mybackup/bundle/snapshots/449577/metadata/449580.json")
		require.NoError(t, err)
		assert.Equal(t, "bundle/snapshots/449577/metadata/449580.json", got)
	})

	// A different bucket under the resolved endpoint is still not ours.
	t.Run("EndpointlessTargetRejectsOtherBucket", func(t *testing.T) {
		_, err := target.metadataPath("minio://minio:9000/other-bucket/backup/mybackup/bundle/snapshots/449577/metadata/449580.json")
		assert.Error(t, err)
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
