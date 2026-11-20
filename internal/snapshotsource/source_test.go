package snapshotsource

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// azureCfg builds an azure backend config for one storage account. The shared
// key has to be a base64-decodable 256-bit value for the SDK to sign with it.
func azureCfg(account, bucket string) storage.Config {
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	return storage.Config{
		Provider:   cfg.ProviderAzure,
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

func TestNew(t *testing.T) {
	// Milvus falls back to its own credential when no spec is given, which is what to rely
	// on when both sides are the same backend — it keeps the key off the wire. The uri
	// omits the endpoint for the same reason: it is Milvus's own store, which it resolves
	// through its own storage config.
	t.Run("SameBackendSendsNoSpec", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"

		source, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyURI)
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", source.DirURI)
		assert.Empty(t, source.ExternalSpec)
	})

	// Two s3 configs that are not one backend — different regions, different
	// credentials — are still one storage service, so the copy goes ahead with
	// the backup side's credentials spelled out in a spec.
	t.Run("OtherBackendSendsSpec", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: cfg.ProviderS3, Region: "us-east-1", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := storage.Config{
			Provider: cfg.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		source, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyURI)
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", source.DirURI)
		assert.Contains(t, source.ExternalSpec, `"access_key_id":"ak"`)
	})

	// A backup held on another provider's service is unreadable to the
	// server-side copy, so the restore refuses it before any job is submitted.
	t.Run("CrossServiceFails", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"}
		backupCfg := storage.Config{
			Provider: cfg.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		_, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyURI)
		assert.ErrorContains(t, err, "are not the same one")
	})

	// The import path reads the source itself, so a backup on another service is
	// permitted: the spec carries the source's own credentials, and the source
	// reports the crossing so the strategy resolves on it.
	t.Run("CrossServicePermittedForTheImportPath", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"}
		backupCfg := storage.Config{
			Provider: cfg.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		source, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", true, SourceKeyURI)
		require.NoError(t, err)
		assert.False(t, source.SameService)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", source.DirURI)
		assert.Contains(t, source.ExternalSpec, `"access_key_id":"ak"`)
	})

	// A restore whose backup container lives in another storage account reads
	// the bundle blobs under a SAS minted from the backup account's key —
	// Milvus cannot read them with the backup credentials it is handed.
	t.Run("CrossAccountAzureMintsASourceSAS", func(t *testing.T) {
		milvusCfg := azureCfg("milvus-account", "milvus-bucket")
		backupCfg := azureCfg("backup-account", "backup-bucket")

		source, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyURI)
		require.NoError(t, err)
		assert.Equal(t, "azure://backup-account.blob.core.windows.net/backup-bucket/backup/mybackup", source.DirURI)

		spec := extfs(t, source.ExternalSpec)
		assert.Equal(t, "backup-account", spec["access_key_id"])
		assert.NotEmpty(t, spec["source_sas_token"])
		assert.True(t, source.SourceSASSet)
	})

	// The explicit token stands in for a minted one, trimmed to the bare query
	// the extfs field expects.
	t.Run("CrossAccountAzureUsesTheExplicitSAS", func(t *testing.T) {
		milvusCfg := azureCfg("milvus-account", "milvus-bucket")
		backupCfg := azureCfg("backup-account", "backup-bucket")
		backupCfg.SourceSAS = "?sv=2024-08-04&sig=abc"

		source, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyURI)
		require.NoError(t, err)
		assert.Equal(t, "sv=2024-08-04&sig=abc", extfs(t, source.ExternalSpec)["source_sas_token"])
	})

	// A SAS on a copy that stays within one account is a misconfiguration the
	// server rejects, so it fails here with the config named.
	t.Run("SameAccountWithSASFails", func(t *testing.T) {
		milvusCfg := azureCfg("account", "milvus-bucket")
		backupCfg := azureCfg("account", "backup-bucket")
		backupCfg.SourceSAS = "sv=2024-08-04&sig=abc"

		_, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyURI)
		assert.ErrorContains(t, err, "crosses azure storage accounts")
	})

	// A SAS minted for a backup on another provider's service never gets as far
	// as the token check: the copy cannot cross services to begin with.
	t.Run("NonAzureMilvusWithSASFails", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := azureCfg("backup-account", "backup-bucket")
		backupCfg.SourceSAS = "sv=2024-08-04&sig=abc"

		_, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyURI)
		assert.ErrorContains(t, err, "are not the same one")
	})
}

func TestNewRelativeKey(t *testing.T) {
	// The bucket-relative mode names no bucket and no endpoint: every cluster in the
	// restore — the target and each secondary — reads the bundle from its own configured
	// bucket at the key, with its own credential, so no spec goes out even where the
	// backup and the milvus storage are different backends.
	t.Run("ImportPathSendsTheBareKeyAndNoSpec", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio-up:9000", Bucket: "bkt-up"}
		backupCfg := storage.Config{
			Provider: cfg.ProviderMinio, Endpoint: "minio-down:9000", Bucket: "bkt-down",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		source, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", true, SourceKeyRelative)
		require.NoError(t, err)
		assert.Equal(t, "backup/mybackup", source.DirURI)
		assert.Empty(t, source.ExternalSpec)
	})

	// The restore api copies data within one storage service and needs a complete uri
	// to copy from; a bare key is only taken by the import api.
	t.Run("RestoreAPIPathFails", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "bkt"}
		backupCfg := milvusCfg

		_, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", false, SourceKeyRelative)
		assert.ErrorContains(t, err, "only taken by the import api")
	})

	// A uri is not a bucket-relative key; the mode exists to send neither bucket nor
	// endpoint, and a uri here would name both.
	t.Run("URIBackupDirFails", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "bkt"}
		backupCfg := milvusCfg

		_, err := New(t.Context(), milvusCfg, backupCfg, "s3://backup-bucket/backup/mybackup", true, SourceKeyRelative)
		assert.ErrorContains(t, err, "is a uri")
	})

	// The import a source sas would authorize never happens on this path: no spec is
	// sent, and the server rejects a bare key next to a spec anyway.
	t.Run("SourceSASFails", func(t *testing.T) {
		milvusCfg := azureCfg("account", "milvus-bucket")
		backupCfg := azureCfg("account", "backup-bucket")
		backupCfg.SourceSAS = "sv=2024-08-04&sig=abc"

		_, err := New(t.Context(), milvusCfg, backupCfg, "backup/mybackup/", true, SourceKeyRelative)
		assert.ErrorContains(t, err, "crosses azure storage accounts")
	})

	// The empty backup dir would import from the bucket root.
	t.Run("EmptyBackupDirFails", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "bkt"}
		backupCfg := milvusCfg

		_, err := New(t.Context(), milvusCfg, backupCfg, "/", true, SourceKeyRelative)
		assert.Error(t, err)
	})
}

func TestParseSourceKey(t *testing.T) {
	for s, want := range map[string]SourceKey{"": SourceKeyURI, "shared": SourceKeyURI, "own_bucket": SourceKeyRelative} {
		got, err := ParseSourceKey(s)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	_, err := ParseSourceKey("relative")
	assert.ErrorContains(t, err, "only support")
}

func TestSource_MetadataURI(t *testing.T) {
	source := Source{DirURI: "s3://backup-bucket/backup/mybackup"}

	// The meta records the path relative to the backup directory, so a backup that was
	// moved resolves against wherever it is being read from now.
	t.Run("RelativeToTheBackupDir", func(t *testing.T) {
		got, err := source.MetadataURI("bundle/snapshots/449577/metadata/449580.json")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle/snapshots/449577/metadata/449580.json", got)
	})

	// A collection with no bundle recorded is a broken backup, not an empty restore.
	t.Run("NoPath", func(t *testing.T) {
		_, err := source.MetadataURI("")
		assert.Error(t, err)
	})

	t.Run("AbsoluteURI", func(t *testing.T) {
		_, err := source.MetadataURI("s3://other-bucket/snapshots/449577/metadata/449580.json")
		assert.ErrorContains(t, err, "relative")
	})

	// In the bucket-relative mode the join is the same concatenation, but the result
	// is the bare key every cluster resolves inside its own bucket.
	t.Run("RelativeToABareBackupDirKey", func(t *testing.T) {
		source := Source{DirURI: "backup/mybackup"}
		got, err := source.MetadataURI("snapshots/449577/metadata/449580.json")
		require.NoError(t, err)
		assert.Equal(t, "backup/mybackup/snapshots/449577/metadata/449580.json", got)
	})
}
