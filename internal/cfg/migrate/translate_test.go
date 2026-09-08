package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// The loader needs the secret itself, so Translate carries an env-supplied one
// into the config. Migrate deliberately does the opposite, see
// TestMigrate_EnvSecretDeferred.
func TestTranslate_KeepsEnvSecret(t *testing.T) {
	t.Setenv("MINIO_SECRET_KEY", "supersecret")

	out, err := Translate(v1Source(t, `
minio:
  storageType: s3
  accessKeyID: ak
  backupBucketName: backups
`))
	require.NoError(t, err)

	assert.Equal(t, "supersecret", out.Milvus.Storage.Auth.SecretAccessKey.Val)
	// The backup side inherits the primary secret in v1, so it has to end up
	// with the resolved value too rather than the default.
	assert.Equal(t, "supersecret", out.Backup.Storage.Auth.SecretAccessKey.Val)
}

// The env stamp records the v1 variable the value actually came from, under
// the v1 source kind the translation stamps.
func TestTranslate_EnvStampKeepsV1Name(t *testing.T) {
	t.Setenv("MINIO_SECRET_KEY", "supersecret")

	out, err := Translate(v1Source(t, "minio:\n  storageType: s3\n  accessKeyID: ak\n"))
	require.NoError(t, err)

	assert.Equal(t, param.SourceV1Env, out.Milvus.Storage.Auth.SecretAccessKey.Used.Kind)
	assert.Equal(t, "minio_secret_key", out.Milvus.Storage.Auth.SecretAccessKey.Used.Key)
}

// v1 environment variables keep resolving through the translation even for
// fields v2 declares no env name for, such as connection parameters.
func TestTranslate_V1EnvNamesStillResolve(t *testing.T) {
	t.Setenv("MILVUS_ADDRESS", "from-v1-env")
	t.Setenv("MINIO_BUCKET_NAME", "v1-bucket")

	out, err := Translate(v1Source(t, "milvus:\n  port: 19531\n"))
	require.NoError(t, err)

	assert.Equal(t, "from-v1-env", out.Milvus.Grpc.Address.Val)
	assert.Equal(t, param.SourceV1Env, out.Milvus.Grpc.Address.Used.Kind)
	assert.Equal(t, "v1-bucket", out.Milvus.Storage.BucketName.Val)
}

// A v2 environment variable does not apply to a v1 load: the v1 schema never
// read the v2 names, so the translation leaves the environment out of the
// translated source and only v1 names resolve.
func TestTranslate_V2EnvNameInertOnV1Load(t *testing.T) {
	t.Setenv("MINIO_SECRET_KEY", "from-v1-env")
	t.Setenv("MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY", "from-v2-env")

	out, err := Translate(v1Source(t, "minio:\n  storageType: s3\n  accessKeyID: ak\n"))
	require.NoError(t, err)

	assert.Equal(t, "from-v1-env", out.Milvus.Storage.Auth.SecretAccessKey.Val)
	assert.Equal(t, "minio_secret_key", out.Milvus.Storage.Auth.SecretAccessKey.Used.Key)
}

func TestTranslate_MapsRenamedKeys(t *testing.T) {
	out, err := Translate(v1Source(t, `
milvus:
  address: milvus-proxy
  port: 19531
  rpcChannelName: my-replicate
http:
  debugMode: true
cloud:
  address: https://api.example.com
backup:
  gcPause:
    enable: false
    address: http://datacoord:9091
  parallelism:
    backupCollection: 8
    copydata: 32
`))
	require.NoError(t, err)

	assert.Equal(t, "milvus-proxy", out.Milvus.Grpc.Address.Val)
	assert.Equal(t, 19531, out.Milvus.Grpc.Port.Val)
	assert.Equal(t, "my-replicate", out.Milvus.Replicate.RPCChannelName.Val)
	assert.True(t, out.Server.DebugMode.Val)
	assert.Equal(t, "https://api.example.com", out.Cloud.Endpoint.Val)
	assert.Equal(t, "http://datacoord:9091", out.Milvus.Management.Endpoint.Val)
	assert.False(t, out.Backup.PauseGC.Val)
	assert.Equal(t, 8, out.Backup.Concurrency.Collections.Val)
	assert.Equal(t, 32, out.Transfer.Concurrency.Val)
}

// Every value decision Migrate explains with a warning still has to be made
// when nobody is listening. This config trips each report-only path at once:
// the dropped http.enabled, the mutual TLS downgrade, the inherited backup root
// path, and crossStorage against two different backends.
func TestTranslate_SettlesValuesWithoutReport(t *testing.T) {
	out, err := Translate(v1Source(t, `
http:
  enabled: false
milvus:
  tlsMode: 2
minio:
  storageType: s3
  accessKeyID: ak
  secretAccessKey: sk
  rootPath: custom-root
  crossStorage: false
  backupAddress: backup-s3
`))
	require.NoError(t, err)

	// v1 downgraded mutual TLS to server TLS without a client key pair, and v2
	// rejects the un-downgraded value, so the translation has to settle it.
	assert.Equal(t, v2.TLSServer, out.Milvus.Grpc.TLSMode.Val)
	assert.Equal(t, v2.TransferAuto, out.Transfer.Mode.Val)
	// v1 put backup data under a customized Milvus root path; v2 keeps the two
	// independent, so the inherited location is carried over explicitly.
	assert.Equal(t, "custom-root", out.Backup.Storage.RootPath.Val)
}

// The backup root path inheritance follows the Milvus root path whichever
// layer set it, here a --set override.
func TestTranslate_BackupRootPathInheritsFromOverride(t *testing.T) {
	src, err := param.NewSource(writeTempV1(t, "minio:\n  bucketName: b\n"),
		map[string]string{"minio.rootPath": "override-root"})
	require.NoError(t, err)

	out, err := Translate(src)
	require.NoError(t, err)

	assert.Equal(t, "override-root", out.Backup.Storage.RootPath.Val)
	assert.Equal(t, param.SourceOverride, out.Backup.Storage.RootPath.Used.Kind)
	assert.Equal(t, "minio.rootpath", out.Backup.Storage.RootPath.Used.Key)
}

func TestTranslate_CrossStorageStreams(t *testing.T) {
	out, err := Translate(v1Source(t, "minio:\n  crossStorage: true\n"))
	require.NoError(t, err)

	assert.Equal(t, v2.TransferStreaming, out.Transfer.Mode.Val)
}

// The root path inheritance is settled exactly as v1's cmp.Or settled it:
// the Milvus root path value when non-empty, "backup" when it was explicitly
// set empty, and the v1 default "files" when nothing set either.
func TestTranslate_BackupRootPathInheritance(t *testing.T) {
	t.Run("ExplicitEmptyMilvusRootFallsToBackup", func(t *testing.T) {
		out, err := Translate(v1Source(t, "minio:\n  rootPath: \"\"\n"))
		require.NoError(t, err)
		assert.Empty(t, out.Milvus.Storage.RootPath.Val)
		assert.Equal(t, "backup", out.Backup.Storage.RootPath.Val)
		assert.True(t, out.Backup.Storage.RootPath.IsDefault(), "a settled v1 default reports as defaulted")
	})

	t.Run("NothingSetLandsOnV1DefaultFiles", func(t *testing.T) {
		out, err := Translate(v1Source(t, "milvus:\n  user: root\n"))
		require.NoError(t, err)
		assert.Equal(t, "files", out.Backup.Storage.RootPath.Val)
		assert.True(t, out.Backup.Storage.RootPath.IsDefault())
	})
}

// The loader path is what a v1 file hits at startup. Azure with useIAM used to
// be forced to auth.type=sharedKey, and the empty account key then tripped the
// v2 validator ("milvus.storage.auth.accountKey is required"), panicking the
// process. It must translate to auth.type=default instead.
func TestTranslate_AzureUseIAM(t *testing.T) {
	out, err := Translate(v1Source(t, `
minio:
  storageType: azure
  useIAM: true
  accessKeyID: myaccount
  backupAccessKeyID: backupaccount
  backupBucketName: backups
`))
	require.NoError(t, err)

	assert.Equal(t, v2.AuthDefault, out.Milvus.Storage.Auth.Type.Val)
	assert.Equal(t, "myaccount", out.Milvus.Storage.AccountName.Val)
	// The backup side inherits useIAM from the primary in v1, so it must land
	// on default too rather than demand an account key.
	assert.Equal(t, v2.AuthDefault, out.Backup.Storage.Auth.Type.Val)
	assert.Equal(t, "backupaccount", out.Backup.Storage.AccountName.Val)
}

// v1 never validated the provider name and only failed when it came time to
// build a client. Translating runs the v2 validator, so the mistake surfaces
// while the config is being loaded.
func TestTranslate_ReportsValidationError(t *testing.T) {
	_, err := Translate(v1Source(t, "minio:\n  storageType: bogus\n"))

	require.Error(t, err)
	assert.ErrorContains(t, err, "translate v1 config to v2")
	assert.ErrorContains(t, err, "bogus")
}

// Both entry points run the same mapping, so a config with no secret to
// withhold has to translate identically either way.
func TestTranslate_MatchesMigrateWithoutEnvSecrets(t *testing.T) {
	const content = `
milvus:
  address: milvus-proxy
  tlsMode: 1
minio:
  storageType: s3
  accessKeyID: ak
  secretAccessKey: sk
  crossStorage: true
`

	translated, err := Translate(v1Source(t, content))
	require.NoError(t, err)

	migrated, report := migrateRun(t, v1Source(t, content))
	require.NoError(t, report.Err())

	assert.Equal(t, migrated, translated)
}
