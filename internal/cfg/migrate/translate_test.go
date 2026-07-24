package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// The loader needs the secret itself, so Translate carries an env-supplied one
// into the config. Migrate deliberately does the opposite, see
// TestMigrate_EnvSecretDeferred.
func TestTranslate_KeepsEnvSecret(t *testing.T) {
	t.Setenv("MINIO_SECRET_KEY", "supersecret")

	out, err := Translate(loadV1(t, `
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

func TestTranslate_MapsRenamedKeys(t *testing.T) {
	out, err := Translate(loadV1(t, `
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
	out, err := Translate(loadV1(t, `
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

func TestTranslate_CrossStorageStreams(t *testing.T) {
	out, err := Translate(loadV1(t, "minio:\n  crossStorage: true\n"))
	require.NoError(t, err)

	assert.Equal(t, v2.TransferStreaming, out.Transfer.Mode.Val)
}

// v1 never validated the provider name and only failed when it came time to
// build a client. Translating runs the v2 validator, so the mistake surfaces
// while the config is being loaded.
func TestTranslate_ReportsValidationError(t *testing.T) {
	_, err := Translate(loadV1(t, "minio:\n  storageType: bogus\n"))

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

	translated, err := Translate(loadV1(t, content))
	require.NoError(t, err)

	migrated, report := Migrate(loadV1(t, content))
	require.NoError(t, report.Err())

	assert.Equal(t, migrated, translated)
}
