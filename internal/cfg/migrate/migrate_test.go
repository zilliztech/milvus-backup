package migrate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// loadV1 writes content to a temp file and resolves it with the v1 loader, so a
// test starts from the same resolved config the application would.
func loadV1(t *testing.T, content string) *cfg.Config {
	t.Helper()

	p := filepath.Join(t.TempDir(), "backup.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))

	c, err := cfg.Load(p, nil)
	require.NoError(t, err)

	return c
}

func TestMigrate_RenamedKeys(t *testing.T) {
	src := loadV1(t, `
milvus:
  address: milvus-proxy
  port: 19531
  user: root
  rpcChannelName: my-replicate
  etcd:
    endpoints: etcd-0:2379,etcd-1:2379
    rootPath: my-root
http:
  debugMode: true
  swaggerBasePath: /backup
cloud:
  address: https://api.example.com
log:
  file:
    filename: /var/log/backup.log
    maxSize: 100
backup:
  gcPause:
    enable: false
    address: http://datacoord:9091
  parallelism:
    backupCollection: 8
    backupSegment: 512
    restoreCollection: 3
    importJob: 64
    copydata: 32
`)

	out, _ := Migrate(src)

	assert.Equal(t, "milvus-proxy", out.Milvus.Grpc.Address.Val)
	assert.Equal(t, 19531, out.Milvus.Grpc.Port.Val)
	assert.Equal(t, "root", out.Milvus.User.Val)
	assert.Equal(t, "my-replicate", out.Milvus.Replicate.RPCChannelName.Val)
	assert.Equal(t, []string{"etcd-0:2379", "etcd-1:2379"}, out.Milvus.Etcd.Endpoints.Val)
	assert.Equal(t, "my-root", out.Milvus.Etcd.RootPath.Val)

	assert.True(t, out.Server.DebugMode.Val)
	assert.Equal(t, "/backup", out.Server.SwaggerBasePath.Val)
	assert.Equal(t, "https://api.example.com", out.Cloud.Endpoint.Val)
	assert.Equal(t, "/var/log/backup.log", out.Log.File.Path.Val)
	assert.Equal(t, 100, out.Log.File.MaxSizeMiB.Val)

	// backup.gcPause.address is a management endpoint, not a GC-only knob.
	assert.Equal(t, "http://datacoord:9091", out.Milvus.Management.Endpoint.Val)
	assert.False(t, out.Backup.PauseGC.Val)

	assert.Equal(t, 8, out.Backup.Concurrency.Collections.Val)
	assert.Equal(t, 512, out.Backup.Concurrency.Segments.Val)
	assert.Equal(t, 3, out.Restore.Concurrency.Collections.Val)
	assert.Equal(t, 64, out.Restore.Concurrency.ImportJobs.Val)
	assert.Equal(t, 32, out.Transfer.Concurrency.Val)
}

func TestMigrate_TLSMode(t *testing.T) {
	t.Run("Disabled", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, "milvus:\n  tlsMode: 0\n"))
		assert.Equal(t, v2.TLSDisabled, out.Milvus.Grpc.TLSMode.Val)
	})

	t.Run("Server", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, "milvus:\n  tlsMode: 1\n"))
		assert.Equal(t, v2.TLSServer, out.Milvus.Grpc.TLSMode.Val)
	})

	t.Run("Mutual", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, "milvus:\n  tlsMode: 2\n  mtlsCertPath: /c.pem\n  mtlsKeyPath: /c.key\n"))
		assert.Equal(t, v2.TLSMutual, out.Milvus.Grpc.TLSMode.Val)
	})

	// v1 silently downgraded mutual TLS to server TLS without a key pair. v2
	// rejects it, so the migrator settles the downgrade and warns.
	t.Run("MutualWithoutKeyPairDowngrades", func(t *testing.T) {
		out, report := Migrate(loadV1(t, "milvus:\n  tlsMode: 2\n"))
		assert.Equal(t, v2.TLSServer, out.Milvus.Grpc.TLSMode.Val)
		assert.NoError(t, report.Err())
		require.Len(t, report.Warnings, 1)
		assert.Contains(t, report.Warnings[0], "mutual")
	})
}

func TestMigrate_StorageProviderAlias(t *testing.T) {
	t.Run("Aliyun", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, "minio:\n  storageType: ali\n"))
		assert.Equal(t, v2.ProviderAliyun, out.Milvus.Storage.Provider.Val)
	})

	t.Run("Tencent", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, "minio:\n  storageType: tc\n"))
		assert.Equal(t, v2.ProviderTencent, out.Milvus.Storage.Provider.Val)
	})
}

func TestMigrate_StorageAuth(t *testing.T) {
	t.Run("Static", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, `
minio:
  storageType: s3
  accessKeyID: ak
  secretAccessKey: sk
  token: tok
`))
		s := out.Milvus.Storage
		assert.Equal(t, v2.AuthStatic, s.Auth.Type.Val)
		assert.Equal(t, "ak", s.Auth.AccessKeyID.Val)
		assert.Equal(t, "sk", s.Auth.SecretAccessKey.Val)
		assert.Equal(t, "tok", s.Auth.SessionToken.Val)
	})

	// v1 overloaded accessKeyID as the Azure account name and secretAccessKey
	// as the account key.
	t.Run("Azure", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, `
minio:
  storageType: azure
  accessKeyID: myaccount
  secretAccessKey: mykey
  bucketName: mycontainer
`))
		s := out.Milvus.Storage
		assert.Equal(t, v2.ProviderAzure, s.Provider.Val)
		assert.Equal(t, "myaccount", s.AccountName.Val)
		assert.Equal(t, "mycontainer", s.BucketName.Val)
		assert.Equal(t, v2.AuthSharedKey, s.Auth.Type.Val)
		assert.Equal(t, "mykey", s.Auth.AccountKey.Val)
	})

	t.Run("GCPNative", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, `
minio:
  storageType: gcpnative
  gcpCredentialJSON: /creds.json
`))
		s := out.Milvus.Storage
		assert.Equal(t, v2.AuthServiceAccount, s.Auth.Type.Val)
		assert.Equal(t, "/creds.json", s.Auth.CredentialsFile.Val)
	})

	t.Run("IAM", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, `
minio:
  storageType: aws
  useIAM: true
  iamEndpoint: http://iam.local
`))
		s := out.Milvus.Storage
		assert.Equal(t, v2.AuthIAM, s.Auth.Type.Val)
		assert.Equal(t, "http://iam.local", s.Auth.Endpoint.Val)
	})
}

// A backup destination that only names what differs still describes a complete
// backend, since v1 inherited the rest from the Milvus storage.
func TestMigrate_BackupStorageInheritance(t *testing.T) {
	out, _ := Migrate(loadV1(t, `
minio:
  storageType: s3
  address: s3.amazonaws.com
  port: 443
  useSSL: true
  accessKeyID: ak
  secretAccessKey: sk
  backupBucketName: backup-bucket
`))

	assert.Equal(t, v2.ProviderS3, out.Backup.Storage.Provider.Val)
	assert.Equal(t, "s3.amazonaws.com", out.Backup.Storage.Address.Val)
	assert.True(t, out.Backup.Storage.UseSSL.Val)
	assert.Equal(t, "ak", out.Backup.Storage.Auth.AccessKeyID.Val)
	assert.Equal(t, "backup-bucket", out.Backup.Storage.BucketName.Val)
}

func TestMigrate_CrossStorage(t *testing.T) {
	t.Run("TrueStreams", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, "minio:\n  crossStorage: true\n"))
		assert.Equal(t, v2.TransferStreaming, out.Transfer.Mode.Val)
	})

	t.Run("FalseAuto", func(t *testing.T) {
		out, _ := Migrate(loadV1(t, "minio:\n  crossStorage: false\n"))
		assert.Equal(t, v2.TransferAuto, out.Transfer.Mode.Val)
	})

	// The behavior change only matters when the two backends differ.
	t.Run("WarnsOnlyWhenBackendsDiffer", func(t *testing.T) {
		same, report := Migrate(loadV1(t, "minio:\n  crossStorage: false\n"))
		assert.Empty(t, report.Warnings)
		assert.Equal(t, v2.TransferAuto, same.Transfer.Mode.Val)

		_, report = Migrate(loadV1(t, `
minio:
  crossStorage: false
  address: milvus-minio
  backupAddress: backup-s3
`))
		require.Len(t, report.Warnings, 1)
		assert.Contains(t, report.Warnings[0], "crossStorage")
	})
}

func TestMigrate_DroppedHTTPEnabled(t *testing.T) {
	_, report := Migrate(loadV1(t, "http:\n  enabled: false\n"))
	require.Len(t, report.Warnings, 1)
	assert.Contains(t, report.Warnings[0], "http.enabled")
}

// A secret kept in a v1 environment variable is not written into the file: the
// v2 field stays at its default and the report names the v2 variable to set.
func TestMigrate_EnvSecretDeferred(t *testing.T) {
	t.Setenv("MINIO_SECRET_KEY", "supersecret")

	out, report := Migrate(loadV1(t, "minio:\n  storageType: s3\n  accessKeyID: ak\n"))

	assert.NotEqual(t, "supersecret", out.Milvus.Storage.Auth.SecretAccessKey.Val)
	assert.Contains(t, report.Comments["milvus.storage.auth.secretaccesskey"], "MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY")
	assert.NoError(t, report.Err()) // the empty required secret is expected, not an error

	var warned bool
	for _, w := range report.Warnings {
		if strings.Contains(w, "MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY") {
			warned = true
		}
	}
	assert.True(t, warned, "expected a warning naming the v2 env var")
}

// The backup storage secret inherits the primary secret in v1. When the primary
// came from an environment variable, the inherited backup secret must not be
// written into the file either.
func TestMigrate_InheritedEnvSecretNotLeaked(t *testing.T) {
	t.Setenv("MINIO_SECRET_KEY", "supersecret")

	out, report := Migrate(loadV1(t, `
minio:
  storageType: s3
  accessKeyID: ak
  backupBucketName: backups
`))

	assert.NotEqual(t, "supersecret", out.Milvus.Storage.Auth.SecretAccessKey.Val)
	assert.NotEqual(t, "supersecret", out.Backup.Storage.Auth.SecretAccessKey.Val)
	assert.Contains(t, report.Comments["backup.storage.auth.secretaccesskey"], "BACKUP_STORAGE_AUTH_SECRET_ACCESS_KEY")
}

func TestMigrate_LegacyEnvReport(t *testing.T) {
	t.Setenv("MILVUS_ADDRESS", "milvus-proxy")

	_, report := Migrate(loadV1(t, "milvus:\n  user: root\n"))

	require.Len(t, report.EnvRenames, 1)
	assert.Equal(t, "MILVUS_ADDRESS", report.EnvRenames[0].From)
	assert.Contains(t, report.EnvRenames[0].To, "MILVUS_GRPC_ADDRESS")
}

// Migrating then rendering must produce a file the v2 loader accepts.
func TestMigrate_RendersLoadableV2(t *testing.T) {
	out, report := Migrate(loadV1(t, `
minio:
  storageType: s3
  address: s3.amazonaws.com
  port: 443
  useSSL: true
  accessKeyID: ak
  secretAccessKey: sk
`))
	require.NoError(t, report.Err())

	data, err := v2.Render(out, report.Comments)
	require.NoError(t, err)

	p := filepath.Join(t.TempDir(), "v2.yaml")
	require.NoError(t, os.WriteFile(p, data, 0o600))

	loaded, err := v2.Load(p, nil)
	require.NoError(t, err)
	assert.Equal(t, v2.ProviderS3, loaded.Milvus.Storage.Provider.Val)
	assert.Equal(t, "ak", loaded.Milvus.Storage.Auth.AccessKeyID.Val)
}
