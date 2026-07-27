package v2

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// writeYAML writes content to a temp file and returns its path. header is
// prepended so a test only has to spell the section it cares about.
func writeYAML(t *testing.T, content string) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "backup.yaml")
	require.NoError(t, os.WriteFile(p, []byte("configVersion: v2\n"+content), 0o600))

	return p
}

func TestLoad_Defaults(t *testing.T) {
	c, err := Load("", nil)
	require.NoError(t, err)

	assert.Equal(t, "localhost", c.Milvus.Grpc.Address.Val)
	assert.Equal(t, 19530, c.Milvus.Grpc.Port.Val)
	assert.Empty(t, c.Milvus.Rest.Endpoint.Val)
	assert.Equal(t, "http://localhost:9091", c.Milvus.Management.Endpoint.Val)
	assert.Equal(t, []string{"localhost:2379"}, c.Milvus.Etcd.Endpoints.Val)

	assert.Equal(t, ProviderMinio, c.Milvus.Storage.Provider.Val)
	assert.Equal(t, AuthStatic, c.Milvus.Storage.Auth.Type.Val)
	assert.Equal(t, "files", c.Milvus.Storage.RootPath.Val)

	assert.True(t, c.Backup.PauseGC.Val)
	assert.Equal(t, 256, c.Restore.MaxSegmentsPerImportJob.Val)
	assert.Equal(t, TransferAuto, c.Transfer.Mode.Val)
	assert.Equal(t, int64(500), c.Transfer.MultipartCopyThresholdMiB.Val)
}

func TestLoad_CompleteFile(t *testing.T) {
	c, err := Load(filepath.Join("testdata", "complete.yaml"), nil)
	require.NoError(t, err)

	assert.Equal(t, "debug", c.Log.Level.Val)
	assert.Equal(t, "/var/log/backup.log", c.Log.File.Path.Val)
	assert.Equal(t, 100, c.Log.File.MaxSizeMiB.Val)

	assert.True(t, c.Server.DebugMode.Val)
	assert.Equal(t, "/backup", c.Server.SwaggerBasePath.Val)

	assert.Equal(t, "milvus-proxy", c.Milvus.Grpc.Address.Val)
	assert.Equal(t, 19531, c.Milvus.Grpc.Port.Val)
	assert.Equal(t, TLSMutual, c.Milvus.Grpc.TLSMode.Val)
	assert.Equal(t, "https://milvus-rest.example.com", c.Milvus.Rest.Endpoint.Val)
	assert.Equal(t, "http://milvus-datacoord:9091", c.Milvus.Management.Endpoint.Val)
	assert.Equal(t, []string{"etcd-0:2379", "etcd-1:2379"}, c.Milvus.Etcd.Endpoints.Val)

	assert.Equal(t, ProviderS3, c.Milvus.Storage.Provider.Val)
	assert.Equal(t, "milvus-bucket", c.Milvus.Storage.BucketName.Val)
	assert.Equal(t, "milvus-ak", c.Milvus.Storage.Auth.AccessKeyID.Val)

	assert.Equal(t, 8, c.Backup.Concurrency.Collections.Val)
	assert.False(t, c.Backup.PauseGC.Val)
	assert.Equal(t, 3, c.Restore.Concurrency.Collections.Val)
	assert.Equal(t, 128, c.Restore.MaxSegmentsPerImportJob.Val)
	assert.True(t, c.Restore.KeepTempFiles.Val)
	assert.Equal(t, TransferStreaming, c.Transfer.Mode.Val)
	assert.Equal(t, "an-api-key", c.Cloud.APIKey.Val)
}

// The backup destination is usually the backend Milvus already uses, so a
// config that only names what differs still describes a complete backend.
func TestLoad_BackupStorageInheritsMilvusStorage(t *testing.T) {
	c, err := Load(filepath.Join("testdata", "complete.yaml"), nil)
	require.NoError(t, err)

	assert.Equal(t, ProviderS3, c.Backup.Storage.Provider.Val)
	assert.Equal(t, "s3.us-west-2.amazonaws.com", c.Backup.Storage.Address.Val)
	assert.Equal(t, 443, c.Backup.Storage.Port.Val)
	assert.Equal(t, "us-west-2", c.Backup.Storage.Region.Val)
	assert.True(t, c.Backup.Storage.UseSSL.Val)
	assert.Equal(t, "milvus-ak", c.Backup.Storage.Auth.AccessKeyID.Val)

	// Explicitly set fields win over the inherited ones.
	assert.Equal(t, "backup-bucket", c.Backup.Storage.BucketName.Val)

	// The root path is never inherited: backup data does not belong under the
	// Milvus root path.
	t.Run("RootPathIsNotInherited", func(t *testing.T) {
		c, err := Load(writeYAML(t, "milvus:\n  storage:\n    rootPath: milvus-files\n"), nil)
		require.NoError(t, err)

		assert.Equal(t, "milvus-files", c.Milvus.Storage.RootPath.Val)
		assert.Equal(t, "backup", c.Backup.Storage.RootPath.Val)
	})
}

func TestLoad_Version(t *testing.T) {
	t.Run("Missing", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "backup.yaml")
		require.NoError(t, os.WriteFile(p, []byte("milvus:\n  user: root\n"), 0o600))

		_, err := Load(p, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configVersion")
	})

	t.Run("V1", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "backup.yaml")
		require.NoError(t, os.WriteFile(p, []byte("configVersion: v1\n"), 0o600))

		_, err := Load(p, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `only accepts "v2"`)
	})
}

// checkKeys warns about every unknown key but never fails the load, so the
// warnings are asserted on its return value rather than on an error.
func TestCheckKeys(t *testing.T) {
	cfg := New()

	warningsFor := func(t *testing.T, yaml string, overrides map[string]string) []string {
		t.Helper()

		var path string
		if yaml != "" {
			path = writeYAML(t, yaml)
		}
		src, err := param.NewSource(path, overrides)
		require.NoError(t, err)

		return checkKeys(cfg, src)
	}

	t.Run("Misspelled", func(t *testing.T) {
		w := warningsFor(t, "milvus:\n  grpc:\n    adress: localhost\n", nil) //nolint:misspell // the typo is what is under test
		require.Len(t, w, 1)
		assert.Contains(t, w[0], `unknown v2 config file key "milvus.grpc.adress"`)
		assert.Contains(t, w[0], "ignoring it")
	})

	t.Run("V1Key", func(t *testing.T) {
		w := warningsFor(t, "milvus:\n  address: localhost\n", nil)
		require.Len(t, w, 1)
		assert.Contains(t, w[0], `v1 config file key "milvus.address"`)
		assert.Contains(t, w[0], "milvus.grpc.address")
	})

	t.Run("RemovedV1Key", func(t *testing.T) {
		w := warningsFor(t, "http:\n  enabled: true\n", nil)
		require.Len(t, w, 1)
		assert.Contains(t, w[0], `"http.enabled" was removed in v2`)
	})

	t.Run("EveryUnknownKeyIsReported", func(t *testing.T) {
		w := warningsFor(t, "minio:\n  address: localhost\n  port: 9000\n", nil)
		joined := strings.Join(w, "\n")
		assert.Contains(t, joined, "milvus.storage.address")
		assert.Contains(t, joined, "milvus.storage.port")
	})

	// A v1 variable that configured something other than a credential is
	// answered with a config key: v2 has no environment variable to rename it to.
	t.Run("V1Override", func(t *testing.T) {
		w := warningsFor(t, "", map[string]string{"MILVUS_ADDRESS": "localhost"})
		require.Len(t, w, 1)
		assert.Contains(t, w[0], `v1 --set key "MILVUS_ADDRESS"`)
		assert.Contains(t, w[0], "milvus.grpc.address")
	})

	t.Run("V1CredentialOverride", func(t *testing.T) {
		w := warningsFor(t, "", map[string]string{"MINIO_SECRET_KEY": "sk"})
		require.Len(t, w, 1)
		assert.Contains(t, w[0], "MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY")
	})

	t.Run("UnknownOverride", func(t *testing.T) {
		w := warningsFor(t, "", map[string]string{"milvus.grpc.hostname": "localhost"})
		require.Len(t, w, 1)
		assert.Contains(t, w[0], `unknown v2 --set key "milvus.grpc.hostname"`)
	})
}

// An unknown key is warned about and then ignored, so loading succeeds and the
// affected field keeps its default rather than the stray value.
func TestLoad_UnknownKeysAreIgnored(t *testing.T) {
	t.Run("MisspelledConfigKey", func(t *testing.T) {
		c, err := Load(writeYAML(t, "milvus:\n  grpc:\n    adress: 10.0.0.1\n"), nil) //nolint:misspell // the typo is what is under test
		require.NoError(t, err)
		assert.Equal(t, "localhost", c.Milvus.Grpc.Address.Val)
	})

	t.Run("V1Override", func(t *testing.T) {
		c, err := Load("", map[string]string{"MILVUS_ADDRESS": "10.0.0.1"})
		require.NoError(t, err)
		assert.Equal(t, "localhost", c.Milvus.Grpc.Address.Val)
	})
}

// Precedence is exercised on a credential, since those are the only parameters
// an environment variable can reach.
func TestLoad_Precedence(t *testing.T) {
	p := writeYAML(t, "milvus:\n  user: fromfile\n")

	t.Run("ConfigFile", func(t *testing.T) {
		c, err := Load(p, nil)
		require.NoError(t, err)
		assert.Equal(t, "fromfile", c.Milvus.User.Val)
	})

	t.Run("EnvOverridesConfigFile", func(t *testing.T) {
		t.Setenv("MILVUS_USER", "fromenv")

		c, err := Load(p, nil)
		require.NoError(t, err)
		assert.Equal(t, "fromenv", c.Milvus.User.Val)
	})

	t.Run("OverrideOverridesEnv", func(t *testing.T) {
		t.Setenv("MILVUS_USER", "fromenv")

		c, err := Load(p, map[string]string{"MILVUS_USER": "fromset"})
		require.NoError(t, err)
		assert.Equal(t, "fromset", c.Milvus.User.Val)
	})

	t.Run("OverrideByConfigKey", func(t *testing.T) {
		c, err := Load(p, map[string]string{"milvus.user": "frombykey"})
		require.NoError(t, err)
		assert.Equal(t, "frombykey", c.Milvus.User.Val)
	})

	// A v1 environment variable is not an alias: it names nothing in v2.
	t.Run("V1EnvIsIgnored", func(t *testing.T) {
		t.Setenv("MILVUS_PORT", "23456")

		c, err := Load(writeYAML(t, "milvus:\n  grpc:\n    port: 12345\n"), nil)
		require.NoError(t, err)
		assert.Equal(t, 12345, c.Milvus.Grpc.Port.Val)
	})
}

// Everything that is not a credential is named by its config key alone, so an
// environment variable spelled after it is inert — including the Kubernetes
// service variables whose names collide with a connection parameter.
func TestLoad_EnvIsCredentialsOnly(t *testing.T) {
	p := writeYAML(t, "milvus:\n  grpc:\n    port: 12345\n")

	t.Run("ConnectionEnvIsIgnored", func(t *testing.T) {
		t.Setenv("MILVUS_GRPC_PORT", "tcp://10.0.0.1:19530")

		c, err := Load(p, nil)
		require.NoError(t, err)
		assert.Equal(t, 12345, c.Milvus.Grpc.Port.Val)
	})

	t.Run("StorageEnvIsIgnored", func(t *testing.T) {
		t.Setenv("MILVUS_STORAGE_ADDRESS", "10.0.0.1")

		c, err := Load(p, nil)
		require.NoError(t, err)
		assert.Equal(t, "localhost", c.Milvus.Storage.Address.Val)
	})

	t.Run("CredentialEnvApplies", func(t *testing.T) {
		t.Setenv("MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY", "sk")

		c, err := Load(p, nil)
		require.NoError(t, err)
		assert.Equal(t, "sk", c.Milvus.Storage.Auth.SecretAccessKey.Val)
	})
}

func TestLoad_EtcdEndpoints(t *testing.T) {
	t.Run("List", func(t *testing.T) {
		c, err := Load(writeYAML(t, "milvus:\n  etcd:\n    endpoints:\n      - a:2379\n      - b:2379\n"), nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"a:2379", "b:2379"}, c.Milvus.Etcd.Endpoints.Val)
	})

	// v1 spelled this as one comma-separated string, which is worth an explicit
	// error rather than a single endpoint named "a:2379,b:2379".
	t.Run("V1CommaSeparatedString", func(t *testing.T) {
		_, err := Load(writeYAML(t, "milvus:\n  etcd:\n    endpoints: a:2379,b:2379\n"), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be a YAML list")
	})

	t.Run("Override", func(t *testing.T) {
		c, err := Load("", map[string]string{"milvus.etcd.endpoints": "a:2379, b:2379"})
		require.NoError(t, err)
		assert.Equal(t, []string{"a:2379", "b:2379"}, c.Milvus.Etcd.Endpoints.Val)
	})
}

func TestConfig_WriteTable(t *testing.T) {
	c, err := Load("", map[string]string{"milvus.password": "supersecret123"})
	require.NoError(t, err)

	var buf strings.Builder
	require.NoError(t, c.WriteTable(&buf))

	out := buf.String()
	assert.Contains(t, out, "Milvus.Grpc.Address")
	assert.Contains(t, out, "Milvus.Storage.Auth.AccessKeyID")
	assert.Contains(t, out, "Backup.Storage.RootPath")
	assert.Contains(t, out, "override")
	assert.NotContains(t, out, "supersecret123")
	assert.Contains(t, out, "su****23")
}
