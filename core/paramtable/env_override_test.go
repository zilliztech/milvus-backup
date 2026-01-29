package paramtable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMinimalConfigForEnvTest(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: yaml-address
  port: "19530"

minio:
  address: yaml-minio-address
  port: "9000"
  accessKeyID: yaml-access-key
  secretAccessKey: yaml-secret-key
  bucketName: yaml-bucket
  rootPath: yaml/path

backup:
  parallelism:
    copydata: 64
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)
	return tmpDir
}

func setEnvWithCleanup(t *testing.T, key, value string) {
	t.Helper()
	os.Setenv(key, value)
	t.Cleanup(func() {
		os.Unsetenv(key)
	})
}

// TestEnvOverride_MinioConfig tests environment variable overrides for Minio config
func TestEnvOverride_MinioConfig(t *testing.T) {
	configDir := setupMinimalConfigForEnvTest(t)

	t.Run("MINIO_ADDRESS overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_ADDRESS", "env-minio-address")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-minio-address", params.MinioCfg.Address)
	})

	t.Run("MINIO_PORT overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_PORT", "9999")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "9999", params.MinioCfg.Port)
	})

	t.Run("MINIO_REGION overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_REGION", "us-east-1")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "us-east-1", params.MinioCfg.Region)
	})

	t.Run("MINIO_ACCESS_KEY overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_ACCESS_KEY", "env-access-key")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-access-key", params.MinioCfg.AccessKeyID)
	})

	t.Run("MINIO_SECRET_KEY overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_SECRET_KEY", "env-secret-key")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-secret-key", params.MinioCfg.SecretAccessKey)
	})

	t.Run("MINIO_TOKEN overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_TOKEN", "env-token")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-token", params.MinioCfg.Token)
	})

	t.Run("GCP_KEY_JSON overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "GCP_KEY_JSON", `{"type":"service_account"}`)

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, `{"type":"service_account"}`, params.MinioCfg.GcpCredentialJSON)
	})

	t.Run("MINIO_USE_SSL overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_USE_SSL", "true")

		params := &BackupParams{}
		params.Init()

		assert.True(t, params.MinioCfg.UseSSL)
	})

	t.Run("MINIO_BUCKET_NAME overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BUCKET_NAME", "env-bucket")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-bucket", params.MinioCfg.BucketName)
	})

	t.Run("MINIO_USE_IAM overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_USE_IAM", "true")

		params := &BackupParams{}
		params.Init()

		assert.True(t, params.MinioCfg.UseIAM)
	})

	t.Run("MINIO_IAM_ENDPOINT overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_IAM_ENDPOINT", "http://iam.env.test")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "http://iam.env.test", params.MinioCfg.IAMEndpoint)
	})

	t.Run("MINIO_ROOT_PATH overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_ROOT_PATH", "env/path")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env/path", params.MinioCfg.RootPath)
	})
}

// TestEnvOverride_MinioBackupConfig tests environment variable overrides for backup storage
func TestEnvOverride_MinioBackupConfig(t *testing.T) {
	configDir := setupMinimalConfigForEnvTest(t)

	t.Run("MINIO_BACKUP_BUCKET_NAME overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_BUCKET_NAME", "env-backup-bucket")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-backup-bucket", params.MinioCfg.BackupBucketName)
	})

	t.Run("MINIO_BACKUP_ROOT_PATH overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_ROOT_PATH", "env/backup/path")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env/backup/path", params.MinioCfg.BackupRootPath)
	})

	t.Run("MINIO_BACKUP_ADDRESS overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_ADDRESS", "env-backup-address")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-backup-address", params.MinioCfg.BackupAddress)
	})

	t.Run("MINIO_BACKUP_PORT overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_PORT", "8888")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "8888", params.MinioCfg.BackupPort)
	})

	t.Run("MINIO_BACKUP_REGION overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_REGION", "eu-west-1")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "eu-west-1", params.MinioCfg.BackupRegion)
	})

	t.Run("MINIO_BACKUP_ACCESS_KEY overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_ACCESS_KEY", "env-backup-access-key")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-backup-access-key", params.MinioCfg.BackupAccessKeyID)
	})

	t.Run("MINIO_BACKUP_SECRET_KEY overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_SECRET_KEY", "env-backup-secret-key")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-backup-secret-key", params.MinioCfg.BackupSecretAccessKey)
	})

	t.Run("MINIO_BACKUP_TOKEN overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_TOKEN", "env-backup-token")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-backup-token", params.MinioCfg.BackupToken)
	})

	t.Run("BACKUP_GCP_KEY_JSON overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "BACKUP_GCP_KEY_JSON", `{"type":"backup_service_account"}`)

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, `{"type":"backup_service_account"}`, params.MinioCfg.BackupGcpCredentialJSON)
	})

	t.Run("MINIO_BACKUP_USE_SSL overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_USE_SSL", "true")

		params := &BackupParams{}
		params.Init()

		assert.True(t, params.MinioCfg.BackupUseSSL)
	})

	t.Run("MINIO_BACKUP_USE_IAM overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_USE_IAM", "true")

		params := &BackupParams{}
		params.Init()

		assert.True(t, params.MinioCfg.BackupUseIAM)
	})

	t.Run("MINIO_BACKUP_IAM_ENDPOINT overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MINIO_BACKUP_IAM_ENDPOINT", "http://backup-iam.env.test")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "http://backup-iam.env.test", params.MinioCfg.BackupIAMEndpoint)
	})
}

// TestEnvOverride_MilvusConfig tests environment variable overrides for Milvus config
func TestEnvOverride_MilvusConfig(t *testing.T) {
	configDir := setupMinimalConfigForEnvTest(t)

	t.Run("MILVUS_ADDRESS overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_ADDRESS", "env-milvus-address")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-milvus-address", params.MilvusCfg.Address)
	})

	t.Run("MILVUS_PORT overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_PORT", "19531")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "19531", params.MilvusCfg.Port)
	})

	t.Run("MILVUS_TLS_MODE overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_TLS_MODE", "2")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, 2, params.MilvusCfg.TLSMode)
	})

	t.Run("MILVUS_USER overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_USER", "env-user")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-user", params.MilvusCfg.User)
	})

	t.Run("MILVUS_PASSWORD overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_PASSWORD", "env-password")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-password", params.MilvusCfg.Password)
	})

	t.Run("MILVUS_CA_CERT_PATH overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_CA_CERT_PATH", "/env/path/ca.crt")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "/env/path/ca.crt", params.MilvusCfg.CACertPath)
	})

	t.Run("MILVUS_SERVER_NAME overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_SERVER_NAME", "env-server-name")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-server-name", params.MilvusCfg.ServerName)
	})

	t.Run("MILVUS_MTLS_CERT_PATH overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_MTLS_CERT_PATH", "/env/path/client.crt")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "/env/path/client.crt", params.MilvusCfg.MTLSCertPath)
	})

	t.Run("MILVUS_MTLS_KEY_PATH overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_MTLS_KEY_PATH", "/env/path/client.key")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "/env/path/client.key", params.MilvusCfg.MTLSKeyPath)
	})

	t.Run("MILVUS_RPC_CHANNEL_NAME overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "MILVUS_RPC_CHANNEL_NAME", "env-rpc-channel")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "env-rpc-channel", params.MilvusCfg.RPCChanelName)
	})
}

// TestEnvOverride_BackupConfig tests environment variable overrides for Backup config
func TestEnvOverride_BackupConfig(t *testing.T) {
	configDir := setupMinimalConfigForEnvTest(t)

	t.Run("BACKUP_PARALLELISM_BACKUP_COLLECTION overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "BACKUP_PARALLELISM_BACKUP_COLLECTION", "8")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, int64(8), params.BackupCfg.BackupCollectionParallelism)
	})

	t.Run("BACKUP_PARALLELISM_COPYDATA overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "BACKUP_PARALLELISM_COPYDATA", "256")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, int64(256), params.BackupCfg.BackupCopyDataParallelism)
	})

	t.Run("BACKUP_PARALLELISM_RESTORE_COLLECTION overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "BACKUP_PARALLELISM_RESTORE_COLLECTION", "16")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, 16, params.BackupCfg.RestoreParallelism)
	})

	t.Run("BACKUP_KEEP_TEMP_FILES overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "BACKUP_KEEP_TEMP_FILES", "true")

		params := &BackupParams{}
		params.Init()

		assert.True(t, params.BackupCfg.KeepTempFiles)
	})

	t.Run("BACKUP_GC_PAUSE_ENABLE overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "BACKUP_GC_PAUSE_ENABLE", "true")

		params := &BackupParams{}
		params.Init()

		assert.True(t, params.BackupCfg.GcPauseEnable)
	})

	t.Run("BACKUP_GC_PAUSE_ADDRESS overrides yaml", func(t *testing.T) {
		setEnvWithCleanup(t, "MILVUSCONF", configDir)
		setEnvWithCleanup(t, "BACKUP_GC_PAUSE_ADDRESS", "http://env-gc:9091")

		params := &BackupParams{}
		params.Init()

		assert.Equal(t, "http://env-gc:9091", params.BackupCfg.GcPauseAddress)
	})
}

// TestEnvOverride_Priority tests that env vars override YAML values
func TestEnvOverride_Priority(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: yaml-milvus-address
  port: "19530"

minio:
  address: yaml-minio-address
  bucketName: yaml-bucket
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	setEnvWithCleanup(t, "MILVUSCONF", tmpDir)
	setEnvWithCleanup(t, "MILVUS_ADDRESS", "env-milvus-address")
	setEnvWithCleanup(t, "MINIO_ADDRESS", "env-minio-address")
	setEnvWithCleanup(t, "MINIO_BUCKET_NAME", "env-bucket")

	params := &BackupParams{}
	params.Init()

	// Verify environment variables override YAML values
	assert.Equal(t, "env-milvus-address", params.MilvusCfg.Address)
	assert.Equal(t, "env-minio-address", params.MinioCfg.Address)
	assert.Equal(t, "env-bucket", params.MinioCfg.BucketName)

	// Verify YAML values are used when no env var is set
	assert.Equal(t, "19530", params.MilvusCfg.Port)
}

// TestEnvOverride_EmptyEnvVar tests that empty env vars don't override
func TestEnvOverride_EmptyEnvVarDoesNotOverride(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: yaml-address
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	setEnvWithCleanup(t, "MILVUSCONF", tmpDir)
	// Set empty env var
	setEnvWithCleanup(t, "MILVUS_ADDRESS", "")

	params := &BackupParams{}
	params.Init()

	// Empty env var should not override YAML value
	assert.Equal(t, "yaml-address", params.MilvusCfg.Address)
}
