package paramtable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupFullConfig(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	configContent := `
log:
  level: debug
  console: false
  file:
    filename: "test.log"
    maxSize: 100
    maxDays: 7
    maxBackups: 5

http:
  enabled: true
  debug_mode: true
  simpleResponse: false

cloud:
  address: https://test.cloud.zilliz.com
  apikey: test-api-key

milvus:
  address: test-milvus-host
  port: "19530"
  user: test-user
  password: test-password
  tlsMode: 1
  caCertPath: /path/to/ca.crt
  serverName: test-server
  mtlsCertPath: /path/to/client.crt
  mtlsKeyPath: /path/to/client.key
  rpcChannelName: test-channel
  etcd:
    endpoints: etcd1:2379,etcd2:2379
    rootPath: test-root

minio:
  storageType: s3
  address: s3.amazonaws.com
  port: "443"
  region: us-west-2
  accessKeyID: AKIAIOSFODNN7EXAMPLE
  secretAccessKey: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  token: test-token
  gcpCredentialJSON: '{"type":"service_account"}'
  useSSL: true
  useIAM: true
  iamEndpoint: http://iam.test
  cloudProvider: aws
  bucketName: test-bucket
  rootPath: test/path
  backupStorageType: gcp
  backupAddress: storage.googleapis.com
  backupPort: "443"
  backupRegion: us-central1
  backupAccessKeyID: backup-key-id
  backupSecretAccessKey: backup-secret-key
  backupToken: backup-token
  backupGcpCredentialJSON: '{"type":"service_account","backup":true}'
  backupBucketName: backup-bucket
  backupRootPath: backup/path
  backupUseSSL: true
  backupUseIAM: false
  backupIamEndpoint: http://backup-iam.test
  crossStorage: true

backup:
  parallelism:
    copydata: 64
    backupCollection: 2
    backupSegment: 512
    restoreCollection: 4
    importJob: 256
  keepTempFiles: true
  gcPause:
    enable: true
    address: http://gc-pause:9091
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)
	return tmpDir
}

func newBackupParamsWithConfig(t *testing.T, configDir string) *BackupParams {
	t.Helper()
	os.Setenv("MILVUSCONF", configDir)
	t.Cleanup(func() {
		os.Unsetenv("MILVUSCONF")
	})

	params := &BackupParams{}
	params.Init()
	return params
}

// TestBackupParams_Init tests the full initialization of BackupParams
func TestBackupParams_Init(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	// Just verify it initialized without panic
	assert.NotNil(t, params)
}

// TestLogConfig tests LogConfig initialization
func TestLogConfig(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	t.Run("Level", func(t *testing.T) {
		assert.Equal(t, "debug", params.LogCfg.Level)
	})

	t.Run("Console", func(t *testing.T) {
		assert.False(t, params.LogCfg.Console)
	})

	t.Run("File settings", func(t *testing.T) {
		assert.Equal(t, "test.log", params.LogCfg.File.Filename)
		assert.Equal(t, 100, params.LogCfg.File.MaxSize)
		assert.Equal(t, 7, params.LogCfg.File.MaxDays)
		assert.Equal(t, 5, params.LogCfg.File.MaxBackups)
	})
}

// TestLogConfig_Defaults tests LogConfig default values
func TestLogConfig_Defaults(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	t.Run("Default level", func(t *testing.T) {
		assert.Equal(t, DefaultLogLevel, params.LogCfg.Level)
	})

	t.Run("Default console", func(t *testing.T) {
		assert.True(t, params.LogCfg.Console)
	})

	t.Run("Default file settings", func(t *testing.T) {
		assert.Equal(t, "logs/backup.log", params.LogCfg.File.Filename)
		assert.Equal(t, 300, params.LogCfg.File.MaxSize)
		assert.Equal(t, 0, params.LogCfg.File.MaxDays)
		assert.Equal(t, 0, params.LogCfg.File.MaxBackups)
	})
}

// TestHTTPConfig tests HTTPConfig initialization
func TestHTTPConfig(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	assert.True(t, params.HTTPCfg.Enabled)
	assert.True(t, params.HTTPCfg.DebugMode)
	assert.False(t, params.HTTPCfg.SimpleResponse)
}

// TestHTTPConfig_Defaults tests HTTPConfig default values
func TestHTTPConfig_Defaults(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	assert.True(t, params.HTTPCfg.Enabled)
	assert.False(t, params.HTTPCfg.DebugMode)
	assert.False(t, params.HTTPCfg.SimpleResponse)
}

// TestCloudConfig tests CloudConfig initialization
func TestCloudConfig(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	assert.Equal(t, "https://test.cloud.zilliz.com", params.CloudConfig.Address)
	assert.Equal(t, "test-api-key", params.CloudConfig.APIKey)
}

// TestCloudConfig_Defaults tests CloudConfig default values
func TestCloudConfig_Defaults(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	assert.Equal(t, DefaultCloudAddress, params.CloudConfig.Address)
	assert.Equal(t, "", params.CloudConfig.APIKey)
}

// TestMilvusConfig tests MilvusConfig initialization
func TestMilvusConfig(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	t.Run("Basic settings", func(t *testing.T) {
		assert.Equal(t, "test-milvus-host", params.MilvusCfg.Address)
		assert.Equal(t, "19530", params.MilvusCfg.Port)
	})

	t.Run("Auth settings", func(t *testing.T) {
		assert.Equal(t, "test-user", params.MilvusCfg.User)
		assert.Equal(t, "test-password", params.MilvusCfg.Password)
	})

	t.Run("TLS settings", func(t *testing.T) {
		assert.Equal(t, 1, params.MilvusCfg.TLSMode)
		assert.Equal(t, "/path/to/ca.crt", params.MilvusCfg.CACertPath)
		assert.Equal(t, "test-server", params.MilvusCfg.ServerName)
		assert.Equal(t, "/path/to/client.crt", params.MilvusCfg.MTLSCertPath)
		assert.Equal(t, "/path/to/client.key", params.MilvusCfg.MTLSKeyPath)
	})

	t.Run("Etcd settings", func(t *testing.T) {
		assert.Equal(t, "etcd1:2379,etcd2:2379", params.MilvusCfg.EtcdConfig.Endpoints)
		assert.Equal(t, "test-root", params.MilvusCfg.EtcdConfig.RootPath)
	})

	t.Run("RPC channel", func(t *testing.T) {
		assert.Equal(t, "test-channel", params.MilvusCfg.RPCChanelName)
	})
}

// TestMilvusConfig_Defaults tests MilvusConfig default values
func TestMilvusConfig_Defaults(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	t.Run("Default TLS", func(t *testing.T) {
		assert.Equal(t, 0, params.MilvusCfg.TLSMode)
		assert.Equal(t, "", params.MilvusCfg.CACertPath)
		assert.Equal(t, "", params.MilvusCfg.ServerName)
		assert.Equal(t, "", params.MilvusCfg.MTLSCertPath)
		assert.Equal(t, "", params.MilvusCfg.MTLSKeyPath)
	})

	t.Run("Default etcd", func(t *testing.T) {
		assert.Equal(t, "localhost:2379", params.MilvusCfg.EtcdConfig.Endpoints)
		assert.Equal(t, "by-dev", params.MilvusCfg.EtcdConfig.RootPath)
	})

	t.Run("Default RPC channel", func(t *testing.T) {
		assert.Equal(t, "by-dev-replicate-msg", params.MilvusCfg.RPCChanelName)
	})
}

// TestMilvusConfig_EmptyUserPassword tests that empty user/password don't cause issues
func TestMilvusConfig_EmptyUserPassword(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	// Should not panic with missing user/password
	assert.Equal(t, "", params.MilvusCfg.User)
	assert.Equal(t, "", params.MilvusCfg.Password)
}

// TestMinioConfig tests MinioConfig initialization
func TestMinioConfig(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	t.Run("Storage type", func(t *testing.T) {
		assert.Equal(t, "s3", params.MinioCfg.StorageType)
	})

	t.Run("Basic settings", func(t *testing.T) {
		assert.Equal(t, "s3.amazonaws.com", params.MinioCfg.Address)
		assert.Equal(t, "443", params.MinioCfg.Port)
		assert.Equal(t, "us-west-2", params.MinioCfg.Region)
	})

	t.Run("Auth settings", func(t *testing.T) {
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", params.MinioCfg.AccessKeyID)
		assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", params.MinioCfg.SecretAccessKey)
		assert.Equal(t, "test-token", params.MinioCfg.Token)
		assert.Equal(t, `{"type":"service_account"}`, params.MinioCfg.GcpCredentialJSON)
	})

	t.Run("SSL and IAM", func(t *testing.T) {
		assert.True(t, params.MinioCfg.UseSSL)
		assert.True(t, params.MinioCfg.UseIAM)
		assert.Equal(t, "http://iam.test", params.MinioCfg.IAMEndpoint)
	})

	t.Run("Bucket settings", func(t *testing.T) {
		assert.Equal(t, "test-bucket", params.MinioCfg.BucketName)
		assert.Equal(t, "test/path", params.MinioCfg.RootPath)
	})

	t.Run("Cloud provider", func(t *testing.T) {
		assert.Equal(t, "aws", params.MinioCfg.CloudProvider)
	})
}

// TestMinioConfig_BackupSettings tests MinioConfig backup storage settings
func TestMinioConfig_BackupSettings(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	t.Run("Backup storage type", func(t *testing.T) {
		assert.Equal(t, "gcp", params.MinioCfg.BackupStorageType)
	})

	t.Run("Backup basic settings", func(t *testing.T) {
		assert.Equal(t, "storage.googleapis.com", params.MinioCfg.BackupAddress)
		assert.Equal(t, "443", params.MinioCfg.BackupPort)
		assert.Equal(t, "us-central1", params.MinioCfg.BackupRegion)
	})

	t.Run("Backup auth settings", func(t *testing.T) {
		assert.Equal(t, "backup-key-id", params.MinioCfg.BackupAccessKeyID)
		assert.Equal(t, "backup-secret-key", params.MinioCfg.BackupSecretAccessKey)
		assert.Equal(t, "backup-token", params.MinioCfg.BackupToken)
		assert.Equal(t, `{"type":"service_account","backup":true}`, params.MinioCfg.BackupGcpCredentialJSON)
	})

	t.Run("Backup SSL and IAM", func(t *testing.T) {
		assert.True(t, params.MinioCfg.BackupUseSSL)
		assert.False(t, params.MinioCfg.BackupUseIAM)
		assert.Equal(t, "http://backup-iam.test", params.MinioCfg.BackupIAMEndpoint)
	})

	t.Run("Backup bucket settings", func(t *testing.T) {
		assert.Equal(t, "backup-bucket", params.MinioCfg.BackupBucketName)
		assert.Equal(t, "backup/path", params.MinioCfg.BackupRootPath)
	})

	t.Run("Cross storage", func(t *testing.T) {
		assert.True(t, params.MinioCfg.CrossStorage)
	})
}

// TestMinioConfig_Defaults tests MinioConfig default values
func TestMinioConfig_Defaults(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	t.Run("Default storage type", func(t *testing.T) {
		assert.Equal(t, DefaultStorageType, params.MinioCfg.StorageType)
	})

	t.Run("Default basic settings", func(t *testing.T) {
		assert.Equal(t, DefaultMinioAddress, params.MinioCfg.Address)
		assert.Equal(t, DefaultMinioPort, params.MinioCfg.Port)
		assert.Equal(t, "", params.MinioCfg.Region)
	})

	t.Run("Default auth settings", func(t *testing.T) {
		assert.Equal(t, DefaultMinioAccessKey, params.MinioCfg.AccessKeyID)
		assert.Equal(t, DefaultMinioSecretAccessKey, params.MinioCfg.SecretAccessKey)
		assert.Equal(t, "", params.MinioCfg.Token)
	})

	t.Run("Default SSL and IAM", func(t *testing.T) {
		assert.False(t, params.MinioCfg.UseSSL)
		assert.False(t, params.MinioCfg.UseIAM)
		assert.Equal(t, DefaultMinioIAMEndpoint, params.MinioCfg.IAMEndpoint)
	})

	t.Run("Default bucket settings", func(t *testing.T) {
		assert.Equal(t, DefaultMinioBucketName, params.MinioCfg.BucketName)
		assert.Equal(t, DefaultMinioRootPath, params.MinioCfg.RootPath)
	})

	t.Run("Default backup inherits from main", func(t *testing.T) {
		assert.Equal(t, DefaultStorageType, params.MinioCfg.BackupStorageType)
		assert.Equal(t, DefaultMinioAddress, params.MinioCfg.BackupAddress)
		assert.Equal(t, DefaultMinioPort, params.MinioCfg.BackupPort)
		assert.Equal(t, DefaultMinioAccessKey, params.MinioCfg.BackupAccessKeyID)
		assert.Equal(t, DefaultMinioSecretAccessKey, params.MinioCfg.BackupSecretAccessKey)
		assert.False(t, params.MinioCfg.BackupUseSSL)
	})

	t.Run("Default cross storage", func(t *testing.T) {
		assert.False(t, params.MinioCfg.CrossStorage)
	})
}

// TestMinioConfig_RootPathTrimSlash tests that root path leading slash is trimmed
func TestMinioConfig_RootPathTrimSlash(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"

minio:
  rootPath: "/leading/slash/path"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	assert.Equal(t, "leading/slash/path", params.MinioCfg.RootPath)
}

// TestBackupConfig tests BackupConfig initialization
func TestBackupConfig(t *testing.T) {
	configDir := setupFullConfig(t)
	params := newBackupParamsWithConfig(t, configDir)

	t.Run("Parallelism settings", func(t *testing.T) {
		assert.Equal(t, int64(2), params.BackupCfg.BackupCollectionParallelism)
		assert.Equal(t, int64(512), params.BackupCfg.BackupSegmentParallelism)
		assert.Equal(t, int64(64), params.BackupCfg.BackupCopyDataParallelism)
		assert.Equal(t, 4, params.BackupCfg.RestoreParallelism)
		assert.Equal(t, int64(256), params.BackupCfg.ImportJobParallelism)
	})

	t.Run("Keep temp files", func(t *testing.T) {
		assert.True(t, params.BackupCfg.KeepTempFiles)
	})

	t.Run("GC pause settings", func(t *testing.T) {
		assert.True(t, params.BackupCfg.GcPauseEnable)
		assert.Equal(t, "http://gc-pause:9091", params.BackupCfg.GcPauseAddress)
	})
}

// TestBackupConfig_Defaults tests BackupConfig default values
func TestBackupConfig_Defaults(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	params := newBackupParamsWithConfig(t, tmpDir)

	t.Run("Default parallelism", func(t *testing.T) {
		assert.Equal(t, int64(1), params.BackupCfg.BackupCollectionParallelism)
		assert.Equal(t, int64(1024), params.BackupCfg.BackupSegmentParallelism)
		assert.Equal(t, int64(128), params.BackupCfg.BackupCopyDataParallelism)
		assert.Equal(t, 1, params.BackupCfg.RestoreParallelism)
		assert.Equal(t, int64(768), params.BackupCfg.ImportJobParallelism)
	})

	t.Run("Default keep temp files", func(t *testing.T) {
		assert.False(t, params.BackupCfg.KeepTempFiles)
	})

	t.Run("Default GC pause", func(t *testing.T) {
		assert.False(t, params.BackupCfg.GcPauseEnable)
		assert.Equal(t, "http://localhost:9091", params.BackupCfg.GcPauseAddress)
	})
}

// TestStorageTypePriority tests storage type configuration priority
func TestStorageTypePriority(t *testing.T) {
	t.Run("storage.storageType takes priority", func(t *testing.T) {
		tmpDir := t.TempDir()
		configContent := `
milvus:
  address: localhost
  port: "19530"

storage:
  storageType: azure

minio:
  storageType: s3
  cloudProvider: gcp
`
		err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
		require.NoError(t, err)

		params := newBackupParamsWithConfig(t, tmpDir)
		assert.Equal(t, "azure", params.MinioCfg.StorageType)
	})

	t.Run("minio.storageType second priority", func(t *testing.T) {
		tmpDir := t.TempDir()
		configContent := `
milvus:
  address: localhost
  port: "19530"

minio:
  storageType: s3
  cloudProvider: gcp
`
		err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
		require.NoError(t, err)

		params := newBackupParamsWithConfig(t, tmpDir)
		assert.Equal(t, "s3", params.MinioCfg.StorageType)
	})

	t.Run("minio.cloudProvider fallback", func(t *testing.T) {
		tmpDir := t.TempDir()
		configContent := `
milvus:
  address: localhost
  port: "19530"

minio:
  cloudProvider: gcp
`
		err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
		require.NoError(t, err)

		params := newBackupParamsWithConfig(t, tmpDir)
		assert.Equal(t, "gcp", params.MinioCfg.StorageType)
	})
}

// TestSupportedStorageTypes tests that all supported storage types are accepted
func TestSupportedStorageTypes(t *testing.T) {
	storageTypes := []string{
		"local", "minio", "s3", "aws", "gcp", "gcpnative",
		"ali", "aliyun", "alibaba", "alicloud", "azure", "tencent", "tc", "hwc",
	}

	for _, st := range storageTypes {
		t.Run(st, func(t *testing.T) {
			tmpDir := t.TempDir()
			configContent := `
milvus:
  address: localhost
  port: "19530"

minio:
  storageType: ` + st + `
  cloudProvider: ` + st + `
`
			err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
			require.NoError(t, err)

			params := newBackupParamsWithConfig(t, tmpDir)
			assert.Equal(t, st, params.MinioCfg.StorageType)
		})
	}
}

// TestUnsupportedStorageType tests that unsupported storage type causes panic
func TestUnsupportedStorageType(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"

minio:
  storageType: unsupported-type
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	assert.Panics(t, func() {
		newBackupParamsWithConfig(t, tmpDir)
	})
}
