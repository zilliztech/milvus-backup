package paramtable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func setupTestConfig(t *testing.T) string {
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

func setupMinimalConfig(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)
	return tmpDir
}

func newBaseTableWithConfig(t *testing.T, configDir string) *BaseTable {
	t.Helper()
	bt := &BaseTable{}
	os.Setenv("MILVUSCONF", configDir)
	t.Cleanup(func() {
		os.Unsetenv("MILVUSCONF")
	})
	bt.Init()
	return bt
}

// TestBaseTable_LoadGetSave tests the basic Load, Get, Save, Remove operations
func TestBaseTable_LoadGetSave(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("Save and Load", func(t *testing.T) {
		err := bt.Save("test.key", "test-value")
		require.NoError(t, err)

		val, err := bt.Load("test.key")
		require.NoError(t, err)
		assert.Equal(t, "test-value", val)
	})

	t.Run("Load non-existent key returns error", func(t *testing.T) {
		_, err := bt.Load("non.existent.key")
		assert.Error(t, err)
	})

	t.Run("Get returns value", func(t *testing.T) {
		err := bt.Save("get.key", "get-value")
		require.NoError(t, err)

		val := bt.Get("get.key")
		assert.Equal(t, "get-value", val)
	})

	t.Run("Get non-existent key returns empty string", func(t *testing.T) {
		val := bt.Get("non.existent.key")
		assert.Equal(t, "", val)
	})

	t.Run("LoadWithDefault returns value when exists", func(t *testing.T) {
		err := bt.Save("default.key", "actual-value")
		require.NoError(t, err)

		val := bt.LoadWithDefault("default.key", "default-value")
		assert.Equal(t, "actual-value", val)
	})

	t.Run("LoadWithDefault returns default when key not exists", func(t *testing.T) {
		val := bt.LoadWithDefault("missing.key", "default-value")
		assert.Equal(t, "default-value", val)
	})

	t.Run("Remove key", func(t *testing.T) {
		err := bt.Save("remove.key", "to-remove")
		require.NoError(t, err)

		err = bt.Remove("remove.key")
		require.NoError(t, err)

		_, err = bt.Load("remove.key")
		assert.Error(t, err)
	})
}

// TestBaseTable_CaseInsensitive tests that key access is case-insensitive
func TestBaseTable_CaseInsensitive(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("Save with uppercase, Load with lowercase", func(t *testing.T) {
		err := bt.Save("UPPER.CASE.KEY", "value1")
		require.NoError(t, err)

		val, err := bt.Load("upper.case.key")
		require.NoError(t, err)
		assert.Equal(t, "value1", val)
	})

	t.Run("Save with lowercase, Load with uppercase", func(t *testing.T) {
		err := bt.Save("lower.case.key", "value2")
		require.NoError(t, err)

		val, err := bt.Load("LOWER.CASE.KEY")
		require.NoError(t, err)
		assert.Equal(t, "value2", val)
	})

	t.Run("Mixed case access", func(t *testing.T) {
		err := bt.Save("Mixed.Case.Key", "value3")
		require.NoError(t, err)

		val1 := bt.Get("MIXED.CASE.KEY")
		val2 := bt.Get("mixed.case.key")
		val3 := bt.LoadWithDefault("MiXeD.cAsE.kEy", "default")

		assert.Equal(t, "value3", val1)
		assert.Equal(t, "value3", val2)
		assert.Equal(t, "value3", val3)
	})
}

// TestBaseTable_ParseBool tests boolean parsing
func TestBaseTable_ParseBool(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("Parse true values", func(t *testing.T) {
		testCases := []string{"true", "True", "TRUE", "1"}
		for _, tc := range testCases {
			bt.Save("bool.key", tc)
			val := bt.ParseBool("bool.key", false)
			assert.True(t, val, "expected true for %q", tc)
		}
	})

	t.Run("Parse false values", func(t *testing.T) {
		testCases := []string{"false", "False", "FALSE", "0"}
		for _, tc := range testCases {
			bt.Save("bool.key", tc)
			val := bt.ParseBool("bool.key", true)
			assert.False(t, val, "expected false for %q", tc)
		}
	})

	t.Run("Use default when key not exists", func(t *testing.T) {
		val := bt.ParseBool("nonexistent.bool", true)
		assert.True(t, val)

		val = bt.ParseBool("nonexistent.bool2", false)
		assert.False(t, val)
	})

	t.Run("Invalid bool panics", func(t *testing.T) {
		bt.Save("invalid.bool", "not-a-bool")
		assert.Panics(t, func() {
			bt.ParseBool("invalid.bool", false)
		})
	})
}

// TestBaseTable_ParseInt tests integer parsing
func TestBaseTable_ParseInt(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("ParseInt success", func(t *testing.T) {
		bt.Save("int.key", "42")
		val := bt.ParseInt("int.key")
		assert.Equal(t, 42, val)
	})

	t.Run("ParseInt negative", func(t *testing.T) {
		bt.Save("int.neg", "-123")
		val := bt.ParseInt("int.neg")
		assert.Equal(t, -123, val)
	})

	t.Run("ParseInt panics on missing key", func(t *testing.T) {
		assert.Panics(t, func() {
			bt.ParseInt("nonexistent.int")
		})
	})

	t.Run("ParseInt panics on invalid value", func(t *testing.T) {
		bt.Save("invalid.int", "not-an-int")
		assert.Panics(t, func() {
			bt.ParseInt("invalid.int")
		})
	})

	t.Run("ParseIntWithDefault returns value when exists", func(t *testing.T) {
		bt.Save("int.default", "100")
		val := bt.ParseIntWithDefault("int.default", 50)
		assert.Equal(t, 100, val)
	})

	t.Run("ParseIntWithDefault returns default when missing", func(t *testing.T) {
		val := bt.ParseIntWithDefault("missing.int", 99)
		assert.Equal(t, 99, val)
	})

	t.Run("ParseIntWithDefault panics on invalid value", func(t *testing.T) {
		bt.Save("invalid.int.default", "xyz")
		assert.Panics(t, func() {
			bt.ParseIntWithDefault("invalid.int.default", 0)
		})
	})
}

// TestBaseTable_ParseInt32 tests int32 parsing
func TestBaseTable_ParseInt32(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("ParseInt32 success", func(t *testing.T) {
		bt.Save("int32.key", "12345")
		val := bt.ParseInt32("int32.key")
		assert.Equal(t, int32(12345), val)
	})

	t.Run("ParseInt32 panics on missing key", func(t *testing.T) {
		assert.Panics(t, func() {
			bt.ParseInt32("nonexistent.int32")
		})
	})

	t.Run("ParseInt32WithDefault returns value when exists", func(t *testing.T) {
		bt.Save("int32.default", "200")
		val := bt.ParseInt32WithDefault("int32.default", 100)
		assert.Equal(t, int32(200), val)
	})

	t.Run("ParseInt32WithDefault returns default when missing", func(t *testing.T) {
		val := bt.ParseInt32WithDefault("missing.int32", 77)
		assert.Equal(t, int32(77), val)
	})
}

// TestBaseTable_ParseInt64 tests int64 parsing
func TestBaseTable_ParseInt64(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("ParseInt64 success", func(t *testing.T) {
		bt.Save("int64.key", "9223372036854775807") // max int64
		val := bt.ParseInt64("int64.key")
		assert.Equal(t, int64(9223372036854775807), val)
	})

	t.Run("ParseInt64 panics on missing key", func(t *testing.T) {
		assert.Panics(t, func() {
			bt.ParseInt64("nonexistent.int64")
		})
	})

	t.Run("ParseInt64WithDefault returns value when exists", func(t *testing.T) {
		bt.Save("int64.default", "1000000")
		val := bt.ParseInt64WithDefault("int64.default", 500000)
		assert.Equal(t, int64(1000000), val)
	})

	t.Run("ParseInt64WithDefault returns default when missing", func(t *testing.T) {
		val := bt.ParseInt64WithDefault("missing.int64", 999)
		assert.Equal(t, int64(999), val)
	})
}

// TestBaseTable_LoadFromKVPair tests loading from key-value pairs
func TestBaseTable_LoadFromKVPair(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("Load multiple pairs", func(t *testing.T) {
		pairs := []*backuppb.KeyValuePair{
			{Key: "kv.key1", Value: "value1"},
			{Key: "kv.key2", Value: "value2"},
			{Key: "kv.key3", Value: "value3"},
		}

		err := bt.LoadFromKVPair(pairs)
		require.NoError(t, err)

		val1, _ := bt.Load("kv.key1")
		val2, _ := bt.Load("kv.key2")
		val3, _ := bt.Load("kv.key3")

		assert.Equal(t, "value1", val1)
		assert.Equal(t, "value2", val2)
		assert.Equal(t, "value3", val3)
	})

	t.Run("Overwrite existing values", func(t *testing.T) {
		bt.Save("existing.key", "old-value")

		pairs := []*backuppb.KeyValuePair{
			{Key: "existing.key", Value: "new-value"},
		}

		err := bt.LoadFromKVPair(pairs)
		require.NoError(t, err)

		val, _ := bt.Load("existing.key")
		assert.Equal(t, "new-value", val)
	})
}

// TestBaseTable_YamlLoading tests that YAML values are correctly loaded
func TestBaseTable_YamlLoading(t *testing.T) {
	configDir := setupTestConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	t.Run("String values", func(t *testing.T) {
		val, err := bt.Load("milvus.address")
		require.NoError(t, err)
		assert.Equal(t, "test-milvus-host", val)
	})

	t.Run("Nested keys with dot notation", func(t *testing.T) {
		val, err := bt.Load("milvus.etcd.endpoints")
		require.NoError(t, err)
		assert.Equal(t, "etcd1:2379,etcd2:2379", val)
	})

	t.Run("Boolean values as strings", func(t *testing.T) {
		val := bt.Get("minio.usessl")
		assert.Equal(t, "true", val)
	})

	t.Run("Numeric values as strings", func(t *testing.T) {
		val := bt.Get("backup.parallelism.copydata")
		assert.Equal(t, "64", val)
	})
}

// TestBaseTable_YamlArrayConversion tests that YAML arrays are converted to comma-separated strings
func TestBaseTable_YamlArrayConversion(t *testing.T) {
	tmpDir := t.TempDir()
	configContent := `
milvus:
  address: localhost
  port: "19530"

test:
  stringArray:
    - item1
    - item2
    - item3
  intArray:
    - 1
    - 2
    - 3
`
	err := os.WriteFile(filepath.Join(tmpDir, "backup.yaml"), []byte(configContent), 0644)
	require.NoError(t, err)

	bt := newBaseTableWithConfig(t, tmpDir)

	t.Run("String array to comma-separated", func(t *testing.T) {
		val, err := bt.Load("test.stringarray")
		require.NoError(t, err)
		assert.Equal(t, "item1,item2,item3", val)
	})

	t.Run("Int array to comma-separated", func(t *testing.T) {
		val, err := bt.Load("test.intarray")
		require.NoError(t, err)
		assert.Equal(t, "1,2,3", val)
	})
}

// TestBaseTable_GetConfigDir tests GetConfigDir returns correct path
func TestBaseTable_GetConfigDir(t *testing.T) {
	configDir := setupMinimalConfig(t)
	bt := newBaseTableWithConfig(t, configDir)

	assert.Equal(t, configDir, bt.GetConfigDir())
}
