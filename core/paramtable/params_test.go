package paramtable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("nonexistent.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read config file")
}

func TestLoad_Success(t *testing.T) {
	tempDir := t.TempDir()
	yamlContent := `
milvus:
  address: test-host
  port: "19530"
  user: testuser
  password: testpass
minio:
  address: minio-host
  port: "9000"
`
	yamlPath := filepath.Join(tempDir, "test.yaml")
	err := os.WriteFile(yamlPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	cfg, err := Load(yamlPath)
	assert.NoError(t, err)
	assert.Equal(t, "test-host", cfg.MilvusCfg.Address)
	assert.Equal(t, "19530", cfg.MilvusCfg.Port)
	assert.Equal(t, "testuser", cfg.MilvusCfg.User)
	assert.Equal(t, "minio-host", cfg.MinioCfg.Address)
}

func TestLoad_EnvOverride(t *testing.T) {
	tempDir := t.TempDir()
	yamlContent := `
milvus:
  address: yaml-host
  port: "19530"
`
	yamlPath := filepath.Join(tempDir, "test.yaml")
	err := os.WriteFile(yamlPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	t.Setenv("MILVUS_ADDRESS", "env-host")

	cfg, err := Load(yamlPath)
	assert.NoError(t, err)
	assert.Equal(t, "env-host", cfg.MilvusCfg.Address)
}

func TestLoad_MissingRequired(t *testing.T) {
	tempDir := t.TempDir()
	yamlContent := `
milvus:
  user: testuser
`
	yamlPath := filepath.Join(tempDir, "test.yaml")
	err := os.WriteFile(yamlPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	_, err = Load(yamlPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "milvus.address")
}

func TestLoad_UnsupportedStorageType(t *testing.T) {
	tempDir := t.TempDir()
	yamlContent := `
milvus:
  address: localhost
  port: "19530"
minio:
  storageType: unsupported
`
	yamlPath := filepath.Join(tempDir, "test.yaml")
	err := os.WriteFile(yamlPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	_, err = Load(yamlPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported storage type")
}

func TestLoad_BackupFallback(t *testing.T) {
	tempDir := t.TempDir()
	yamlContent := `
milvus:
  address: localhost
  port: "19530"
minio:
  address: primary-host
  port: "9000"
  accessKeyID: primary-key
  secretAccessKey: primary-secret
`
	yamlPath := filepath.Join(tempDir, "test.yaml")
	err := os.WriteFile(yamlPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	cfg, err := Load(yamlPath)
	assert.NoError(t, err)

	// Backup config should fallback to primary config
	assert.Equal(t, "primary-host", cfg.MinioCfg.BackupAddress)
	assert.Equal(t, "9000", cfg.MinioCfg.BackupPort)
	assert.Equal(t, "primary-key", cfg.MinioCfg.BackupAccessKeyID)
	assert.Equal(t, "primary-secret", cfg.MinioCfg.BackupSecretAccessKey)
}

func TestLoad_Defaults(t *testing.T) {
	tempDir := t.TempDir()
	yamlContent := `
milvus:
  address: localhost
  port: "19530"
`
	yamlPath := filepath.Join(tempDir, "test.yaml")
	err := os.WriteFile(yamlPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	cfg, err := Load(yamlPath)
	assert.NoError(t, err)

	// Check defaults
	assert.Equal(t, "info", cfg.LogCfg.Level)
	assert.True(t, cfg.LogCfg.Console)
	assert.Equal(t, Minio, cfg.MinioCfg.StorageType)
	assert.Equal(t, "minioadmin", cfg.MinioCfg.AccessKeyID)
	assert.Equal(t, 128, cfg.BackupCfg.Parallelism.CopyData)
}

func TestMaskSensitiveValue(t *testing.T) {
	assert.Equal(t, "***", MaskSensitiveValue(""))
	assert.Equal(t, "***", MaskSensitiveValue("ab"))
	assert.Equal(t, "***", MaskSensitiveValue("12345"))
	assert.Equal(t, "12***56", MaskSensitiveValue("123456"))
	assert.Equal(t, "mi***in", MaskSensitiveValue("minioadmin"))
	assert.Equal(t, "AK***EY", MaskSensitiveValue("AKIAIOSFODNN7EXAMPLEKEY"))
}

func TestBackupParams_MaskedConfig(t *testing.T) {
	cfg := &BackupParams{
		MilvusCfg: MilvusConfig{
			Password: "secretpassword",
		},
		MinioCfg: MinioConfig{
			AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
			SecretAccessKey: "wJalrXUtnFEMI/K7MDENG",
		},
		CloudConfig: CloudConfig{
			APIKey: "sk-1234567890",
		},
	}

	masked := cfg.MaskedConfig()

	assert.Equal(t, "se***rd", masked.MilvusCfg.Password)
	assert.Equal(t, "AK***LE", masked.MinioCfg.AccessKeyID)
	assert.Equal(t, "wJ***NG", masked.MinioCfg.SecretAccessKey)
	assert.Equal(t, "sk***90", masked.CloudConfig.APIKey)

	// Original should be unchanged
	assert.Equal(t, "secretpassword", cfg.MilvusCfg.Password)
}

func TestIsSensitiveEnv(t *testing.T) {
	assert.True(t, isSensitiveEnv("MILVUS_PASSWORD"))
	assert.True(t, isSensitiveEnv("MINIO_SECRET_KEY"))
	assert.True(t, isSensitiveEnv("MINIO_ACCESS_KEY"))
	assert.True(t, isSensitiveEnv("API_TOKEN"))

	assert.False(t, isSensitiveEnv("MILVUS_ADDRESS"))
	assert.False(t, isSensitiveEnv("MINIO_BUCKET_NAME"))
}
