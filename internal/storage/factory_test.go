package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func TestNewBackupCredential(t *testing.T) {
	t.Run("Azure", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.BackupStorageType.Set(cfg.CloudProviderAzure)
		params.BackupAccessKeyID.Set("accountName")
		params.BackupUseIAM.Set(true)
		cred := newBackupCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})

	t.Run("IAM", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.BackupUseIAM.Set(true)
		params.BackupIAMEndpoint.Set("iamEndpoint")
		cred := newBackupCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.BackupStorageType.Set(cfg.CloudProviderGCPNative)
		params.BackupGcpCredentialJSON.Set("path/to/json")
		cred := newBackupCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.BackupAccessKeyID.Set("ak")
		params.BackupSecretAccessKey.Set("sk")
		params.BackupToken.Set("token")

		cred := newBackupCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
		assert.Equal(t, "token", cred.Token)
	})
}

func TestNewMilvusCredential(t *testing.T) {
	t.Run("Azure", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.StorageType.Set(cfg.CloudProviderAzure)
		params.AccessKeyID.Set("accountName")
		params.UseIAM.Set(true)
		cred := newMilvusCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})

	t.Run("IAM", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.UseIAM.Set(true)
		params.IAMEndpoint.Set("iamEndpoint")
		cred := newMilvusCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.StorageType.Set(cfg.CloudProviderGCPNative)
		params.GcpCredentialJSON.Set("path/to/json")
		cred := newMilvusCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &cfg.MinioConfig{}
		params.AccessKeyID.Set("ak")
		params.SecretAccessKey.Set("sk")
		params.Token.Set("token")
		cred := newMilvusCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
		assert.Equal(t, "token", cred.Token)
	})
}
