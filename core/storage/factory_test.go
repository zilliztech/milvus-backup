package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func TestNewBackupCredential(t *testing.T) {
	t.Run("Azure", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			BackupStorageType: paramtable.CloudProviderAzure,
			BackupAccessKeyID: "accountName",
			BackupUseIAM:      true,
		}
		cred := newBackupCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})

	t.Run("IAM", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			BackupUseIAM:      true,
			BackupIAMEndpoint: "iamEndpoint",
		}
		cred := newBackupCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			BackupStorageType:       paramtable.CloudProviderGCPNative,
			BackupGcpCredentialJSON: "path/to/json",
		}
		cred := newBackupCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			BackupAccessKeyID:     "ak",
			BackupSecretAccessKey: "sk",
			BackupToken:           "token",
		}

		cred := newBackupCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
		assert.Equal(t, "token", cred.Token)
	})
}

func TestNewMilvusCredential(t *testing.T) {
	t.Run("Azure", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			StorageType: paramtable.CloudProviderAzure,
			AccessKeyID: "accountName",
			UseIAM:      true,
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})

	t.Run("IAM", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			UseIAM:      true,
			IAMEndpoint: "iamEndpoint",
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			StorageType:       paramtable.CloudProviderGCPNative,
			GcpCredentialJSON: "path/to/json",
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &paramtable.MinioConfig{
			AccessKeyID:     "ak",
			SecretAccessKey: "sk",
			Token:           "token",
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
		assert.Equal(t, "token", cred.Token)
	})
}
