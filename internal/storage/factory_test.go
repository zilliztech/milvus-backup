package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func TestNewBackupCredential(t *testing.T) {
	t.Run("Azure", func(t *testing.T) {
		params := &cfg.MinioConfig{
			BackupStorageType: cfg.Value[string]{Val: cfg.CloudProviderAzure},
			BackupAccessKeyID: cfg.Value[string]{Val: "accountName"},
			BackupUseIAM:      cfg.Value[bool]{Val: true},
		}
		cred := newBackupCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})

	t.Run("IAM", func(t *testing.T) {
		params := &cfg.MinioConfig{
			BackupUseIAM:      cfg.Value[bool]{Val: true},
			BackupIAMEndpoint: cfg.Value[string]{Val: "iamEndpoint"},
		}
		cred := newBackupCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &cfg.MinioConfig{
			BackupStorageType:       cfg.Value[string]{Val: cfg.CloudProviderGCPNative},
			BackupGcpCredentialJSON: cfg.Value[string]{Val: "path/to/json"},
		}
		cred := newBackupCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &cfg.MinioConfig{
			BackupAccessKeyID:     cfg.Value[string]{Val: "ak"},
			BackupSecretAccessKey: cfg.Value[string]{Val: "sk"},
			BackupToken:           cfg.Value[string]{Val: "token"},
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
		params := &cfg.MinioConfig{
			StorageType: cfg.Value[string]{Val: cfg.CloudProviderAzure},
			AccessKeyID: cfg.Value[string]{Val: "accountName"},
			UseIAM:      cfg.Value[bool]{Val: true},
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})

	t.Run("IAM", func(t *testing.T) {
		params := &cfg.MinioConfig{
			UseIAM:      cfg.Value[bool]{Val: true},
			IAMEndpoint: cfg.Value[string]{Val: "iamEndpoint"},
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &cfg.MinioConfig{
			StorageType:       cfg.Value[string]{Val: cfg.CloudProviderGCPNative},
			GcpCredentialJSON: cfg.Value[string]{Val: "path/to/json"},
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &cfg.MinioConfig{
			AccessKeyID:     cfg.Value[string]{Val: "ak"},
			SecretAccessKey: cfg.Value[string]{Val: "sk"},
			Token:           cfg.Value[string]{Val: "token"},
		}
		cred := newMilvusCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
		assert.Equal(t, "token", cred.Token)
	})
}
