package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func TestNewCredential(t *testing.T) {
	t.Run("Azure", func(t *testing.T) {
		params := &cfg.StorageConfig{
			Provider:    cfg.Value[string]{Val: cfg.CloudProviderAzure},
			AccessKeyID: cfg.Value[string]{Val: "accountName"},
			UseIAM:      cfg.Value[bool]{Val: true},
		}
		cred := newCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})

	t.Run("IAM", func(t *testing.T) {
		params := &cfg.StorageConfig{
			UseIAM:      cfg.Value[bool]{Val: true},
			IAMEndpoint: cfg.Value[string]{Val: "iamEndpoint"},
		}
		cred := newCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &cfg.StorageConfig{
			Provider:          cfg.Value[string]{Val: cfg.CloudProviderGCPNative},
			GcpCredentialJSON: cfg.Value[string]{Val: "path/to/json"},
		}
		cred := newCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &cfg.StorageConfig{
			AccessKeyID:     cfg.Value[string]{Val: "ak"},
			SecretAccessKey: cfg.Value[string]{Val: "sk"},
			Token:           cfg.Value[string]{Val: "token"},
		}
		cred := newCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
		assert.Equal(t, "token", cred.Token)
	})
}
