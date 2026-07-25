package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

func TestNewCredential(t *testing.T) {
	t.Run("Static", func(t *testing.T) {
		cred := newCredential(&v2.StorageConfig{Auth: v2.StorageAuthConfig{
			Type:            param.Value[string]{Val: v2.AuthStatic},
			AccessKeyID:     param.Value[string]{Val: "ak"},
			SecretAccessKey: param.Value[string]{Val: "sk"},
			SessionToken:    param.Value[string]{Val: "token"},
		}})

		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
		assert.Equal(t, "token", cred.Token)
	})

	// Azure signs with the account name and one of its access keys.
	t.Run("SharedKey", func(t *testing.T) {
		cred := newCredential(&v2.StorageConfig{
			AccountName: param.Value[string]{Val: "accountName"},
			Auth: v2.StorageAuthConfig{
				Type:       param.Value[string]{Val: v2.AuthSharedKey},
				AccountKey: param.Value[string]{Val: "accountKey"},
			},
		})

		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "accountName", cred.AzureAccountName)
		assert.Equal(t, "accountName", cred.AK)
		assert.Equal(t, "accountKey", cred.SK)
	})

	t.Run("ServiceAccount", func(t *testing.T) {
		cred := newCredential(&v2.StorageConfig{Auth: v2.StorageAuthConfig{
			Type:            param.Value[string]{Val: v2.AuthServiceAccount},
			CredentialsFile: param.Value[string]{Val: "path/to/json"},
		}})

		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("IAM", func(t *testing.T) {
		cred := newCredential(&v2.StorageConfig{Auth: v2.StorageAuthConfig{
			Type:     param.Value[string]{Val: v2.AuthIAM},
			Endpoint: param.Value[string]{Val: "iamEndpoint"},
		}})

		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	// The SDK resolves credentials on its own, which is what the clients do for
	// IAM when there is no endpoint to fetch them from.
	t.Run("Default", func(t *testing.T) {
		cred := newCredential(&v2.StorageConfig{
			AccountName: param.Value[string]{Val: "accountName"},
			Auth: v2.StorageAuthConfig{
				Type: param.Value[string]{Val: v2.AuthDefault},
			},
		})

		assert.Equal(t, IAM, cred.Type)
		assert.Empty(t, cred.IAMEndpoint)
		assert.Equal(t, "accountName", cred.AzureAccountName)
	})
}

func TestUseStreaming(t *testing.T) {
	minio := Config{Provider: v2.ProviderMinio, Endpoint: "localhost:9000"}
	s3 := Config{Provider: v2.ProviderS3, Endpoint: "s3.amazonaws.com:443"}

	t.Run("Streaming", func(t *testing.T) {
		assert.True(t, UseStreaming(v2.TransferStreaming, minio, minio))
		assert.True(t, UseStreaming(v2.TransferStreaming, minio, s3))
	})

	t.Run("Direct", func(t *testing.T) {
		assert.False(t, UseStreaming(v2.TransferDirect, minio, minio))
		assert.False(t, UseStreaming(v2.TransferDirect, minio, s3))
	})

	t.Run("AutoSameBackend", func(t *testing.T) {
		assert.False(t, UseStreaming(v2.TransferAuto, minio, minio))
	})

	t.Run("AutoDifferentBackend", func(t *testing.T) {
		assert.True(t, UseStreaming(v2.TransferAuto, minio, s3))
	})

	// v1 only compared the provider, so two MinIO deployments looked like one
	// backend and were copied server-side, which cannot work.
	t.Run("AutoSameProviderDifferentEndpoint", func(t *testing.T) {
		other := Config{Provider: v2.ProviderMinio, Endpoint: "elsewhere:9000"}
		assert.True(t, UseStreaming(v2.TransferAuto, minio, other))
	})
}
