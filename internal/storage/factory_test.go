package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

func TestMilvusEndpoint(t *testing.T) {
	t.Run("Unset", func(t *testing.T) {
		s := &v2.StorageConfig{}
		assert.Empty(t, milvusEndpoint(s))
	})

	t.Run("PortFallsBackToTheSectionPort", func(t *testing.T) {
		s := &v2.StorageConfig{
			Port:          param.Value[int]{Val: 9000},
			MilvusAddress: param.Value[string]{Val: "milvus-minio"},
		}
		assert.Equal(t, "milvus-minio:9000", milvusEndpoint(s))
	})

	t.Run("ExplicitPort", func(t *testing.T) {
		s := &v2.StorageConfig{
			Port:          param.Value[int]{Val: 9000},
			MilvusAddress: param.Value[string]{Val: "milvus-minio"},
			MilvusPort:    param.Value[int]{Val: 9001},
		}
		assert.Equal(t, "milvus-minio:9001", milvusEndpoint(s))
	})
}

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

// storageConfig is the single point where a v2 storage section becomes the
// config the clients run on: every usecase's params funnel through it, so
// this mapping is what makes a forked override actually reach a client.
func TestStorageConfigMapsSection(t *testing.T) {
	s := &v2.StorageConfig{
		Provider:   param.Value[string]{Val: v2.ProviderMinio},
		Address:    param.Value[string]{Val: "minio"},
		Port:       param.Value[int]{Val: 9000},
		UseSSL:     param.Value[bool]{Val: true},
		Region:     param.Value[string]{Val: "us-east-1"},
		BucketName: param.Value[string]{Val: "bucket"},
		Auth: v2.StorageAuthConfig{
			Type:            param.Value[string]{Val: v2.AuthStatic},
			AccessKeyID:     param.Value[string]{Val: "ak"},
			SecretAccessKey: param.Value[string]{Val: "sk"},
		},
	}

	got := storageConfig(s, 64)

	assert.Equal(t, v2.ProviderMinio, got.Provider)
	assert.Equal(t, "minio:9000", got.Endpoint)
	assert.True(t, got.UseSSL)
	assert.Equal(t, "us-east-1", got.Region)
	assert.Equal(t, "bucket", got.Bucket)
	assert.Equal(t, "ak", got.Credential.AK)
	assert.Equal(t, "sk", got.Credential.SK)
	assert.Empty(t, got.MilvusEndpoint)
	assert.Equal(t, int64(64), got.MultipartCopyThresholdMiB)
}

// The wrappers pick their section off the whole config. The restore, create
// and get_backup handlers fork backup.storage.* keys, so the backup client
// must read the backup section, not the milvus one.
func TestBackupAndMilvusStorageConfigReadTheirSections(t *testing.T) {
	c := &v2.Config{
		Milvus: v2.MilvusConfig{Storage: v2.StorageConfig{
			BucketName: param.Value[string]{Val: "milvus-bucket"},
			Address:    param.Value[string]{Val: "milvus-minio"},
			Port:       param.Value[int]{Val: 9000},
		}},
		Backup: v2.BackupConfig{Storage: v2.StorageConfig{
			BucketName: param.Value[string]{Val: "backup-bucket"},
			Address:    param.Value[string]{Val: "backup-minio"},
			Port:       param.Value[int]{Val: 9001},
		}},
	}

	backup := BackupStorageConfig(c)
	assert.Equal(t, "backup-bucket", backup.Bucket)
	assert.Equal(t, "backup-minio:9001", backup.Endpoint)

	milvus := MilvusStorageConfig(c)
	assert.Equal(t, "milvus-bucket", milvus.Bucket)
	assert.Equal(t, "milvus-minio:9000", milvus.Endpoint)
}
