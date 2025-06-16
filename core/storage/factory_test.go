package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func TestNewBackupCredential(t *testing.T) {
	t.Run("IAM", func(t *testing.T) {
		params := &paramtable.BackupParams{MinioCfg: paramtable.MinioConfig{
			BackupUseIAM:      true,
			BackupIAMEndpoint: "iamEndpoint",
		}}
		cred := newBackupCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &paramtable.BackupParams{MinioCfg: paramtable.MinioConfig{
			BackupStorageType:       paramtable.CloudProviderGCPNative,
			BackupGcpCredentialJSON: "path/to/json",
		}}
		cred := newBackupCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &paramtable.BackupParams{MinioCfg: paramtable.MinioConfig{
			BackupAccessKeyID:     "ak",
			BackupSecretAccessKey: "sk",
		}}
		cred := newBackupCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
	})
}

func TestNewMilvusCredential(t *testing.T) {
	t.Run("IAM", func(t *testing.T) {
		params := &paramtable.BackupParams{MinioCfg: paramtable.MinioConfig{
			UseIAM:      true,
			IAMEndpoint: "iamEndpoint",
		}}
		cred := newMilvusCredential(params)
		assert.Equal(t, IAM, cred.Type)
		assert.Equal(t, "iamEndpoint", cred.IAMEndpoint)
	})

	t.Run("GCPCredJSON", func(t *testing.T) {
		params := &paramtable.BackupParams{MinioCfg: paramtable.MinioConfig{
			StorageType:       paramtable.CloudProviderGCPNative,
			GcpCredentialJSON: "path/to/json",
		}}
		cred := newMilvusCredential(params)
		assert.Equal(t, GCPCredJSON, cred.Type)
		assert.Equal(t, "path/to/json", cred.GCPCredJSON)
	})

	t.Run("Static", func(t *testing.T) {
		params := &paramtable.BackupParams{MinioCfg: paramtable.MinioConfig{
			AccessKeyID:     "ak",
			SecretAccessKey: "sk",
		}}
		cred := newMilvusCredential(params)
		assert.Equal(t, Static, cred.Type)
		assert.Equal(t, "ak", cred.AK)
		assert.Equal(t, "sk", cred.SK)
	})
}
