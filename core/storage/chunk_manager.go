package storage

import (
	"context"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func NewChunkManager(ctx context.Context, params paramtable.BackupParams) (ChunkManager, error) {
	engine := params.MinioCfg.StorageType
	switch engine {
	case paramtable.Local:
		return newLocalChunkManagerWithParams(ctx, params)
	case paramtable.CloudProviderAzure:
		return newAzureChunkManagerWithParams(ctx, params)
	default:
		return newMinioChunkManagerWithParams(ctx, params)
	}
}

func newMinioChunkManagerWithParams(ctx context.Context, params paramtable.BackupParams) (*MinioChunkManager, error) {
	c := newDefaultConfig()
	c.address = params.MinioCfg.Address + ":" + params.MinioCfg.Port
	c.accessKeyID = params.MinioCfg.AccessKeyID
	c.secretAccessKeyID = params.MinioCfg.SecretAccessKey
	c.useSSL = params.MinioCfg.UseSSL
	c.bucketName = params.MinioCfg.BackupBucketName
	c.rootPath = params.MinioCfg.RootPath
	//c.cloudProvider = params.MinioCfg.CloudProvider
	c.storageType = params.MinioCfg.StorageType
	c.useIAM = params.MinioCfg.UseIAM
	c.iamEndpoint = params.MinioCfg.IAMEndpoint
	c.createBucket = true
	return newMinioChunkManagerWithConfig(ctx, c)
}

func newAzureChunkManagerWithParams(ctx context.Context, params paramtable.BackupParams) (*AzureChunkManager, error) {
	c := newDefaultConfig()
	c.address = params.MinioCfg.Address + ":" + params.MinioCfg.Port
	c.accessKeyID = params.MinioCfg.AccessKeyID
	c.secretAccessKeyID = params.MinioCfg.SecretAccessKey
	c.useSSL = params.MinioCfg.UseSSL
	c.bucketName = params.MinioCfg.BucketName
	c.rootPath = params.MinioCfg.RootPath
	//c.cloudProvider = params.MinioCfg.CloudProvider
	c.storageType = params.MinioCfg.StorageType
	c.useIAM = params.MinioCfg.UseIAM
	c.iamEndpoint = params.MinioCfg.IAMEndpoint
	c.createBucket = true

	c.backupAccessKeyID = params.MinioCfg.BackupAccessKeyID
	c.backupSecretAccessKeyID = params.MinioCfg.BackupSecretAccessKey
	c.backupBucketName = params.MinioCfg.BackupBucketName
	c.backupRootPath = params.MinioCfg.BackupRootPath

	return NewAzureChunkManager(ctx, c)
}

func newLocalChunkManagerWithParams(ctx context.Context, params paramtable.BackupParams) (*LocalChunkManager, error) {
	c := newDefaultConfig()
	c.rootPath = params.MinioCfg.RootPath
	//c.cloudProvider = params.MinioCfg.CloudProvider
	c.storageType = params.MinioCfg.StorageType
	c.backupRootPath = params.MinioCfg.BackupRootPath

	return NewLocalChunkManager(ctx, c)
}
