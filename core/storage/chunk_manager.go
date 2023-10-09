package storage

import (
	"context"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func NewChunkManager(ctx context.Context, params paramtable.BackupParams) (ChunkManager, error) {
	engine := params.MinioCfg.CloudProvider
	if engine == "azure" {
		return newAzureChunkManagerWithParams(ctx, params)
	} else {
		return newMinioChunkManagerWithParams(ctx, params)
	}
	//switch engine {
	//case "local":
	//	return newMinioChunkManagerWithParams(ctx, params)
	//	//return NewLocalChunkManager(RootPath(f.config.rootPath)), nil
	//case "minio":
	//case "s3":
	//case "gcp":
	//case "aliyun":
	//	return newMinioChunkManagerWithParams(ctx, params)
	//case "azure":
	//	return newAzureChunkManagerWithParams(ctx, params)
	//default:
	//	return nil, errors.New("no chunk manager implemented with engine: " + engine)
	//}
}

func newMinioChunkManagerWithParams(ctx context.Context, params paramtable.BackupParams) (*MinioChunkManager, error) {
	c := newDefaultConfig()
	c.address = params.MinioCfg.Address + ":" + params.MinioCfg.Port
	c.accessKeyID = params.MinioCfg.AccessKeyID
	c.secretAccessKeyID = params.MinioCfg.SecretAccessKey
	c.useSSL = params.MinioCfg.UseSSL
	c.bucketName = params.MinioCfg.BackupBucketName
	c.rootPath = params.MinioCfg.RootPath
	c.cloudProvider = params.MinioCfg.CloudProvider
	c.storageEngine = params.MinioCfg.StorageType
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
	c.bucketName = params.MinioCfg.BackupBucketName
	c.rootPath = params.MinioCfg.RootPath
	c.cloudProvider = params.MinioCfg.CloudProvider
	c.storageEngine = params.MinioCfg.StorageType
	c.useIAM = params.MinioCfg.UseIAM
	c.iamEndpoint = params.MinioCfg.IAMEndpoint
	c.createBucket = true
	return NewAzureChunkManager(ctx, c)
}
