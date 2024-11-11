package storage

import (
	"context"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

// ChunkManager is to manager chunks.
type ChunkManager interface {
	Config() *StorageConfig

	// Write writes @content to @filePath.
	Write(ctx context.Context, bucketName string, filePath string, content []byte) error
	// Exist returns true if @filePath exists.
	Exist(ctx context.Context, bucketName string, filePath string) (bool, error)
	// Read reads @filePath and returns content.
	Read(ctx context.Context, bucketName string, filePath string) ([]byte, error)
	// ListWithPrefix list all objects with same @prefix
	ListWithPrefix(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []int64, error)
	// Remove delete @filePath.
	Remove(ctx context.Context, bucketName string, filePath string) error
	// RemoveWithPrefix remove files with same @prefix.
	RemoveWithPrefix(ctx context.Context, bucketName string, prefix string) error
	// Copy files from fromPath into toPath recursively
	Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error

	// ListObjectsPage paginate list of all objects
	ListObjectsPage(ctx context.Context, bucket, prefix string) (ListObjectsPaginator, error)
	// HeadObject determine if an object exists, and you have permission to access it.
	HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error)
	// GetObject get an object
	GetObject(ctx context.Context, bucket, key string) (*Object, error)
	// UploadObject stream upload an object
	UploadObject(ctx context.Context, i UploadObjectInput) error
}

func NewChunkManager(ctx context.Context, params paramtable.BackupParams, config *StorageConfig) (ChunkManager, error) {
	switch config.StorageType {
	case paramtable.Local:
		return NewLocalChunkManager(ctx, config)
	case paramtable.CloudProviderAzure:
		// todo @wayblink
		return newAzureChunkManagerWithParams(ctx, params, config)
	case paramtable.CloudProviderGCPNative:
		return newGCPNativeChunkManagerWithConfig(ctx, config)
	default:
		return NewMinioChunkManagerWithConfig(ctx, config)
	}
}

func newAzureChunkManagerWithParams(ctx context.Context, params paramtable.BackupParams, config *StorageConfig) (*AzureChunkManager, error) {
	c := newDefaultConfig()
	c.Address = params.MinioCfg.Address + ":" + params.MinioCfg.Port
	c.AccessKeyID = params.MinioCfg.AccessKeyID
	c.SecretAccessKeyID = params.MinioCfg.SecretAccessKey
	c.UseSSL = params.MinioCfg.UseSSL
	c.BucketName = params.MinioCfg.BucketName
	c.RootPath = params.MinioCfg.RootPath
	c.StorageType = params.MinioCfg.StorageType
	c.UseIAM = params.MinioCfg.UseIAM
	c.IAMEndpoint = params.MinioCfg.IAMEndpoint
	c.CreateBucket = true

	c.backupAccessKeyID = params.MinioCfg.BackupAccessKeyID
	c.backupSecretAccessKeyID = params.MinioCfg.BackupSecretAccessKey
	c.backupBucketName = params.MinioCfg.BackupBucketName
	c.backupRootPath = params.MinioCfg.BackupRootPath

	return NewAzureChunkManager(ctx, c)
}
