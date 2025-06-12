package storage

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func NewBackupStorage(ctx context.Context, params *paramtable.BackupParams) (Client, error) {
	ep := params.MinioCfg.BackupAddress + ":" + params.MinioCfg.BackupPort
	log.Info("create backup storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.MinioCfg.BackupBucketName))

	cfg := Config{
		Provider:          params.MinioCfg.BackupStorageType,
		Endpoint:          ep,
		Bucket:            params.MinioCfg.BackupBucketName,
		AK:                params.MinioCfg.BackupAccessKeyID,
		SK:                params.MinioCfg.BackupSecretAccessKey,
		GcpCredentialJSON: params.MinioCfg.BackupGcpCredentialJSON,
		UseSSL:            params.MinioCfg.BackupUseSSL,
		UseIAM:            params.MinioCfg.BackupUseIAM,
		IAMEndpoint:       params.MinioCfg.BackupIAMEndpoint,
	}

	cli, err := NewClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}
	if err := CreateBucketIfNotExist(ctx, cli, ""); err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}

	return cli, nil
}

func NewMilvusStorage(ctx context.Context, params *paramtable.BackupParams) (Client, error) {
	ep := params.MinioCfg.Address + ":" + params.MinioCfg.Port
	log.Info("create milvus storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.MinioCfg.BucketName))

	cfg := Config{
		Provider:          params.MinioCfg.StorageType,
		Endpoint:          ep,
		UseSSL:            params.MinioCfg.UseSSL,
		IAMEndpoint:       params.MinioCfg.IAMEndpoint,
		UseIAM:            params.MinioCfg.UseIAM,
		AK:                params.MinioCfg.AccessKeyID,
		SK:                params.MinioCfg.SecretAccessKey,
		GcpCredentialJSON: params.MinioCfg.GcpCredentialJSON,
		Bucket:            params.MinioCfg.BucketName,
	}

	return NewClient(ctx, cfg)
}

func NewClient(ctx context.Context, cfg Config) (Client, error) {
	switch cfg.Provider {
	case paramtable.CloudProviderAli, paramtable.CloudProviderAliyun:
		return newAliyunClient(cfg)
	case paramtable.CloudProviderAWS, paramtable.S3, paramtable.Minio:
		return newMinioClient(cfg)
	case paramtable.CloudProviderAzure:
		return newAzureClient(cfg)
	case paramtable.CloudProviderTencent, paramtable.CloudProviderTencentShort:
		return newTencentClient(cfg)
	case paramtable.CloudProviderGCP:
		return newGCPClient(cfg)
	case paramtable.CloudProviderGCPNative:
		return newGCPNativeClient(ctx, cfg)
	case paramtable.Local:
		return newLocalClient(cfg), nil
	default:
		return nil, fmt.Errorf("storage: unsupported storage type: %s", cfg.Provider)
	}
}
