package storage

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func newBackupCredential(params *paramtable.BackupParams) Credential {
	if params.MinioCfg.BackupUseIAM {
		return Credential{Type: IAM, IAMEndpoint: params.MinioCfg.BackupIAMEndpoint}
	}

	if params.MinioCfg.BackupStorageType == paramtable.CloudProviderGCPNative &&
		params.MinioCfg.BackupGcpCredentialJSON != "" {
		return Credential{Type: GCPCredJSON, GCPCredJSON: params.MinioCfg.BackupGcpCredentialJSON}
	}

	return Credential{
		Type:  Static,
		AK:    params.MinioCfg.BackupAccessKeyID,
		SK:    params.MinioCfg.BackupSecretAccessKey,
		Token: params.MinioCfg.BackupToken,
	}
}

func NewBackupStorage(ctx context.Context, params *paramtable.BackupParams) (Client, error) {
	ep := net.JoinHostPort(params.MinioCfg.BackupAddress, params.MinioCfg.BackupPort)
	log.Info("create backup storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.MinioCfg.BackupBucketName))

	cred := newBackupCredential(params)
	cfg := Config{
		Provider:   params.MinioCfg.BackupStorageType,
		Endpoint:   ep,
		UseSSL:     params.MinioCfg.BackupUseSSL,
		Bucket:     params.MinioCfg.BackupBucketName,
		Credential: cred,
		Region:     params.MinioCfg.BackupRegion,
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

func newMilvusCredential(params *paramtable.BackupParams) Credential {
	if params.MinioCfg.UseIAM {
		return Credential{Type: IAM, IAMEndpoint: params.MinioCfg.IAMEndpoint}
	}

	if params.MinioCfg.StorageType == paramtable.CloudProviderGCPNative &&
		params.MinioCfg.GcpCredentialJSON != "" {
		return Credential{Type: GCPCredJSON, GCPCredJSON: params.MinioCfg.GcpCredentialJSON}
	}

	return Credential{
		Type:  Static,
		AK:    params.MinioCfg.AccessKeyID,
		SK:    params.MinioCfg.SecretAccessKey,
		Token: params.MinioCfg.Token,
	}
}

func NewMilvusStorage(ctx context.Context, params *paramtable.BackupParams) (Client, error) {
	ep := net.JoinHostPort(params.MinioCfg.Address, params.MinioCfg.Port)
	log.Info("create milvus storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.MinioCfg.BucketName))

	cfg := Config{
		Provider:   params.MinioCfg.StorageType,
		Endpoint:   ep,
		UseSSL:     params.MinioCfg.UseSSL,
		Credential: newMilvusCredential(params),
		Bucket:     params.MinioCfg.BucketName,
		Region:     params.MinioCfg.Region,
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
