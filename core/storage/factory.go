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
	var cred Credential
	if params.MinioCfg.BackupStorageType == paramtable.CloudProviderAzure {
		cred.AzureAccountName = params.MinioCfg.BackupAccessKeyID
	}

	if params.MinioCfg.BackupUseIAM {
		cred.Type = IAM
		cred.IAMEndpoint = params.MinioCfg.BackupIAMEndpoint
		return cred
	}

	if params.MinioCfg.BackupStorageType == paramtable.CloudProviderGCPNative &&
		params.MinioCfg.BackupGcpCredentialJSON != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.MinioCfg.BackupGcpCredentialJSON
		return cred
	}

	cred.Type = Static
	cred.AK = params.MinioCfg.BackupAccessKeyID
	cred.SK = params.MinioCfg.BackupSecretAccessKey
	cred.Token = params.MinioCfg.BackupToken
	return cred
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
	var cred Credential
	if params.MinioCfg.StorageType == paramtable.CloudProviderAzure {
		cred.AzureAccountName = params.MinioCfg.AccessKeyID
	}

	if params.MinioCfg.UseIAM {
		cred.Type = IAM
		cred.IAMEndpoint = params.MinioCfg.IAMEndpoint
		return cred
	}

	if params.MinioCfg.StorageType == paramtable.CloudProviderGCPNative &&
		params.MinioCfg.GcpCredentialJSON != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.MinioCfg.GcpCredentialJSON
		return cred
	}

	cred.Type = Static
	cred.AK = params.MinioCfg.AccessKeyID
	cred.SK = params.MinioCfg.SecretAccessKey
	cred.Token = params.MinioCfg.Token
	return cred
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
	case paramtable.CloudProviderAli, paramtable.CloudProviderAliyun, paramtable.CloudProviderAlibaba:
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
