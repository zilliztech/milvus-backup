package storage

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func newBackupCredential(params *paramtable.MinioConfig) Credential {
	var cred Credential
	if params.BackupStorageType == paramtable.CloudProviderAzure {
		cred.AzureAccountName = params.BackupAccessKeyID
	}

	if params.BackupUseIAM {
		cred.Type = IAM
		cred.IAMEndpoint = params.BackupIAMEndpoint
		return cred
	}

	if params.BackupStorageType == paramtable.CloudProviderGCPNative &&
		params.BackupGcpCredentialJSON != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.BackupGcpCredentialJSON
		return cred
	}

	cred.Type = Static
	cred.AK = params.BackupAccessKeyID
	cred.SK = params.BackupSecretAccessKey
	cred.Token = params.BackupToken
	return cred
}

func NewBackupStorage(ctx context.Context, params *paramtable.MinioConfig) (Client, error) {
	ep := net.JoinHostPort(params.BackupAddress, params.BackupPort)
	log.Info("create backup storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.BackupBucketName))

	cred := newBackupCredential(params)
	cfg := Config{
		Provider:   params.BackupStorageType,
		Endpoint:   ep,
		UseSSL:     params.BackupUseSSL,
		Bucket:     params.BackupBucketName,
		Credential: cred,
		Region:     params.BackupRegion,
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

func newMilvusCredential(params *paramtable.MinioConfig) Credential {
	var cred Credential
	if params.StorageType == paramtable.CloudProviderAzure {
		cred.AzureAccountName = params.AccessKeyID
	}

	if params.UseIAM {
		cred.Type = IAM
		cred.IAMEndpoint = params.IAMEndpoint
		return cred
	}

	if params.StorageType == paramtable.CloudProviderGCPNative &&
		params.GcpCredentialJSON != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.GcpCredentialJSON
		return cred
	}

	cred.Type = Static
	cred.AK = params.AccessKeyID
	cred.SK = params.SecretAccessKey
	cred.Token = params.Token
	return cred
}

func NewMilvusStorage(ctx context.Context, params *paramtable.MinioConfig) (Client, error) {
	ep := net.JoinHostPort(params.Address, params.Port)
	log.Info("create milvus storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.BucketName))

	cfg := Config{
		Provider:   params.StorageType,
		Endpoint:   ep,
		UseSSL:     params.UseSSL,
		Credential: newMilvusCredential(params),
		Bucket:     params.BucketName,
		Region:     params.Region,
	}

	return NewClient(ctx, cfg)
}

func NewClient(ctx context.Context, cfg Config) (Client, error) {
	switch cfg.Provider {
	case paramtable.CloudProviderAli, paramtable.CloudProviderAliyun, paramtable.CloudProviderAlibaba, paramtable.CloudProviderAliCloud:
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
