package storage

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func newBackupCredential(params *cfg.MinioConfig) Credential {
	var cred Credential
	if params.BackupStorageType.Value() == cfg.CloudProviderAzure {
		cred.AzureAccountName = params.BackupAccessKeyID.Value()
	}

	if params.BackupUseIAM.Value() {
		cred.Type = IAM
		cred.IAMEndpoint = params.BackupIAMEndpoint.Value()
		return cred
	}

	if params.BackupStorageType.Value() == cfg.CloudProviderGCPNative &&
		params.BackupGcpCredentialJSON.Value() != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.BackupGcpCredentialJSON.Value()
		return cred
	}

	cred.Type = Static
	cred.AK = params.BackupAccessKeyID.Value()
	cred.SK = params.BackupSecretAccessKey.Value()
	cred.Token = params.BackupToken.Value()
	return cred
}

func BackupStorageConfig(params *cfg.MinioConfig) Config {
	ep := net.JoinHostPort(params.BackupAddress.Value(), strconv.Itoa(params.BackupPort.Value()))
	return Config{
		Provider:   params.BackupStorageType.Value(),
		Endpoint:   ep,
		UseSSL:     params.BackupUseSSL.Value(),
		Bucket:     params.BackupBucketName.Value(),
		Credential: newBackupCredential(params),
		Region:     params.BackupRegion.Value(),
	}
}

func NewBackupStorage(ctx context.Context, params *cfg.MinioConfig) (Client, error) {
	ep := net.JoinHostPort(params.BackupAddress.Value(), strconv.Itoa(params.BackupPort.Value()))
	log.Info("create backup storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.BackupBucketName.Value()))

	cfg := BackupStorageConfig(params)

	cli, err := NewClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}
	if err := CreateBucketIfNotExist(ctx, cli, ""); err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}

	return cli, nil
}

func newMilvusCredential(params *cfg.MinioConfig) Credential {
	var cred Credential
	if params.StorageType.Value() == cfg.CloudProviderAzure {
		cred.AzureAccountName = params.AccessKeyID.Value()
	}

	if params.UseIAM.Value() {
		cred.Type = IAM
		cred.IAMEndpoint = params.IAMEndpoint.Value()
		return cred
	}

	if params.StorageType.Value() == cfg.CloudProviderGCPNative &&
		params.GcpCredentialJSON.Value() != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.GcpCredentialJSON.Value()
		return cred
	}

	cred.Type = Static
	cred.AK = params.AccessKeyID.Value()
	cred.SK = params.SecretAccessKey.Value()
	cred.Token = params.Token.Value()
	return cred
}

func MilvusStorageConfig(params *cfg.MinioConfig) Config {
	ep := net.JoinHostPort(params.Address.Value(), strconv.Itoa(params.Port.Value()))
	return Config{
		Provider:   params.StorageType.Value(),
		Endpoint:   ep,
		UseSSL:     params.UseSSL.Value(),
		Credential: newMilvusCredential(params),
		Bucket:     params.BucketName.Value(),
		Region:     params.Region.Value(),
	}
}

func NewMilvusStorage(ctx context.Context, params *cfg.MinioConfig) (Client, error) {
	ep := net.JoinHostPort(params.Address.Value(), strconv.Itoa(params.Port.Value()))
	log.Info("create milvus storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.BucketName.Value()))

	cfg := MilvusStorageConfig(params)

	return NewClient(ctx, cfg)
}

func NewClient(ctx context.Context, conf Config) (Client, error) {
	switch conf.Provider {
	case cfg.CloudProviderAli, cfg.CloudProviderAliyun, cfg.CloudProviderAlibaba, cfg.CloudProviderAliCloud:
		return newAliyunClient(conf)
	case cfg.CloudProviderAWS, cfg.S3, cfg.Minio:
		return newMinioClient(conf)
	case cfg.CloudProviderAzure:
		return newAzureClient(conf)
	case cfg.CloudProviderTencent, cfg.CloudProviderTencentShort:
		return newTencentClient(conf)
	case cfg.CloudProviderGCP:
		return newGCPClient(conf)
	case cfg.CloudProviderGCPNative:
		return newGCPNativeClient(ctx, conf)
	case cfg.CloudProviderHwc:
		return NewHwcClient(conf)
	case cfg.Local:
		return newLocalClient(conf), nil
	default:
		return nil, fmt.Errorf("storage: unsupported storage type: %s", conf.Provider)
	}
}
