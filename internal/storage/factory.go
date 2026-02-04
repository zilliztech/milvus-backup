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
	if params.BackupStorageType.Val == cfg.CloudProviderAzure {
		cred.AzureAccountName = params.BackupAccessKeyID.Val
	}

	if params.BackupUseIAM.Val {
		cred.Type = IAM
		cred.IAMEndpoint = params.BackupIAMEndpoint.Val
		return cred
	}

	if params.BackupStorageType.Val == cfg.CloudProviderGCPNative &&
		params.BackupGcpCredentialJSON.Val != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.BackupGcpCredentialJSON.Val
		return cred
	}

	cred.Type = Static
	cred.AK = params.BackupAccessKeyID.Val
	cred.SK = params.BackupSecretAccessKey.Val
	cred.Token = params.BackupToken.Val
	return cred
}

func BackupStorageConfig(params *cfg.MinioConfig) Config {
	ep := net.JoinHostPort(params.BackupAddress.Val, strconv.Itoa(params.BackupPort.Val))
	return Config{
		Provider:                  params.BackupStorageType.Val,
		Endpoint:                  ep,
		UseSSL:                    params.BackupUseSSL.Val,
		Bucket:                    params.BackupBucketName.Val,
		Credential:                newBackupCredential(params),
		Region:                    params.BackupRegion.Val,
		MultipartCopyThresholdMiB: params.MultipartCopyThresholdMiB.Val,
	}
}

func NewBackupStorage(ctx context.Context, params *cfg.MinioConfig) (Client, error) {
	ep := net.JoinHostPort(params.BackupAddress.Val, strconv.Itoa(params.BackupPort.Val))
	log.Info("create backup storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.BackupBucketName.Val))

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
	if params.StorageType.Val == cfg.CloudProviderAzure {
		cred.AzureAccountName = params.AccessKeyID.Val
	}

	if params.UseIAM.Val {
		cred.Type = IAM
		cred.IAMEndpoint = params.IAMEndpoint.Val
		return cred
	}

	if params.StorageType.Val == cfg.CloudProviderGCPNative &&
		params.GcpCredentialJSON.Val != "" {
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = params.GcpCredentialJSON.Val
		return cred
	}

	cred.Type = Static
	cred.AK = params.AccessKeyID.Val
	cred.SK = params.SecretAccessKey.Val
	cred.Token = params.Token.Val
	return cred
}

func MilvusStorageConfig(params *cfg.MinioConfig) Config {
	ep := net.JoinHostPort(params.Address.Val, strconv.Itoa(params.Port.Val))
	return Config{
		Provider:                  params.StorageType.Val,
		Endpoint:                  ep,
		UseSSL:                    params.UseSSL.Val,
		Credential:                newMilvusCredential(params),
		Bucket:                    params.BucketName.Val,
		Region:                    params.Region.Val,
		MultipartCopyThresholdMiB: params.MultipartCopyThresholdMiB.Val,
	}
}

func NewMilvusStorage(ctx context.Context, params *cfg.MinioConfig) (Client, error) {
	ep := net.JoinHostPort(params.Address.Val, strconv.Itoa(params.Port.Val))
	log.Info("create milvus storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", params.BucketName.Val))

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
