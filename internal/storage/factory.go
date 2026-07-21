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

func newCredential(params *cfg.StorageConfig) Credential {
	var cred Credential
	if params.Provider.Val == cfg.CloudProviderAzure {
		cred.AzureAccountName = params.AccessKeyID.Val
	}

	if params.UseIAM.Val {
		cred.Type = IAM
		cred.IAMEndpoint = params.IAMEndpoint.Val
		return cred
	}

	if params.Provider.Val == cfg.CloudProviderGCPNative &&
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

func BuildConfig(params *cfg.StorageConfig, multipartCopyThresholdMiB int64) Config {
	ep := net.JoinHostPort(params.Address.Val, strconv.Itoa(params.Port.Val))
	return Config{
		Provider:                  params.Provider.Val,
		Endpoint:                  ep,
		UseSSL:                    params.UseSSL.Val,
		Bucket:                    params.BucketName.Val,
		Credential:                newCredential(params),
		Region:                    params.Region.Val,
		MultipartCopyThresholdMiB: multipartCopyThresholdMiB,
	}
}

func NewBackupStorage(ctx context.Context, params *cfg.Config) (Client, error) {
	storageParams := &params.Backup.Storage
	ep := net.JoinHostPort(storageParams.Address.Val, strconv.Itoa(storageParams.Port.Val))
	log.Info("create backup storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", storageParams.BucketName.Val))

	conf := BuildConfig(storageParams, params.Transfer.MultipartCopyThresholdMiB.Val)

	cli, err := NewClient(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}
	if err := CreateBucketIfNotExist(ctx, cli, ""); err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}

	return cli, nil
}

func NewMilvusStorage(ctx context.Context, params *cfg.Config) (Client, error) {
	storageParams := &params.Milvus.Storage
	ep := net.JoinHostPort(storageParams.Address.Val, strconv.Itoa(storageParams.Port.Val))
	log.Info("create milvus storage client",
		zap.String("endpoint", ep),
		zap.String("bucket", storageParams.BucketName.Val))

	conf := BuildConfig(storageParams, params.Transfer.MultipartCopyThresholdMiB.Val)

	return NewClient(ctx, conf)
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
