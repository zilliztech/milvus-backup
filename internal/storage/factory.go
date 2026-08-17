package storage

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"go.uber.org/zap"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// newCredential maps a storage section's auth onto the credential the clients
// take. v2 names the authentication method outright, so this is a switch on
// auth.type rather than v1's guesswork over the provider, the useIAM flag and
// whether a GCP credential file happened to be set.
func newCredential(s *v2.StorageConfig) Credential {
	auth := &s.Auth

	// Azure builds its service URL from the account name whichever way it
	// authenticates, so the name is carried on every credential.
	cred := Credential{AzureAccountName: s.AccountName.Val}

	switch auth.Type.Val {
	case v2.AuthSharedKey:
		// Azure signs with the account name and one of its access keys.
		cred.Type = Static
		cred.AK = s.AccountName.Val
		cred.SK = auth.AccountKey.Val
	case v2.AuthServiceAccount:
		cred.Type = GCPCredJSON
		cred.GCPCredJSON = auth.CredentialsFile.Val
	case v2.AuthIAM:
		cred.Type = IAM
		cred.IAMEndpoint = auth.Endpoint.Val
	case v2.AuthDefault:
		// The provider SDK resolves credentials on its own: an instance role,
		// workload identity, or DefaultAzureCredential. That is what the clients
		// already do for IAM when there is no endpoint to fetch from.
		cred.Type = IAM
	default:
		cred.Type = Static
		cred.AK = auth.AccessKeyID.Val
		cred.SK = auth.SecretAccessKey.Val
		cred.Token = auth.SessionToken.Val
	}

	return cred
}

// storageConfig maps one storage section onto a client config.
// multipartCopyThresholdMiB is passed in because it belongs to the transfer
// policy rather than to either backend: it describes how bytes move between
// them.
func storageConfig(s *v2.StorageConfig, multipartCopyThresholdMiB int64) Config {
	return Config{
		Provider:                  s.Provider.Val,
		Endpoint:                  net.JoinHostPort(s.Address.Val, strconv.Itoa(s.Port.Val)),
		UseSSL:                    s.UseSSL.Val,
		Region:                    s.Region.Val,
		MilvusEndpoint:            milvusEndpoint(s),
		Credential:                newCredential(s),
		Bucket:                    s.BucketName.Val,
		MultipartCopyThresholdMiB: multipartCopyThresholdMiB,
	}
}

// milvusEndpoint renders the optional Milvus-view endpoint override. The port
// falls back to the section's own port, so a host-only override stays short.
func milvusEndpoint(s *v2.StorageConfig) string {
	if s.MilvusAddress.Val == "" {
		return ""
	}
	port := s.MilvusPort.Val
	if port == 0 {
		port = s.Port.Val
	}
	return net.JoinHostPort(s.MilvusAddress.Val, strconv.Itoa(port))
}

// MilvusStorageConfig describes the backend the Milvus deployment keeps its
// data in.
func MilvusStorageConfig(c *v2.Config) Config {
	return storageConfig(&c.Milvus.Storage, c.Transfer.MultipartCopyThresholdMiB.Val)
}

// BackupStorageConfig describes the backend backup data is written to.
func BackupStorageConfig(c *v2.Config) Config {
	return storageConfig(&c.Backup.Storage, c.Transfer.MultipartCopyThresholdMiB.Val)
}

func NewMilvusStorage(ctx context.Context, c *v2.Config) (Client, error) {
	conf := MilvusStorageConfig(c)
	log.Info("create milvus storage client",
		zap.String("endpoint", conf.Endpoint),
		zap.String("bucket", conf.Bucket))

	return NewClient(ctx, conf)
}

func NewBackupStorage(ctx context.Context, c *v2.Config) (Client, error) {
	conf := BackupStorageConfig(c)
	log.Info("create backup storage client",
		zap.String("endpoint", conf.Endpoint),
		zap.String("bucket", conf.Bucket))

	cli, err := NewClient(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}
	if err := CreateBucketIfNotExist(ctx, cli, ""); err != nil {
		return nil, fmt.Errorf("create backup storage client: %w", err)
	}

	return cli, nil
}

// UseStreaming reports whether objects have to be streamed through
// milvus-backup rather than copied by the storage service itself.
//
// v1 asked this as the boolean minio.crossStorage, and separately forced
// streaming when the two providers differed. v2 states the policy directly:
// auto keeps that rule, while direct and streaming pin the answer.
func UseStreaming(mode string, src, dest Config) bool {
	switch mode {
	case v2.TransferStreaming:
		return true
	case v2.TransferDirect:
		return false
	default:
		return !SameBackend(src, dest)
	}
}

// SameBackend reports whether two configs name the same backend, i.e. the same
// provider reached at the same endpoint. Buckets and root paths may differ
// within one backend, so they are not compared.
func SameBackend(a, b Config) bool {
	return a.Provider == b.Provider &&
		a.Endpoint == b.Endpoint &&
		a.Region == b.Region &&
		a.UseSSL == b.UseSSL &&
		a.Credential.AzureAccountName == b.Credential.AzureAccountName
}

func NewClient(ctx context.Context, conf Config) (Client, error) {
	// v2 gives each provider exactly one name, so the v1 spelling aliases (ali,
	// alibaba, alicloud, tc) do not reach here: they are folded into the
	// canonical name while the configuration is loaded.
	switch conf.Provider {
	case v2.ProviderAliyun:
		return newAliyunClient(conf)
	case v2.ProviderAWS, v2.ProviderS3, v2.ProviderMinio:
		return newMinioClient(conf)
	case v2.ProviderAzure:
		return newAzureClient(conf)
	case v2.ProviderTencent:
		return newTencentClient(conf)
	case v2.ProviderGCP:
		return newGCPClient(conf)
	case v2.ProviderGCPNative:
		return newGCPNativeClient(ctx, conf)
	case v2.ProviderHwc:
		return NewHwcClient(conf)
	case v2.ProviderLocal:
		return newLocalClient(conf), nil
	default:
		return nil, fmt.Errorf("storage: unsupported storage type: %s", conf.Provider)
	}
}
