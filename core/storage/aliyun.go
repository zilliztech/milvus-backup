package storage

import (
	"fmt"

	"github.com/alibabacloud-go/tea/tea"
	aliyunCred "github.com/aliyun/credentials-go/credentials"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
)

// newAliyunClient returns a minio.Client which is compatible for aliyun OSS
func newAliyunClient(cfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region, BucketLookup: minio.BucketLookupDNS}
	switch cfg.Credential.Type {
	case IAM:
		provider, err := newAliCredProvider()
		if err != nil {
			return nil, fmt.Errorf("storage: create aliyun credential: %w", err)
		}
		opts.Creds = minioCred.New(provider)
	case Static:
		opts.Creds = minioCred.NewStaticV4(cfg.Credential.AK, cfg.Credential.SK, cfg.Credential.Token)
	case MinioCredProvider:
		opts.Creds = minioCred.New(cfg.Credential.MinioCredProvider)
	default:
		return nil, fmt.Errorf("storage: aliyun unsupported credential type: %s", cfg.Credential.Type.String())
	}

	cli, err := minio.New(cfg.Endpoint, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create aliyun client: %w", err)
	}

	return &MinioClient{cfg: cfg, cli: cli}, nil
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type aliCredProvider struct {
	// aliyunCreds doesn't provide a way to get the expiry time, so we use the cache to check if it's expired
	// when aliyunCreds.GetCredential is different from the cache, we know it's expired
	credCache *aliyunCred.CredentialModel
	cred      aliyunCred.Credential
}

func newAliCredProvider() (minioCred.Provider, error) {
	cred, err := aliyunCred.NewCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("storage: create aliyun credential: %w", err)
	}

	return &aliCredProvider{cred: cred}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (a *aliCredProvider) Retrieve() (minioCred.Value, error) {
	cred, err := a.cred.GetCredential()
	if err != nil {
		return minioCred.Value{}, fmt.Errorf("storage: get credential from aliyun credential: %w", err)
	}

	a.credCache = cred
	return minioCred.Value{
		AccessKeyID:     tea.StringValue(cred.AccessKeyId),
		SecretAccessKey: tea.StringValue(cred.AccessKeySecret),
		SessionToken:    tea.StringValue(cred.SecurityToken),
	}, nil
}

func (a *aliCredProvider) RetrieveWithCredContext(_ *minioCred.CredContext) (minioCred.Value, error) {
	return a.Retrieve()
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (a *aliCredProvider) IsExpired() bool {
	cred, err := a.cred.GetCredential()
	if err != nil {
		return true
	}
	if a.credCache == nil {
		return true
	}

	if tea.StringValue(a.credCache.AccessKeyId) != tea.StringValue(cred.AccessKeyId) ||
		tea.StringValue(a.credCache.AccessKeySecret) != tea.StringValue(cred.AccessKeySecret) ||
		tea.StringValue(a.credCache.SecurityToken) != tea.StringValue(cred.SecurityToken) {
		return true
	}

	return false
}
