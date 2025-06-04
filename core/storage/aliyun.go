package storage

import (
	"fmt"

	"github.com/alibabacloud-go/tea/tea"
	aliyunCred "github.com/aliyun/credentials-go/credentials"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
)

// NewAliyunClient returns a minio.Client which is compatible for aliyun OSS
func NewAliyunClient(cfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, BucketLookup: minio.BucketLookupDNS}
	if cfg.UseIAM {
		provider, err := newAliCredProvider()
		if err != nil {
			return nil, fmt.Errorf("storage: create aliyun credential: %w", err)
		}
		opts.Creds = minioCred.New(provider)
	} else {
		opts.Creds = minioCred.NewStaticV4(cfg.AK, cfg.SK, cfg.Token)
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
	// aliyunCreds doesn't provide a way to get the expire time, so we use the cache to check if it's expired
	// when aliyunCreds.GetAccessKeyId is different from the cache, we know it's expired
	akCache string
	cred    aliyunCred.Credential
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

	a.akCache = tea.StringValue(cred.AccessKeyId)
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
	return tea.StringValue(cred.AccessKeyId) != a.akCache
}
