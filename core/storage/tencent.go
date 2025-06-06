package storage

import (
	"fmt"

	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

// NewTencentClient returns a minio.Client which is compatible for tencent OSS
func NewTencentClient(cfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, BucketLookup: minio.BucketLookupDNS}
	if cfg.UseIAM {
		provider, err := newTencentCredProvider()
		if err != nil {
			return nil, fmt.Errorf("storage: create tencent credential provider: %w", err)
		}
		opts.Creds = minioCred.New(provider)
	} else {
		opts.Creds = minioCred.NewStaticV4(cfg.AK, cfg.SK, cfg.Token)
	}

	cli, err := minio.New(cfg.Endpoint, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: new tencent client %w", err)
	}

	return &MinioClient{cli: cli, cfg: cfg}, nil
}

// tencentCredProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type tencentCredProvider struct {
	// tencentCreds doesn't provide a way to get the expired time, so we use the cache to check if it's expired
	// when tencentCreds.GetSecretId is different from the cache, we know it's expired
	akCache string
	creds   common.CredentialIface
}

func newTencentCredProvider() (minioCred.Provider, error) {
	provider, err := common.DefaultTkeOIDCRoleArnProvider()
	if err != nil {
		return nil, fmt.Errorf("storage: create tencent credential provider: %w", err)
	}

	cred, err := provider.GetCredential()
	if err != nil {
		return nil, fmt.Errorf("storage: get credential from tencent credential provider: %w", err)
	}
	return &tencentCredProvider{creds: cred}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (c *tencentCredProvider) Retrieve() (minioCred.Value, error) {
	ak := c.creds.GetSecretId()
	c.akCache = ak

	sk := c.creds.GetSecretKey()
	securityToken := c.creds.GetToken()
	return minioCred.Value{
		AccessKeyID:     ak,
		SecretAccessKey: sk,
		SessionToken:    securityToken,
	}, nil
}

func (c *tencentCredProvider) RetrieveWithCredContext(_ *minioCred.CredContext) (minioCred.Value, error) {
	return c.Retrieve()
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (c *tencentCredProvider) IsExpired() bool {
	ak := c.creds.GetSecretId()
	return ak != c.akCache
}
