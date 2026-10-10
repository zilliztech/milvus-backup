package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"

	"github.com/zilliztech/milvus-backup/internal/retry"
)

// NewTencentClient returns a minio.Client which is compatible for tencent OSS
func newTencentClient(cfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region, BucketLookup: minio.BucketLookupDNS}
	switch cfg.Credential.Type {
	case IAM:
		provider, err := newTencentCredProvider()
		if err != nil {
			return nil, err
		}
		opts.Creds = minioCred.New(provider)
	case Static:
		opts.Creds = minioCred.NewStaticV4(cfg.Credential.AK, cfg.Credential.SK, cfg.Credential.Token)
	default:
		return nil, fmt.Errorf("storage: tencent unsupported credential type: %s", cfg.Credential.Type)
	}

	return newInternalMinio(cfg, &opts)
}

// tencentCredProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type tencentCredProvider struct {
	// mu serializes the SDK calls: minio locks each Credentials wrapper
	// separately while this provider is shared by all of them.
	mu    sync.Mutex
	creds common.CredentialIface
}

// newTencentCredProvider returns the process-wide tencent credential provider.
// One per process, because building the TKE OIDC provider exchanges the web
// identity token for STS credentials right away: one per client would issue an
// AssumeRoleWithWebIdentity call per client and trip the STS rate limit.
var newTencentCredProvider = sync.OnceValues(func() (minioCred.Provider, error) {
	provider, err := common.DefaultTkeOIDCRoleArnProvider()
	if err != nil {
		return nil, fmt.Errorf("storage: create tencent credential provider: %w", err)
	}

	// GetCredential exchanges the TKE web identity token for STS credentials
	// over the network, and OnceValues caches the outcome for the process
	// lifetime, so retry transient blips instead of failing init forever.
	// Background context on purpose: the init must not be cancellable by
	// whichever request happened to trigger it.
	var cred common.CredentialIface
	err = retry.Do(context.Background(), func() error {
		var exchangeErr error
		cred, exchangeErr = provider.GetCredential()
		return exchangeErr
	})
	if err != nil {
		return nil, fmt.Errorf("storage: get credential from tencent credential provider: %w", err)
	}

	return &tencentCredProvider{creds: cred}, nil
})

// Retrieve returns the current SDK credential. RoleArnCredential serves its
// cached token and refreshes it internally when due, so answering minio's
// every fetch costs no extra STS traffic.
func (c *tencentCredProvider) Retrieve() (minioCred.Value, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return minioCred.Value{
		AccessKeyID:     c.creds.GetSecretId(),
		SecretAccessKey: c.creds.GetSecretKey(),
		SessionToken:    c.creds.GetToken(),
	}, nil
}

func (c *tencentCredProvider) RetrieveWithCredContext(_ *minioCred.CredContext) (minioCred.Value, error) {
	return c.Retrieve()
}

// IsExpired always reports expired so minio re-fetches through Retrieve on
// every signature. The SDK keeps the real expiration private and re-serves its
// cached token until it refreshes internally, so the cached secret id this
// struct used to keep only bridged that gap; delegating expiry to the SDK is
// simpler and cannot go stale.
func (c *tencentCredProvider) IsExpired() bool {
	return true
}
