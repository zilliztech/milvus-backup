package storage

import (
	"fmt"
	"sync"

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
			return nil, err
		}
		opts.Creds = minioCred.New(provider)
	case Static:
		opts.Creds = minioCred.NewStaticV4(cfg.Credential.AK, cfg.Credential.SK, cfg.Credential.Token)
	case MinioCredProvider:
		opts.Creds = minioCred.New(cfg.Credential.MinioCredProvider)
	default:
		return nil, fmt.Errorf("storage: aliyun unsupported credential type: %s", cfg.Credential.Type.String())
	}

	return newInternalMinio(cfg, &opts)
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type aliCredProvider struct {
	// mu serializes the SDK calls: minio locks each Credentials wrapper
	// separately while this provider is shared by all of them, and the
	// credentials-go session caches hold no lock of their own.
	mu   sync.Mutex
	cred aliyunCred.Credential
}

// newAliCredProvider returns the process-wide aliyun credential provider. One
// per process, because the credentials-go SDK fetches and refreshes STS tokens
// per provider instance: one per client would multiply the AssumeRoleWithOIDC
// traffic by the number of clients and trip the STS rate limit.
var newAliCredProvider = sync.OnceValues(func() (minioCred.Provider, error) {
	cred, err := aliyunCred.NewCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("storage: create aliyun credential: %w", err)
	}

	return &aliCredProvider{cred: cred}, nil
})

// Retrieve returns the current credentials-go session. The SDK serves its
// cached session and refreshes it 180s before expiry, so answering minio's
// every fetch costs no extra STS traffic.
func (a *aliCredProvider) Retrieve() (minioCred.Value, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cred, err := a.cred.GetCredential()
	if err != nil {
		return minioCred.Value{}, fmt.Errorf("storage: get credential from aliyun credential: %w", err)
	}

	return minioCred.Value{
		AccessKeyID:     tea.StringValue(cred.AccessKeyId),
		SecretAccessKey: tea.StringValue(cred.AccessKeySecret),
		SessionToken:    tea.StringValue(cred.SecurityToken),
	}, nil
}

func (a *aliCredProvider) RetrieveWithCredContext(_ *minioCred.CredContext) (minioCred.Value, error) {
	return a.Retrieve()
}

// IsExpired always reports expired so minio re-fetches through Retrieve on
// every signature. The SDK keeps the real expiration private and re-serves its
// cached session until its own refresh time, so the cache-and-compare this
// struct used to keep only bridged that gap; delegating expiry to the SDK is
// simpler and cannot go stale.
func (a *aliCredProvider) IsExpired() bool {
	return true
}
