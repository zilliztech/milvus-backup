package storage

import (
	"context"
	"fmt"
	"sync"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"

	"github.com/zilliztech/milvus-backup/internal/retry"
)

// newTencentClient returns an S3 client which is compatible for tencent COS.
// COS resolves buckets by virtual host, so the client keeps the DNS-style
// lookup the minio client used.
func newTencentClient(cfg Config) (*S3Client, error) {
	provider, ok, err := baseS3CredProvider(cfg.Credential)
	if err != nil {
		return nil, err
	}
	if !ok {
		if cfg.Credential.Type != IAM {
			return nil, fmt.Errorf("storage: tencent unsupported credential type: %s", cfg.Credential.Type.String())
		}
		provider, err = newTencentCredProvider()
		if err != nil {
			return nil, err
		}
	}

	return newS3ClientWithCreds(cfg, provider, false)
}

// tencentCredProvider implements aws.CredentialsProvider on top of the
// tencentcloud-sdk-go credential.
type tencentCredProvider struct {
	// mu serializes the SDK calls: the AWS SDK resolves credentials from
	// several goroutines while this provider is shared by all of them.
	mu    sync.Mutex
	creds common.CredentialIface
}

// newTencentCredProvider returns the process-wide tencent credential provider.
// One per process, because building the TKE OIDC provider exchanges the web
// identity token for STS credentials right away: one per client would issue an
// AssumeRoleWithWebIdentity call per client and trip the STS rate limit.
var newTencentCredProvider = sync.OnceValues(func() (aws.CredentialsProvider, error) {
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
// cached token and refreshes it internally when due, so answering the AWS
// SDK's every fetch costs no extra STS traffic.
func (c *tencentCredProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return aws.Credentials{
		AccessKeyID:     c.creds.GetSecretId(),
		SecretAccessKey: c.creds.GetSecretKey(),
		SessionToken:    c.creds.GetToken(),
	}, nil
}
