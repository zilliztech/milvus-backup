package storage

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/alibabacloud-go/tea/tea"
	aliyunCred "github.com/aliyun/credentials-go/credentials"
	aws "github.com/aws/aws-sdk-go-v2/aws"
)

// newAliyunClient returns an S3 client which is compatible for aliyun OSS.
// OSS resolves buckets by virtual host — new buckets accept nothing else — so
// the client keeps the DNS-style lookup the minio client used.
func newAliyunClient(ctx context.Context, cfg Config) (*S3Client, error) {
	var provider aws.CredentialsProvider
	switch cfg.Credential.Type {
	case IAM:
		p, err := newAliCredProvider()
		if err != nil {
			return nil, err
		}
		provider = p
	case Static:
		provider = staticS3CredProvider(cfg.Credential)
	case AWSCredProvider:
		if cfg.Credential.AWSCredProvider == nil {
			return nil, fmt.Errorf("storage: aliyun credential type %s requires a provider", cfg.Credential.Type.String())
		}
		provider = cfg.Credential.AWSCredProvider
	default:
		return nil, fmt.Errorf("storage: aliyun unsupported credential type: %s", cfg.Credential.Type.String())
	}

	return newS3ClientWithCreds(ctx, cfg, provider, false)
}

// aliCredProvider implements aws.CredentialsProvider on top of the
// credentials-go session.
type aliCredProvider struct {
	// mu serializes the SDK calls: the AWS SDK resolves credentials from
	// several goroutines while this provider is shared by all of the
	// process's clients, and the credentials-go session caches hold no lock
	// of their own.
	mu   sync.Mutex
	cred aliyunCred.Credential
}

// resolveAliCredConfig chooses the credentials-go Config for IAM auth.
// When RRSA/OIDC environment variables are set and static env keys are not,
// select oidc_role_arn explicitly instead of relying on the default chain:
// credentials-go's DefaultCredentialsProvider sets lastUsedProvider before
// calling p.GetCredentials() and falls through on error without resetting it,
// so a transient STS AssumeRoleWithOIDC failure silently falls back to
// ecs_ram_role (IMDS) and permanently latches that provider on the singleton.
func resolveAliCredConfig() *aliyunCred.Config {
	if os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID") != "" && os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET") != "" {
		return nil
	}
	if os.Getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE") != "" || os.Getenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN") != "" {
		return &aliyunCred.Config{Type: new("oidc_role_arn")}
	}
	return nil
}

func newAliyunCredential() (aliyunCred.Credential, error) {
	return aliyunCred.NewCredential(resolveAliCredConfig())
}

// newAliCredProvider returns the process-wide aliyun credential provider. One
// per process, because the credentials-go SDK fetches and refreshes STS tokens
// per provider instance: one per client would multiply the AssumeRoleWithOIDC
// traffic by the number of clients and trip the STS rate limit.
var newAliCredProvider = sync.OnceValues(func() (aws.CredentialsProvider, error) {
	cred, err := newAliyunCredential()
	if err != nil {
		return nil, fmt.Errorf("storage: create aliyun credential: %w", err)
	}

	return &aliCredProvider{cred: cred}, nil
})

// Retrieve returns the current credentials-go session. The SDK serves its
// cached session and refreshes it 180s before expiry, so answering the AWS
// SDK's every fetch costs no extra STS traffic.
func (a *aliCredProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cred, err := a.cred.GetCredential()
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("storage: get credential from aliyun credential: %w", err)
	}

	return aws.Credentials{
		AccessKeyID:     tea.StringValue(cred.AccessKeyId),
		SecretAccessKey: tea.StringValue(cred.AccessKeySecret),
		SessionToken:    tea.StringValue(cred.SecurityToken),
	}, nil
}
