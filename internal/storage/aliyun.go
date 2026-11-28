package storage

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/alibabacloud-go/tea/tea"
	aliyunCred "github.com/aliyun/credentials-go/credentials"
	aws "github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

// newAliyunClient returns an S3 client which is compatible for aliyun OSS.
// OSS resolves buckets by virtual host — new buckets accept nothing else — so
// the client keeps the DNS-style lookup the minio client used.
func newAliyunClient(ctx context.Context, cfg Config) (*S3Client, error) {
	log.Info("create aliyun storage client",
		zap.String("endpoint", cfg.Endpoint),
		zap.String("bucket", cfg.Bucket),
		zap.String("auth_type", cfg.Credential.Type.String()))

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
	mu              sync.Mutex
	cred            aliyunCred.Credential
	providerName    string
	retrievalFailed bool
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
	conf := resolveAliCredConfig()
	credentialType := "default"
	if conf != nil {
		credentialType = tea.StringValue(conf.Type)
	}
	fields := []zap.Field{
		zap.String("credential_type", credentialType),
		zap.Bool("env_access_key_id_set", os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID") != ""),
		zap.Bool("env_access_key_secret_set", os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET") != ""),
		zap.Bool("oidc_token_file_set", os.Getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE") != ""),
		zap.Bool("oidc_provider_arn_set", os.Getenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN") != ""),
		zap.Bool("role_arn_set", os.Getenv("ALIBABA_CLOUD_ROLE_ARN") != ""),
	}
	log.Info("initialize aliyun credential", fields...)
	cred, err := aliyunCred.NewCredential(conf)
	if err != nil {
		log.Warn("initialize aliyun credential failed", fields...)
	}
	return cred, err
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
		if !a.retrievalFailed {
			// SDK errors can include response bodies; the caller retains the error.
			log.Warn("aliyun credential retrieval failed",
				zap.String("credential_type", tea.StringValue(a.cred.GetType())),
				zap.String("last_provider_name", a.providerName))
		}
		a.retrievalFailed = true
		return aws.Credentials{}, fmt.Errorf("storage: get credential from aliyun credential: %w", err)
	}

	providerName := tea.StringValue(cred.ProviderName)
	if providerName == "" {
		providerName = "unknown"
	}
	if providerName != a.providerName || a.retrievalFailed {
		log.Info("aliyun credential provider resolved",
			zap.String("credential_type", tea.StringValue(a.cred.GetType())),
			zap.String("provider_name", providerName),
			zap.String("previous_provider_name", a.providerName),
			zap.Bool("recovered", a.retrievalFailed))
	}
	a.providerName = providerName
	a.retrievalFailed = false

	return aws.Credentials{
		AccessKeyID:     tea.StringValue(cred.AccessKeyId),
		SecretAccessKey: tea.StringValue(cred.AccessKeySecret),
		SessionToken:    tea.StringValue(cred.SecurityToken),
	}, nil
}
