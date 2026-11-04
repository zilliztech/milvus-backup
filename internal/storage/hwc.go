package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/provider"
	iam "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/model"
	hwcRegion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/region"
)

var _ aws.CredentialsProvider = (*HwcCredentialProvider)(nil)

// HwcCredentialProvider implements aws.CredentialsProvider on top of an OBS
// temporary credential minted from the IAM token of the environment.
type HwcCredentialProvider struct {
	// mu guards the cached temporary credential: the AWS SDK resolves
	// credentials from several goroutines.
	mu sync.Mutex

	expireTime time.Time
	lastCred   *model.Credential
	iamClient  *iam.IamClient
}

// Retrieve serves the cached temporary credential until two minutes before it
// expires, then mints a fresh one over the network. The 2-minute lead keeps a
// credential from dying mid-signature, and the cache keeps the mint rate at
// one per roughly two hours instead of one per request.
func (c *HwcCredentialProvider) Retrieve(_ context.Context) (aws.Credentials, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isExpired() {
		return c.awsValue(), nil
	}

	cred, err := retrieveTempCredential(c.iamClient)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("storage: hwc retrieve temporary credential: %w", err)
	}

	ti, err := time.Parse(time.RFC3339Nano, cred.ExpiresAt)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("storage: hwc parse temporary credential expire time: %w", err)
	}

	c.expireTime = ti
	c.lastCred = cred

	return c.awsValue(), nil
}

func (c *HwcCredentialProvider) isExpired() bool {
	now := time.Now().Add(time.Minute * 2) // prepare 2 minute get the new credential
	return now.After(c.expireTime)
}

func (c *HwcCredentialProvider) awsValue() aws.Credentials {
	return aws.Credentials{
		AccessKeyID:     c.lastCred.Access,
		SecretAccessKey: c.lastCred.Secret,
		SessionToken:    c.lastCred.Securitytoken,
		CanExpire:       true,
		Expires:         c.expireTime,
	}
}

func newHwcCredProvider(cfg Config) (aws.CredentialsProvider, error) {
	iamCred, err := provider.BasicCredentialEnvProvider().GetCredentials()
	if err != nil {
		return nil, fmt.Errorf("storage: new hwc iam credential: %w", err)
	}
	region, err := hwcRegion.SafeValueOf(cfg.Region)
	if err != nil {
		return nil, fmt.Errorf("storage: new hwc get region: %w", err)
	}

	hcClient, err := iam.IamClientBuilder().WithCredential(iamCred).WithRegion(region).SafeBuild()
	if err != nil {
		return nil, fmt.Errorf("storage: new hwc iam client: %w", err)
	}
	iamClient := iam.NewIamClient(hcClient)

	cred, err := retrieveTempCredential(iamClient)
	if err != nil {
		return nil, fmt.Errorf("storage: hwc create temporary credential: %w", err)
	}

	t, err := time.Parse(time.RFC3339Nano, cred.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("storage: hwc parse temporary credential expire time: %w", err)
	}

	return &HwcCredentialProvider{iamClient: iamClient, expireTime: t, lastCred: cred}, nil
}

func retrieveTempCredential(iamClient *iam.IamClient) (*model.Credential, error) {
	duration := int32(2 * time.Hour.Seconds())
	req := &model.CreateTemporaryAccessKeyByTokenRequest{
		Body: &model.CreateTemporaryAccessKeyByTokenRequestBody{
			Auth: &model.TokenAuth{
				Identity: &model.TokenAuthIdentity{
					Methods: []model.TokenAuthIdentityMethods{
						model.GetTokenAuthIdentityMethodsEnum().TOKEN,
					},
					Token: &model.IdentityToken{
						DurationSeconds: &duration,
					},
				},
			},
		},
	}

	resp, err := iamClient.CreateTemporaryAccessKeyByToken(req)
	if err != nil {
		return nil, fmt.Errorf("storage: hwc retrieve temporary credential: %w", err)
	}

	if resp.Credential == nil {
		return nil, errors.New("storage: hwc retrieve temporary credential is nil")
	}

	return resp.Credential, nil
}

func NewHwcClient(cfg Config) (*S3Client, error) {
	provider, ok, err := baseS3CredProvider(cfg.Credential)
	if err != nil {
		return nil, err
	}
	if !ok {
		if cfg.Credential.Type != IAM {
			return nil, fmt.Errorf("storage: hwc unsupported credential type: %s", cfg.Credential.Type.String())
		}
		provider, err = newHwcCredProvider(cfg)
		if err != nil {
			return nil, fmt.Errorf("storage: new hwc iam provider: %w", err)
		}
	}

	// OBS resolves buckets by virtual host, so the client keeps the DNS-style
	// lookup the minio client used.
	return newS3ClientWithCreds(cfg, provider, false)
}
