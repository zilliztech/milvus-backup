package storage

import (
	"errors"
	"fmt"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/provider"
	iam "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/model"
	hwcRegion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/region"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
)

var _ minioCred.Provider = (*HwcCredentialProvider)(nil)

type HwcCredentialProvider struct {
	expireTime time.Time
	iamClient  *iam.IamClient
}

func (c *HwcCredentialProvider) RetrieveWithCredContext(_ *minioCred.CredContext) (minioCred.Value, error) {
	return c.Retrieve()
}

func (c *HwcCredentialProvider) Retrieve() (minioCred.Value, error) {
	cred, err := retrieveTempCredential(c.iamClient)
	if err != nil {
		return minioCred.Value{}, fmt.Errorf("storage: hwc retrieve tempporary credential %w", err)
	}

	ti, err := time.Parse(time.RFC3339Nano, cred.ExpiresAt)
	if err != nil {
		return minioCred.Value{}, fmt.Errorf("storage: hwc retrieve tempporary credential parse expireTime time %w", err)
	}

	c.expireTime = ti

	return minioCred.Value{
		AccessKeyID:     cred.Access,
		SecretAccessKey: cred.Secret,
		SessionToken:    cred.Securitytoken,
	}, nil
}

func (c *HwcCredentialProvider) IsExpired() bool {
	now := time.Now().Add(time.Minute * 2) //prepare 2 minute get the new credential
	expired := now.After(c.expireTime)
	return expired
}

func newHwcCredProvider(cfg Config) (minioCred.Provider, error) {
	iamCred, err := provider.BasicCredentialEnvProvider().GetCredentials()
	if err != nil {
		return nil, fmt.Errorf("storage: new hwc iam credential  %w", err)
	}
	region, err := hwcRegion.SafeValueOf(cfg.Region)
	if err != nil {
		return nil, fmt.Errorf("storage: new hwc get region %w", err)
	}

	hcClient, err := iam.IamClientBuilder().WithCredential(iamCred).WithRegion(region).SafeBuild()
	if err != nil {
		return nil, fmt.Errorf("storage: new hwc hcClient %w", err)
	}
	iamClient := iam.NewIamClient(hcClient)

	cred, err := retrieveTempCredential(iamClient)
	if err != nil {
		return nil, fmt.Errorf("storage: hwc create tempporary credential %w", err)
	}

	t, err := time.Parse(time.RFC3339Nano, cred.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("storage: hwc create tempporary parse expireTime time %w", err)
	}

	return &HwcCredentialProvider{iamClient: iamClient, expireTime: t}, nil
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
		return nil, fmt.Errorf("storage: hwc retrieve temporary credential %w", err)
	}

	if resp.Credential == nil {
		return nil, errors.New("storage: hwc retrieve temporary credential is nil")
	}

	return resp.Credential, nil
}

func NewHwcClient(cfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region, BucketLookup: minio.BucketLookupDNS}
	switch cfg.Credential.Type {
	case IAM:
		hwcProvider, err := newHwcCredProvider(cfg)
		if err != nil {
			return nil, fmt.Errorf("storage: new hwc iam provider %w", err)
		}
		opts.Creds = minioCred.New(hwcProvider)
	case Static:
		opts.Creds = minioCred.NewStaticV4(cfg.Credential.AK, cfg.Credential.SK, cfg.Credential.Token)
	default:
		return nil, fmt.Errorf("storage: new hwc client unsupported credential type %s", cfg.Credential.Type)
	}

	return newInternalMinio(cfg, &opts)
}
