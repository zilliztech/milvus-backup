package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/alibabacloud-go/tea/tea"
	aliyunCred "github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Building the provider must not depend on any config: every aliyun client in
// the process has to land on the same instance, or each of them would refresh
// STS tokens on its own and multiply the AssumeRoleWithOIDC traffic.
func TestAliCredProviderIsProcessWide(t *testing.T) {
	first, err := newAliCredProvider()
	require.NoError(t, err)

	second, err := newAliCredProvider()
	require.NoError(t, err)

	assert.Same(t, first, second)
}

type fakeAliCred struct {
	model *aliyunCred.CredentialModel
}

func (f *fakeAliCred) GetCredential() (*aliyunCred.CredentialModel, error) {
	return f.model, nil
}

func (f *fakeAliCred) GetAccessKeyId() (*string, error)     { return nil, nil }
func (f *fakeAliCred) GetAccessKeySecret() (*string, error) { return nil, nil }
func (f *fakeAliCred) GetSecurityToken() (*string, error)   { return nil, nil }
func (f *fakeAliCred) GetBearerToken() *string              { return nil }
func (f *fakeAliCred) GetType() *string                     { return nil }

// Retrieve must hand out whatever the SDK session currently holds. The SDK
// rotates its session under our feet, so any provider-side snapshot would go
// stale; the AWS signing middleware calls Retrieve per operation.
func TestAliCredProviderServesFreshSession(t *testing.T) {
	fake := &fakeAliCred{model: &aliyunCred.CredentialModel{
		AccessKeyId:     new("ak-1"),
		AccessKeySecret: new("sk-1"),
		SecurityToken:   new("token-1"),
	}}
	p := &aliCredProvider{cred: fake}

	first, err := p.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ak-1", first.AccessKeyID)

	fake.model = &aliyunCred.CredentialModel{
		AccessKeyId:     new("ak-2"),
		AccessKeySecret: new("sk-2"),
		SecurityToken:   new("token-2"),
	}

	second, err := p.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ak-2", second.AccessKeyID)
	assert.Equal(t, "token-2", second.SessionToken)
}

// Every minio client wrapper shares the one provider, so concurrent fetches
// must be safe; run under -race to pin the provider-level mutex.
func TestAliCredProviderSurvivesConcurrentFetches(t *testing.T) {
	fake := &fakeAliCred{model: &aliyunCred.CredentialModel{
		AccessKeyId:     new("ak"),
		AccessKeySecret: new("sk"),
		SecurityToken:   new("token"),
	}}
	p := &aliCredProvider{cred: fake}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, err := p.Retrieve(context.Background())
			assert.NoError(t, err)
		})
	}
	wg.Wait()
}

func TestNewAliyunCredential(t *testing.T) {
	allEnvKeys := []string{
		"ALIBABA_CLOUD_ACCESS_KEY_ID",
		"ALIBABA_CLOUD_ACCESS_KEY_SECRET",
		"ALIBABA_CLOUD_OIDC_TOKEN_FILE",
		"ALIBABA_CLOUD_OIDC_PROVIDER_ARN",
		"ALIBABA_CLOUD_ROLE_ARN",
	}

	tests := []struct {
		name     string
		env      map[string]string
		wantType string
		wantErr  bool
	}{
		{
			name:     "NoEnvVarsUsesDefault",
			wantType: "default",
		},
		{
			name: "FullRRSAEnvUsesExplicitOIDC",
			env: map[string]string{
				"ALIBABA_CLOUD_OIDC_TOKEN_FILE":   "/var/run/secrets/tokens/oidc-token",
				"ALIBABA_CLOUD_OIDC_PROVIDER_ARN": "acs:ram::1234567890123456:oidc-provider/ack-rrsa-example",
				"ALIBABA_CLOUD_ROLE_ARN":          "acs:ram::1234567890123456:role/example-role",
			},
			wantType: "oidc_role_arn",
		},
		{
			name: "StaticEnvTakesPrecedenceOverRRSA",
			env: map[string]string{
				"ALIBABA_CLOUD_ACCESS_KEY_ID":     "static-ak",
				"ALIBABA_CLOUD_ACCESS_KEY_SECRET": "static-sk",
				"ALIBABA_CLOUD_OIDC_TOKEN_FILE":   "/var/run/secrets/tokens/oidc-token",
				"ALIBABA_CLOUD_OIDC_PROVIDER_ARN": "acs:ram::1234567890123456:oidc-provider/ack-rrsa-example",
				"ALIBABA_CLOUD_ROLE_ARN":          "acs:ram::1234567890123456:role/example-role",
			},
			wantType: "default",
		},
		{
			name: "PartialStaticEnvWithRRSAUsesExplicitOIDC",
			env: map[string]string{
				"ALIBABA_CLOUD_ACCESS_KEY_ID":     "static-ak-only",
				"ALIBABA_CLOUD_OIDC_TOKEN_FILE":   "/var/run/secrets/tokens/oidc-token",
				"ALIBABA_CLOUD_OIDC_PROVIDER_ARN": "acs:ram::1234567890123456:oidc-provider/ack-rrsa-example",
				"ALIBABA_CLOUD_ROLE_ARN":          "acs:ram::1234567890123456:role/example-role",
			},
			wantType: "oidc_role_arn",
		},
		{
			name: "PartialRRSAOnlyTokenFileFailsFast",
			env: map[string]string{
				"ALIBABA_CLOUD_OIDC_TOKEN_FILE": "/var/run/secrets/tokens/oidc-token",
			},
			wantErr: true,
		},
		{
			name: "PartialRRSAMissingRoleARNFailsFast",
			env: map[string]string{
				"ALIBABA_CLOUD_OIDC_TOKEN_FILE":   "/var/run/secrets/tokens/oidc-token",
				"ALIBABA_CLOUD_OIDC_PROVIDER_ARN": "acs:ram::1234567890123456:oidc-provider/ack-rrsa-example",
			},
			wantErr: true,
		},
		{
			name: "PartialRRSAOnlyProviderARNFailsFast",
			env: map[string]string{
				"ALIBABA_CLOUD_OIDC_PROVIDER_ARN": "acs:ram::1234567890123456:oidc-provider/ack-rrsa-example",
			},
			wantErr: true,
		},
		{
			name: "RoleARNAloneUsesDefault",
			env: map[string]string{
				"ALIBABA_CLOUD_ROLE_ARN": "acs:ram::1234567890123456:role/example-role",
			},
			wantType: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range allEnvKeys {
				t.Setenv(k, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			cred, err := newAliyunCredential()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, cred)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cred)
			assert.Equal(t, tt.wantType, tea.StringValue(cred.GetType()))
		})
	}

	t.Run("StaticEnvCredentialsResolvedWhenRRSAAlsoSet", func(t *testing.T) {
		for _, k := range allEnvKeys {
			t.Setenv(k, "")
		}
		t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "static-ak")
		t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "static-sk")
		t.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", "/var/run/secrets/tokens/oidc-token")
		t.Setenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN", "acs:ram::1234567890123456:oidc-provider/ack-rrsa-example")
		t.Setenv("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::1234567890123456:role/example-role")

		cred, err := newAliyunCredential()
		require.NoError(t, err)

		model, err := cred.GetCredential()
		require.NoError(t, err)
		assert.Equal(t, "static-ak", tea.StringValue(model.AccessKeyId))
		assert.Equal(t, "static-sk", tea.StringValue(model.AccessKeySecret))
	})
}
