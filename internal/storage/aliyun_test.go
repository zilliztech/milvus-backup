package storage

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/alibabacloud-go/tea/tea"
	aliyunCred "github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/log"
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
	model    *aliyunCred.CredentialModel
	err      error
	credType string
}

func (f *fakeAliCred) GetCredential() (*aliyunCred.CredentialModel, error) {
	return f.model, f.err
}

func (f *fakeAliCred) GetAccessKeyId() (*string, error)     { return nil, nil }
func (f *fakeAliCred) GetAccessKeySecret() (*string, error) { return nil, nil }
func (f *fakeAliCred) GetSecurityToken() (*string, error)   { return nil, nil }
func (f *fakeAliCred) GetBearerToken() *string              { return nil }
func (f *fakeAliCred) GetType() *string                     { return new(f.credType) }

func observeAliyunLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	previous := log.L()
	level := log.GetLevel()
	core, logs := observer.New(zapcore.InfoLevel)
	log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(level)})
	t.Cleanup(func() {
		log.ReplaceGlobals(previous, &log.ZapProperties{Level: zap.NewAtomicLevelAt(level)})
	})
	return logs
}

func TestNewAliyunClientLogsAuthMode(t *testing.T) {
	for _, auth := range []CredentialType{Static, IAM} {
		t.Run(auth.String(), func(t *testing.T) {
			logs := observeAliyunLogs(t)
			client, err := newAliyunClient(context.Background(), Config{
				Provider: cfg.ProviderAliyun,
				Endpoint: "oss-cn-hangzhou.aliyuncs.com:443",
				Bucket:   "example-bucket",
				Region:   "cn-hangzhou",
				UseSSL:   true,
				Credential: Credential{
					Type: auth, AK: "private-access-key", SK: "private-secret-key", Token: "private-token",
				},
			})
			require.NoError(t, err)
			require.NotNil(t, client)
			entries := logs.FilterMessage("create aliyun storage client").All()
			require.Len(t, entries, 1)
			assert.Equal(t, map[string]any{
				"endpoint":  "oss-cn-hangzhou.aliyuncs.com:443",
				"bucket":    "example-bucket",
				"auth_type": auth.String(),
			}, entries[0].ContextMap())
		})
	}
}

func TestAliCredProviderLogsProviderChanges(t *testing.T) {
	logs := observeAliyunLogs(t)
	fake := &fakeAliCred{credType: "default", model: &aliyunCred.CredentialModel{
		AccessKeyId:     new("private-access-key"),
		AccessKeySecret: new("private-secret-key"),
		SecurityToken:   new("private-session-token"),
		ProviderName:    new("default/oidc_role_arn"),
	}}
	p := &aliCredProvider{cred: fake}

	for _, name := range []string{"default/oidc_role_arn", "default/ecs_ram_role", ""} {
		previous := p.providerName
		fake.model.ProviderName = new(name)
		_, err := p.Retrieve(context.Background())
		require.NoError(t, err)
		entries := logs.TakeAll()
		require.Len(t, entries, 1)
		assert.Equal(t, "aliyun credential provider resolved", entries[0].Message)
		if name == "" {
			name = "unknown"
		}
		assert.Equal(t, map[string]any{
			"credential_type":        "default",
			"provider_name":          name,
			"previous_provider_name": previous,
			"recovered":              false,
		}, entries[0].ContextMap())

		// A new session from the same provider must not emit another log.
		fake.model.AccessKeyId = new("rotated-private-access-key")
		_, err = p.Retrieve(context.Background())
		require.NoError(t, err)
		assert.Zero(t, logs.Len())
	}
}

func TestAliCredProviderLogsFailureAndRecovery(t *testing.T) {
	for _, resolved := range []bool{false, true} {
		name := "InitialFailure"
		if resolved {
			name = "FailureAfterResolution"
		}
		t.Run(name, func(t *testing.T) {
			logs := observeAliyunLogs(t)
			fake := &fakeAliCred{credType: "oidc_role_arn", model: &aliyunCred.CredentialModel{
				ProviderName: new("oidc_role_arn"),
			}}
			p := &aliCredProvider{cred: fake}
			previous := ""
			if resolved {
				_, err := p.Retrieve(context.Background())
				require.NoError(t, err)
				logs.TakeAll()
				previous = "oidc_role_arn"
			}

			fake.err = errors.New("SDK response containing private-token")
			for range 2 {
				_, err := p.Retrieve(context.Background())
				assert.ErrorIs(t, err, fake.err)
			}
			entries := logs.TakeAll()
			require.Len(t, entries, 1)
			assert.Equal(t, zapcore.WarnLevel, entries[0].Level)
			assert.Equal(t, "aliyun credential retrieval failed", entries[0].Message)
			assert.Equal(t, map[string]any{
				"credential_type":    "oidc_role_arn",
				"last_provider_name": previous,
			}, entries[0].ContextMap())

			fake.err = nil
			_, err := p.Retrieve(context.Background())
			require.NoError(t, err)
			entries = logs.TakeAll()
			require.Len(t, entries, 1)
			assert.Equal(t, "aliyun credential provider resolved", entries[0].Message)
			assert.Equal(t, map[string]any{
				"credential_type":        "oidc_role_arn",
				"provider_name":          "oidc_role_arn",
				"previous_provider_name": previous,
				"recovered":              true,
			}, entries[0].ContextMap())
		})
	}
}

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
			logs := observeAliyunLogs(t)
			for _, k := range allEnvKeys {
				t.Setenv(k, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			cred, err := newAliyunCredential()
			credentialType := tt.wantType
			if tt.wantErr {
				credentialType = "oidc_role_arn"
			}
			entries := logs.FilterMessage("initialize aliyun credential").All()
			require.Len(t, entries, 1)
			assert.Equal(t, map[string]any{
				"credential_type":           credentialType,
				"env_access_key_id_set":     tt.env["ALIBABA_CLOUD_ACCESS_KEY_ID"] != "",
				"env_access_key_secret_set": tt.env["ALIBABA_CLOUD_ACCESS_KEY_SECRET"] != "",
				"oidc_token_file_set":       tt.env["ALIBABA_CLOUD_OIDC_TOKEN_FILE"] != "",
				"oidc_provider_arn_set":     tt.env["ALIBABA_CLOUD_OIDC_PROVIDER_ARN"] != "",
				"role_arn_set":              tt.env["ALIBABA_CLOUD_ROLE_ARN"] != "",
			}, entries[0].ContextMap())
			if tt.wantErr {
				failures := logs.FilterMessage("initialize aliyun credential failed").All()
				require.Len(t, failures, 1)
				assert.Equal(t, zapcore.WarnLevel, failures[0].Level)
				assert.Equal(t, entries[0].ContextMap(), failures[0].ContextMap())
				assert.Error(t, err)
				assert.Nil(t, cred)
				return
			}

			require.NoError(t, err)
			assert.Zero(t, logs.FilterLevelExact(zapcore.WarnLevel).Len())
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
