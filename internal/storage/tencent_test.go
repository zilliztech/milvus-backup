package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A missing token file makes construction fail deterministically without
// network. The second call returning the same error instance proves clients
// built after a failure reuse the init result instead of re-running the STS
// exchange per request — which is what would trip the STS rate limit.
func TestTencentCredProviderInitHappensOnce(t *testing.T) {
	t.Setenv("TKE_REGION", "ap-shanghai")
	t.Setenv("TKE_PROVIDER_ID", "provider-id")
	t.Setenv("TKE_WEB_IDENTITY_TOKEN_FILE", "/nonexistent/token")
	t.Setenv("TKE_ROLE_ARN", "role-arn")

	_, first := newTencentCredProvider()
	require.Error(t, first)

	_, second := newTencentCredProvider()
	assert.Same(t, first, second)
}

type fakeTencentCred struct {
	secretId string
}

func (f *fakeTencentCred) GetSecretId() string { return f.secretId }
func (f *fakeTencentCred) GetSecretKey() string {
	return "sk"
}
func (f *fakeTencentCred) GetToken() string { return "token" }
func (f *fakeTencentCred) GetCredential() (string, string, string) {
	return f.secretId, "sk", "token"
}

// Retrieve must hand out whatever the SDK credential currently holds. The SDK
// rotates its token under our feet, so any provider-side snapshot would go
// stale.
func TestTencentCredProviderServesFreshSession(t *testing.T) {
	fake := &fakeTencentCred{secretId: "ak-1"}
	p := &tencentCredProvider{creds: fake}

	first, err := p.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "ak-1", first.AccessKeyID)

	fake.secretId = "ak-2"

	assert.True(t, p.IsExpired())
	second, err := p.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "ak-2", second.AccessKeyID)
	assert.Equal(t, "token", second.SessionToken)
}

// Every minio client wrapper shares the one provider, so concurrent fetches
// must be safe; run under -race to pin the provider-level mutex.
func TestTencentCredProviderSurvivesConcurrentFetches(t *testing.T) {
	fake := &fakeTencentCred{secretId: "ak"}
	p := &tencentCredProvider{creds: fake}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Retrieve()
			assert.NoError(t, err)
			assert.True(t, p.IsExpired())
		}()
	}
	wg.Wait()
}
