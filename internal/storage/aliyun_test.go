package storage

import (
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
// stale.
func TestAliCredProviderServesFreshSession(t *testing.T) {
	fake := &fakeAliCred{model: &aliyunCred.CredentialModel{
		AccessKeyId:     tea.String("ak-1"),
		AccessKeySecret: tea.String("sk-1"),
		SecurityToken:   tea.String("token-1"),
	}}
	p := &aliCredProvider{cred: fake}

	first, err := p.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "ak-1", first.AccessKeyID)

	fake.model = &aliyunCred.CredentialModel{
		AccessKeyId:     tea.String("ak-2"),
		AccessKeySecret: tea.String("sk-2"),
		SecurityToken:   tea.String("token-2"),
	}

	assert.True(t, p.IsExpired())
	second, err := p.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "ak-2", second.AccessKeyID)
	assert.Equal(t, "token-2", second.SessionToken)
}

// Every minio client wrapper shares the one provider, so concurrent fetches
// must be safe; run under -race to pin the provider-level mutex.
func TestAliCredProviderSurvivesConcurrentFetches(t *testing.T) {
	fake := &fakeAliCred{model: &aliyunCred.CredentialModel{
		AccessKeyId:     tea.String("ak"),
		AccessKeySecret: tea.String("sk"),
		SecurityToken:   tea.String("token"),
	}}
	p := &aliCredProvider{cred: fake}

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
