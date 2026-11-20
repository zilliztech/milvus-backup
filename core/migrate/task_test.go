package migrate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/client/cloud"
)

type fakeCloudClient struct {
	applyFn   func(ctx context.Context, clusterID, dir string) (*cloud.ApplyVolumeResp, error)
	migrateFn func(ctx context.Context, source cloud.Source, dest cloud.Destination) (string, error)
}

func (f *fakeCloudClient) ApplyVolume(ctx context.Context, clusterID, dir string) (*cloud.ApplyVolumeResp, error) {
	return f.applyFn(ctx, clusterID, dir)
}

func (f *fakeCloudClient) Migrate(ctx context.Context, source cloud.Source, dest cloud.Destination) (string, error) {
	if f.migrateFn != nil {
		return f.migrateFn(ctx, source, dest)
	}
	return "", nil
}

func volumeResp(seq int, expiry time.Time) *cloud.ApplyVolumeResp {
	return &cloud.ApplyVolumeResp{
		Credentials: cloud.Credentials{
			ExpireTime:   expiry.UTC().Format(time.RFC3339Nano),
			SessionToken: fmt.Sprintf("token-%d", seq),
			TmpAK:        fmt.Sprintf("ak-%d", seq),
			TmpSK:        fmt.Sprintf("sk-%d", seq),
		},
	}
}

func TestVolumeRetrieve(t *testing.T) {
	t.Run("CachesUntilExpiredAndRefreshes", func(t *testing.T) {
		var calls atomic.Int32
		cli := &fakeCloudClient{
			applyFn: func(_ context.Context, clusterID, dir string) (*cloud.ApplyVolumeResp, error) {
				assert.Equal(t, "cluster-1", clusterID)
				assert.Equal(t, "prefix-1", dir)
				n := int(calls.Add(1))
				return volumeResp(n, time.Now().Add(time.Hour)), nil
			},
		}

		vol := newVolume("cluster-1", "prefix-1", cli)

		// First retrieve applies a credential.
		creds1, err := vol.Retrieve(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "ak-1", creds1.AccessKeyID)
		assert.Equal(t, "sk-1", creds1.SecretAccessKey)
		assert.Equal(t, "token-1", creds1.SessionToken)
		assert.True(t, creds1.CanExpire)
		assert.Equal(t, int32(1), calls.Load())

		// Second retrieve returns the cached credential without re-applying.
		creds2, err := vol.Retrieve(t.Context())
		require.NoError(t, err)
		assert.Equal(t, creds1, creds2)
		assert.Equal(t, int32(1), calls.Load())

		// Expire the cached credential and verify it refreshes.
		vol.currentResp.mu.Lock()
		vol.currentResp.expiration = time.Now().Add(-time.Second)
		vol.currentResp.mu.Unlock()

		creds3, err := vol.Retrieve(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "ak-2", creds3.AccessKeyID)
		assert.Equal(t, "token-2", creds3.SessionToken)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("ConcurrentRetrieveAcrossExpirations", func(t *testing.T) {
		var calls atomic.Int32
		cli := &fakeCloudClient{
			applyFn: func(_ context.Context, _, _ string) (*cloud.ApplyVolumeResp, error) {
				n := int(calls.Add(1))
				return volumeResp(n, time.Now().Add(2*time.Millisecond)), nil
			},
		}

		vol := newVolume("cluster-1", "prefix-1", cli)

		const goroutines = 16
		const iterations = 40
		ctx := t.Context()
		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				for range iterations {
					creds, err := vol.Retrieve(ctx)
					assert.NoError(t, err)
					assert.NotEmpty(t, creds.AccessKeyID)
					assert.NotEmpty(t, creds.SessionToken)
				}
			})
		}
		wg.Wait()
	})

	t.Run("ApplyErrorIsReturned", func(t *testing.T) {
		cli := &fakeCloudClient{
			applyFn: func(_ context.Context, _, _ string) (*cloud.ApplyVolumeResp, error) {
				return nil, errors.New("cloud unavailable")
			},
		}

		vol := newVolume("cluster-1", "prefix-1", cli)
		_, err := vol.Retrieve(t.Context())
		require.Error(t, err)
		assert.ErrorContains(t, err, "cloud unavailable")
	})

	t.Run("InvalidExpireTimeReturnsError", func(t *testing.T) {
		cli := &fakeCloudClient{
			applyFn: func(_ context.Context, _, _ string) (*cloud.ApplyVolumeResp, error) {
				return &cloud.ApplyVolumeResp{
					Credentials: cloud.Credentials{ExpireTime: "not-a-time"},
				}, nil
			},
		}

		vol := newVolume("cluster-1", "prefix-1", cli)
		_, err := vol.Retrieve(t.Context())
		require.Error(t, err)
		assert.ErrorContains(t, err, "parse expire time")
	})
}

func TestVolumeToken(t *testing.T) {
	t.Run("InitialApplyCachesAndRefreshes", func(t *testing.T) {
		var calls atomic.Int32
		cli := &fakeCloudClient{
			applyFn: func(_ context.Context, _, _ string) (*cloud.ApplyVolumeResp, error) {
				n := int(calls.Add(1))
				return volumeResp(n, time.Now().Add(time.Hour)), nil
			},
		}

		vol := newVolume("cluster-1", "prefix-1", cli)

		// Initial call with nil resp should apply instead of panicking.
		tok1, err := vol.Token()
		require.NoError(t, err)
		assert.Equal(t, "token-1", tok1.AccessToken)
		assert.Equal(t, int32(1), calls.Load())

		// Cached hit.
		tok2, err := vol.Token()
		require.NoError(t, err)
		assert.Equal(t, "token-1", tok2.AccessToken)
		assert.Equal(t, int32(1), calls.Load())

		// Expire and refresh.
		vol.currentResp.mu.Lock()
		vol.currentResp.expiration = time.Now().Add(-time.Second)
		vol.currentResp.mu.Unlock()

		tok3, err := vol.Token()
		require.NoError(t, err)
		assert.Equal(t, "token-2", tok3.AccessToken)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("ConcurrentTokenAcrossExpirations", func(t *testing.T) {
		var calls atomic.Int32
		cli := &fakeCloudClient{
			applyFn: func(_ context.Context, _, _ string) (*cloud.ApplyVolumeResp, error) {
				n := int(calls.Add(1))
				// oauth2.Token.Valid() subtracts a 10s expiryDelta, so add 2ms past that
				// to exercise both cached RLock reads and concurrent Lock refreshes.
				return volumeResp(n, time.Now().Add(10*time.Second+2*time.Millisecond)), nil
			},
		}

		vol := newVolume("cluster-1", "prefix-1", cli)

		const goroutines = 16
		const iterations = 40
		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				for range iterations {
					tok, err := vol.Token()
					assert.NoError(t, err)
					assert.NotEmpty(t, tok.AccessToken)
				}
			})
		}
		wg.Wait()
	})
}
