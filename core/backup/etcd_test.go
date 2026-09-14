package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// blockingEtcd stands in for an endpoint that accepts the connection and then
// never answers -- the failure mode reported in #1216. Every call returns only
// when the caller's context is done, so a test that finishes proves the caller
// imposed a deadline of its own.
type blockingEtcd struct{ clientv3.KV }

func (b *blockingEtcd) Get(ctx context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockingEtcd) Status(ctx context.Context, _ string) (*clientv3.StatusResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// downEtcd fails every call immediately, as an endpoint that is not listening at
// all would.
type downEtcd struct{ clientv3.KV }

func (d *downEtcd) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return nil, errors.New("connection refused")
}

func (d *downEtcd) Status(context.Context, string) (*clientv3.StatusResponse, error) {
	return nil, errors.New("connection refused")
}

// oneUpEtcd answers only for upEndpoint, as a cluster with one down member does.
type oneUpEtcd struct {
	clientv3.KV

	upEndpoint string
}

func (o *oneUpEtcd) Status(_ context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	if endpoint != o.upEndpoint {
		return nil, errors.New("connection refused")
	}
	return &clientv3.StatusResponse{}, nil
}

func TestEtcdMeta_Probe(t *testing.T) {
	t.Run("Reachable", func(t *testing.T) {
		etcd := newEtcdMeta(&fakeKV{}, []string{"localhost:2379"})
		assert.NoError(t, etcd.probe(context.Background()))
	})

	t.Run("OneReachableMemberIsEnough", func(t *testing.T) {
		etcd := newEtcdMeta(&oneUpEtcd{upEndpoint: "b:2379"}, []string{"a:2379", "b:2379"})
		assert.NoError(t, etcd.probe(context.Background()))
	})

	t.Run("UnreachableNamesEveryEndpoint", func(t *testing.T) {
		etcd := newEtcdMeta(&downEtcd{}, []string{"a:2379", "b:2379"})

		err := etcd.probe(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "a:2379")
		assert.Contains(t, err.Error(), "b:2379")
	})

	t.Run("SilentEndpointDoesNotBlock", func(t *testing.T) {
		etcd := newEtcdMeta(&blockingEtcd{}, []string{"127.0.0.1:2379"})
		etcd.timeout = 100 * time.Millisecond

		start := time.Now()
		err := etcd.probe(context.Background())
		require.Error(t, err)
		assert.Less(t, time.Since(start), time.Second)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Contains(t, err.Error(), "127.0.0.1:2379")
	})

	t.Run("NoEndpoint", func(t *testing.T) {
		etcd := newEtcdMeta(&fakeKV{}, nil)
		assert.ErrorContains(t, etcd.probe(context.Background()), "no etcd endpoint configured")
	})
}

func TestEtcdMeta_GetPrefix(t *testing.T) {
	t.Run("SilentEndpointDoesNotBlock", func(t *testing.T) {
		etcd := newEtcdMeta(&blockingEtcd{}, []string{"127.0.0.1:2379"})
		etcd.timeout = 100 * time.Millisecond

		start := time.Now()
		_, err := etcd.getPrefix(context.Background(), "by-dev/meta/field-index/1/")
		require.Error(t, err)
		assert.Less(t, time.Since(start), time.Second)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Contains(t, err.Error(), "127.0.0.1:2379")
		assert.Contains(t, err.Error(), "by-dev/meta/field-index/1/")
	})
}

// TestEtcdMeta_ProbeWithRealClient pins the premise of the fix: clientv3.New
// does not dial, so a client built against an address nothing listens on is
// returned without error and only the probe reports the endpoint as unreachable.
func TestEtcdMeta_ProbeWithRealClient(t *testing.T) {
	// Port 1 is privileged and never an etcd, so the connection is refused.
	const endpoint = "127.0.0.1:1"

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err, "clientv3.New must not dial, otherwise the probe would be redundant")
	defer cli.Close()

	etcd := newEtcdMeta(cli, []string{endpoint})
	etcd.timeout = time.Second

	start := time.Now()
	err = etcd.probe(context.Background())
	require.Error(t, err)
	assert.Less(t, time.Since(start), 10*time.Second)
	assert.Contains(t, err.Error(), endpoint)
}
