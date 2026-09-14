package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// _etcdRequestTimeout bounds every etcd request the backup makes. Milvus bounds
// its own etcd calls with etcd.requestTimeout, whose default is 10000ms, and
// there is no reason for this tool to wait longer than the server it reads from.
const _etcdRequestTimeout = 10 * time.Second

// etcdReader is the part of *clientv3.Client the backup uses: prefix reads for
// the Milvus metadata, and Status for the reachability probe.
type etcdReader interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
}

// etcdMeta reads Milvus metadata that the gRPC API does not expose. It carries
// the endpoints it was built from because clientv3 never reports which address a
// call was talking to, and an operator handed a bare "context deadline exceeded"
// has nothing to act on.
type etcdMeta struct {
	cli       etcdReader
	endpoints []string

	// timeout applies to a whole call, probe included, and is a field only so
	// that tests do not have to wait out the production default.
	timeout time.Duration
}

func newEtcdMeta(cli etcdReader, endpoints []string) *etcdMeta {
	return &etcdMeta{cli: cli, endpoints: endpoints, timeout: _etcdRequestTimeout}
}

// probe establishes that etcd answers before the backup starts copying data.
// clientv3.New does not dial unless the config carries grpc.WithBlock(), so
// construction succeeds against endpoints that do not exist and the failure
// surfaces at the first read -- which happens only after every collection has
// been copied. A blocking dial would not be enough either: it proves the TCP
// connection, not that the peer speaks etcd, and an address that accepts the
// connection and then never answers is exactly the reported failure. Status is
// the cheapest RPC that settles both.
//
// One reachable member is enough. The client fails over between endpoints, so a
// single down member of an otherwise healthy cluster must not fail a backup.
func (e *etcdMeta) probe(ctx context.Context) error {
	if len(e.endpoints) == 0 {
		return errors.New("backup: no etcd endpoint configured")
	}

	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	errs := make([]error, 0, len(e.endpoints))
	for _, endpoint := range e.endpoints {
		_, err := e.cli.Status(ctx, endpoint)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", endpoint, err))
	}

	return fmt.Errorf("backup: etcd is unreachable at %v: %w", e.endpoints, errors.Join(errs...))
}

// getPrefix reads every key under prefix. The deadline is the point of the
// wrapper: the task context has none, so an endpoint that accepts the connection
// but never answers blocks the read until the process is killed.
func (e *etcdMeta) getPrefix(ctx context.Context, prefix string) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	resp, err := e.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd get %s from %v: %w", prefix, e.endpoints, err)
	}

	return resp, nil
}
