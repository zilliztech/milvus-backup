package storage

import (
	"context"
	"fmt"
	"iter"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/retry"
)

var _ Client = (*MinioClient)(nil)

// MinioClient implements Client against the GCS XML API through minio-go, the
// one backend that cannot ride the shared S3 adapter: its static-credential
// flow is legacy SigV2 and its bearer-token flow rewrites x-amz headers into
// x-goog ones (see gcp.go), neither of which aws-sdk-go-v2 offers. It is
// reached only through the gcpClient wrapper in gcp.go.
type MinioClient struct {
	cfg Config

	logger *zap.Logger

	cli *minio.Client
}

func newInternalMinio(storeCfg Config, opts *minio.Options) (*MinioClient, error) {
	if storeCfg.CACertPath != "" {
		// minio-go's TLS configuration is reachable only through the
		// transport, so trusting a private CA means supplying one.
		tr, err := tlsTransport(storeCfg.CACertPath)
		if err != nil {
			return nil, err
		}
		opts.Transport = tr
	}

	cli, err := minio.New(storeCfg.Endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create %s client: %w", storeCfg.Provider, err)
	}

	logger := log.L().With(zap.String("provider", storeCfg.Provider), zap.String("endpoint", storeCfg.Endpoint))

	return &MinioClient{cfg: storeCfg, cli: cli, logger: logger}, nil
}

func (m *MinioClient) Config() Config {
	return m.cfg
}

func (m *MinioClient) s3Cfg() Config { return m.cfg }

func (m *MinioClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(s3Backed)
	if !ok {
		return fmt.Errorf("storage: minio copy object only supports an s3-compatible source client")
	}
	srcCfg := srcCli.s3Cfg()

	// The GCS XML API has no multipart copy, so server-side copies are
	// single-part regardless of size; S3-compatible providers take that path
	// through the shared adapter in s3.go.
	m.logger.Debug("copy object by single part", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
	return m.copyObject(ctx, srcCfg, i)
}

func (m *MinioClient) copyObject(ctx context.Context, srcCfg Config, i CopyObjectInput) error {
	dst := minio.CopyDestOptions{Bucket: m.cfg.Bucket, Object: i.DestKey}
	src := minio.CopySrcOptions{Bucket: srcCfg.Bucket, Object: i.SrcAttr.Key}
	return retry.Do(ctx, func() error {
		// minio-go >= v7.4.0 detects the S3 "200 OK with an embedded <Error>"
		// failure mode itself (minio-go#2306) and returns it as an error here.
		if _, err := m.cli.CopyObject(ctx, dst, src); err != nil {
			return fmt.Errorf("storage: %s copy from %s / %s to %s / %s: %w", m.cfg.Provider, srcCfg.Bucket, i.SrcAttr.Key, m.cfg.Bucket, i.DestKey, err)
		}

		return nil
	})
}

func (m *MinioClient) HeadObject(ctx context.Context, key string) (ObjectAttr, error) {
	attr, err := m.cli.StatObject(ctx, m.cfg.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: %s head object: %w", m.cfg.Provider, err)
	}

	return ObjectAttr{Key: attr.Key, Length: attr.Size}, nil
}

func (m *MinioClient) GetObject(ctx context.Context, key string) (*Object, error) {
	obj, err := m.cli.GetObject(ctx, m.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object: %w", m.cfg.Provider, err)
	}

	attr, err := obj.Stat()
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object attr: %w", m.cfg.Provider, err)
	}

	return &Object{Length: attr.Size, Body: obj}, nil
}

func (m *MinioClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	size := int64(-1)
	if i.Size > 0 {
		size = i.Size
	}
	if _, err := m.cli.PutObject(ctx, m.cfg.Bucket, i.Key, i.Body, size, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("storage: %s upload object: %w", m.cfg.Provider, err)
	}

	return nil
}

func (m *MinioClient) DeleteObject(ctx context.Context, key string) error {
	return retry.Do(ctx, func() error {
		if err := m.cli.RemoveObject(ctx, m.cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("storage: %s delete object: %w", m.cfg.Provider, err)
		}

		return nil
	})
}

// NewObjectIter lists objects in the bucket under prefix. minio-go's
// ListObjects runs on a background goroutine that only returns once its
// context is canceled and the channel is drained, so the sequence cancels
// and drains in a defer. That defer runs whenever the range ends —
// exhausted, cut short by a break or return, or stopped by a listing error —
// and guarantees the goroutine exits; draining a fully-exhausted channel
// returns immediately.
func (m *MinioClient) NewObjectIter(ctx context.Context, prefix string, recursive bool) iter.Seq2[ObjectAttr, error] {
	return func(yield func(ObjectAttr, error) bool) {
		opt := minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive}
		subCtx, cancel := context.WithCancel(ctx)
		objCh := m.cli.ListObjects(subCtx, m.cfg.Bucket, opt)
		defer func() {
			cancel()
			for range objCh {
			}
		}()

		for item := range objCh {
			if item.Err != nil {
				yield(ObjectAttr{}, fmt.Errorf("storage: %s list prefix: %w", m.cfg.Provider, item.Err))
				return
			}
			if !yield(ObjectAttr{Key: item.Key, Length: item.Size}, nil) {
				return
			}
		}
	}
}

// BucketExist checks if the bucket exists by listing a single object.
// We use ListObjects instead of BucketExists (HEAD bucket) to minimize
// required S3 permissions. MaxKeys=1 avoids iterating the entire bucket,
// which can timeout on large buckets, while preserving caller context
// propagation.
func (m *MinioClient) BucketExist(ctx context.Context, prefix string) (bool, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	objCh := m.cli.ListObjects(subCtx, m.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:  prefix,
		MaxKeys: 1,
	})

	obj, ok := <-objCh
	if !ok {
		return true, nil
	}

	if obj.Err != nil {
		if minio.ToErrorResponse(obj.Err).Code == "NoSuchBucket" {
			return false, nil
		}
		return false, fmt.Errorf("storage: %s list objects: %w", m.cfg.Provider, obj.Err)
	}

	return true, nil
}

func (m *MinioClient) CreateBucket(ctx context.Context) error {
	if err := m.cli.MakeBucket(ctx, m.cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("storage: %s create bucket: %w", m.cfg.Provider, err)
	}

	return nil
}
