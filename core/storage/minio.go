package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

var _ Client = (*MinioClient)(nil)

func newMinioClient(cfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL}
	switch cfg.Credential.Type {
	case IAM:
		opts.Creds = credentials.NewIAM(cfg.Credential.IAMEndpoint)
	case Static:
		opts.Creds = credentials.NewStaticV4(cfg.Credential.AK, cfg.Credential.SK, cfg.Credential.Token)
	case MinioCredProvider:
		opts.Creds = credentials.New(cfg.Credential.MinioCredProvider)
	default:
		return nil, fmt.Errorf("storage: minio unsupported credential type %s", cfg.Credential.Type.String())
	}

	cli, err := minio.New(cfg.Endpoint, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create minio client %w", err)
	}

	return &MinioClient{cfg: cfg, cli: cli}, nil
}

type MinioClient struct {
	cfg Config

	cli *minio.Client
}

func (m *MinioClient) Config() Config {
	return m.cfg
}

func (m *MinioClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(*MinioClient)
	if !ok {
		return fmt.Errorf("storage: minio copy object only support minio client")
	}

	dst := minio.CopyDestOptions{Bucket: m.cfg.Bucket, Object: i.DestKey}
	src := minio.CopySrcOptions{Bucket: srcCli.cfg.Bucket, Object: i.SrcKey}
	return retry.Do(ctx, func() error {
		if _, err := m.cli.CopyObject(ctx, dst, src); err != nil {
			return fmt.Errorf("storage: %s copy from %s / %s  to %s / %s %w", m.cfg.Provider, i.SrcKey, m.cfg.Bucket, m.cfg.Bucket, i.DestKey, err)
		}
		return nil
	})
}

func (m *MinioClient) HeadObject(ctx context.Context, key string) (ObjectAttr, error) {
	attr, err := m.cli.StatObject(ctx, m.cfg.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: %s head object %w", m.cfg.Provider, err)
	}

	return ObjectAttr{Key: attr.Key, Length: attr.Size}, nil
}

func (m *MinioClient) GetObject(ctx context.Context, key string) (*Object, error) {
	obj, err := m.cli.GetObject(ctx, m.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object %w", m.cfg.Provider, err)
	}

	attr, err := obj.Stat()
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object attr %w", m.cfg.Provider, err)
	}

	return &Object{Length: attr.Size, Body: obj}, nil
}

func (m *MinioClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	size := int64(-1)
	if i.Size > 0 {
		size = i.Size
	}
	if _, err := m.cli.PutObject(ctx, m.cfg.Bucket, i.Key, i.Body, size, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("storage: %s upload object %w", m.cfg.Provider, err)
	}

	return nil
}

func (m *MinioClient) DeleteObject(ctx context.Context, key string) error {
	return retry.Do(ctx, func() error {
		if err := m.cli.RemoveObject(ctx, m.cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("storage: %s delete object %w", m.cfg.Provider, err)
		}

		return nil
	})
}

type MinioObjectIterator struct {
	cli *MinioClient

	objCh   <-chan minio.ObjectInfo
	nextObj minio.ObjectInfo
	hasNext bool
}

func newMinioObjectIterator(cli *MinioClient, objCh <-chan minio.ObjectInfo) (*MinioObjectIterator, error) {
	nextObj, ok := <-objCh
	if !ok {
		return &MinioObjectIterator{cli: cli, objCh: objCh, hasNext: false}, nil
	}

	if nextObj.Err != nil {
		return &MinioObjectIterator{cli: cli, objCh: objCh, hasNext: false}, fmt.Errorf("storage: %s list prefix %w", cli.cfg.Provider, nextObj.Err)
	}

	return &MinioObjectIterator{cli: cli, objCh: objCh, nextObj: nextObj, hasNext: true}, nil
}

func (m *MinioObjectIterator) HasNext() bool { return m.hasNext }

func (m *MinioObjectIterator) Next() (ObjectAttr, error) {
	curr := m.nextObj
	m.nextObj, m.hasNext = <-m.objCh

	if curr.Err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: %s list prefix %w", m.cli.cfg.Provider, curr.Err)
	}
	return ObjectAttr{Key: curr.Key, Length: curr.Size}, nil
}

func (m *MinioClient) ListPrefix(ctx context.Context, prefix string, recursive bool) (ObjectIterator, error) {
	opt := minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive}
	objCh := m.cli.ListObjects(ctx, m.cfg.Bucket, opt)
	return newMinioObjectIterator(m, objCh)
}

func (m *MinioClient) BucketExist(ctx context.Context, prefix string) (bool, error) {
	bucketExists := true
	var errs error
	for obj := range m.cli.ListObjects(ctx, m.cfg.Bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		if obj.Err != nil {
			if minio.ToErrorResponse(obj.Err).Code == "NoSuchBucket" {
				bucketExists = false
			} else {
				errs = errors.Join(errs, fmt.Errorf("storage: %s list objects %w", m.cfg.Provider, obj.Err))
			}
		}
	}

	return bucketExists, errs
}

func (m *MinioClient) CreateBucket(ctx context.Context) error {
	if err := m.cli.MakeBucket(ctx, m.cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("storage: %s create bucket %w", m.cfg.Provider, err)
	}

	return nil
}
