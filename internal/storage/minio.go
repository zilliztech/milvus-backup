package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/retry"
)

const (
	_MiB = 1 << 20
	_GiB = 1 << 30
	_TiB = 1 << 40
)

const (
	// s3 doc say min part size is 5MB, but we use 10MB to avoid too many parts
	_minPartSize      int64 = 10 * _MiB
	_maxMultiCopySize int64 = 5 * _TiB
	_maxParts         int64 = 10000

	_maxCopyPartParallelism = 10
)

var _ Client = (*MinioClient)(nil)

func newMinioClient(cfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region}
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

	return newInternalMinio(cfg, &opts)
}

func newInternalMinio(cfg Config, opts *minio.Options) (*MinioClient, error) {
	cli, err := minio.New(cfg.Endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create %s client: %w", cfg.Provider, err)
	}

	core, err := minio.NewCore(cfg.Endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create %s client: %w", cfg.Provider, err)
	}

	logger := log.L().With(zap.String("provider", cfg.Provider), zap.String("endpoint", cfg.Endpoint))

	return &MinioClient{cfg: cfg, cli: cli, core: core, logger: logger}, nil
}

type MinioClient struct {
	cfg Config

	logger *zap.Logger

	cli  *minio.Client
	core *minio.Core // only for multipart copy
}

func (m *MinioClient) Config() Config {
	return m.cfg
}

func (m *MinioClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(*MinioClient)
	if !ok {
		return fmt.Errorf("storage: minio copy object only support minio client")
	}

	// gcp does not support multipart copy
	if i.SrcAttr.Length >= 500*_MiB && srcCli.cfg.Provider != paramtable.CloudProviderGCP {
		m.logger.Debug("copy object by multipart", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
		return m.multiPartCopy(ctx, srcCli, i)
	}

	m.logger.Debug("copy object by single part", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
	return m.copyObject(ctx, srcCli, i)
}

func (m *MinioClient) copyObject(ctx context.Context, srcCli *MinioClient, i CopyObjectInput) error {
	dst := minio.CopyDestOptions{Bucket: m.cfg.Bucket, Object: i.DestKey}
	src := minio.CopySrcOptions{Bucket: srcCli.cfg.Bucket, Object: i.SrcAttr.Key}
	return retry.Do(ctx, func() error {
		if _, err := m.cli.CopyObject(ctx, dst, src); err != nil {
			return fmt.Errorf("storage: %s copy from %s / %s  to %s / %s %w", m.cfg.Provider, srcCli.cfg.Bucket, i.SrcAttr.Key, m.cfg.Bucket, i.DestKey, err)
		}
		return nil
	})
}

type copyPartInput struct {
	SrcBucket string
	SrcKey    string

	DestKey  string
	UploadID string

	Part part
}

func newCopyPartInput(srcCli *MinioClient, srcKey, destKey, uploadID string, part part) copyPartInput {
	return copyPartInput{
		SrcBucket: srcCli.cfg.Bucket,
		SrcKey:    srcKey,

		DestKey:  destKey,
		UploadID: uploadID,

		Part: part,
	}
}

func (m *MinioClient) copyPart(ctx context.Context, i copyPartInput) (minio.CompletePart, error) {
	var output minio.CompletePart

	m.logger.Debug("copy part", zap.Any("input", i))
	err := retry.Do(ctx, func() error {
		var err error
		output, err = m.core.CopyObjectPart(ctx,
			i.SrcBucket,
			i.SrcKey,
			m.cfg.Bucket,
			i.DestKey,
			i.UploadID,
			i.Part.Index,
			i.Part.Offset,
			i.Part.Size,
			nil)
		if err != nil {
			return fmt.Errorf("storage: %s copy part %w", m.cfg.Provider, err)
		}

		return nil
	})

	return output, err
}

type sortableCompletedParts []minio.CompletePart

func (a sortableCompletedParts) Len() int           { return len(a) }
func (a sortableCompletedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortableCompletedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

func (m *MinioClient) multiPartCopy(ctx context.Context, srcCli *MinioClient, i CopyObjectInput) error {
	parts, err := splitIntoParts(i.SrcAttr.Length)
	if err != nil {
		return fmt.Errorf("storage: %s split into parts %w", m.cfg.Provider, err)
	}

	uploadID, err := m.core.NewMultipartUpload(ctx, m.cfg.Bucket, i.DestKey, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("storage: %s new multipart upload %w", m.cfg.Provider, err)
	}

	// do not use if err := ...; err != nil { return err } because we need to abort the multipart upload when error
	defer func() {
		if err != nil {
			m.logger.Error("multi part copy failed, abort multipart upload", zap.Error(err), zap.String("upload_id", uploadID))

			if err := m.core.AbortMultipartUpload(ctx, m.cfg.Bucket, i.DestKey, uploadID); err != nil {
				m.logger.Error("abort multipart upload failed", zap.Error(err))
			}
		}

	}()

	completedParts := make([]minio.CompletePart, 0, len(parts))
	var mu sync.Mutex
	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_maxCopyPartParallelism)
	for _, p := range parts {
		g.Go(func() error {
			input := newCopyPartInput(srcCli, i.SrcAttr.Key, i.DestKey, uploadID, p)
			completePart, err := m.copyPart(subCtx, input)
			if err != nil {
				return fmt.Errorf("storage: %s copy part %w", m.cfg.Provider, err)
			}
			mu.Lock()
			completedParts = append(completedParts, completePart)
			mu.Unlock()

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return fmt.Errorf("storage: %s wait for copy part %w", m.cfg.Provider, err)
	}

	sort.Sort(sortableCompletedParts(completedParts))
	_, err = m.core.CompleteMultipartUpload(ctx, m.cfg.Bucket, i.DestKey, uploadID, completedParts, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("storage: %s complete multipart upload %w", m.cfg.Provider, err)
	}

	return nil
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

// isDeleteSuccessful checks if the error from RemoveObject should be treated as success.
// Some S3-compatible storage returns 200 instead of 204 for successful deletion,
// minio SDK may treat this as an error, but 200 should be considered successful.
func isDeleteSuccessful(err error) bool {
	if err == nil {
		return true
	}
	return minio.ToErrorResponse(err).StatusCode == http.StatusOK
}

func (m *MinioClient) DeleteObject(ctx context.Context, key string) error {
	return retry.Do(ctx, func() error {
		if err := m.cli.RemoveObject(ctx, m.cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
			if isDeleteSuccessful(err) {
				return nil
			}
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

type part struct {
	Index  int
	Offset int64
	Size   int64
}

func splitIntoParts(totalSize int64) ([]part, error) {
	if totalSize <= _minPartSize {
		return nil, fmt.Errorf("storage: total size %d is less than min part size %d", totalSize, _minPartSize)
	}

	if totalSize > _maxMultiCopySize {
		return nil, fmt.Errorf("storage: total size %d is greater than max part size %d", totalSize, _maxMultiCopySize)
	}

	ceilDiv := func(a, b int64) int64 { return (a + b - 1) / b }
	partSize := max(_minPartSize, ceilDiv(totalSize, _maxParts))

	var parts []part
	var offset int64
	index := 1
	for offset < totalSize {
		remaining := totalSize - offset
		size := partSize
		if remaining < size {
			size = remaining
		}
		parts = append(parts, part{Index: index, Offset: offset, Size: size})
		offset += size
		index++
	}

	return parts, nil
}
