package storage

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/internal/cfg"
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

	// aborting an unfinished multipart upload runs on a context detached from the
	// caller's, so it needs its own bound to avoid hanging the copy path
	_abortTimeout  = 30 * time.Second
	_abortAttempts = 3
)

var _ Client = (*MinioClient)(nil)
var _ batchDeleter = (*MinioClient)(nil)

func newMinioClient(storeCfg Config) (*MinioClient, error) {
	opts := minio.Options{Secure: storeCfg.UseSSL, Region: storeCfg.Region}
	switch storeCfg.Credential.Type {
	case IAM:
		opts.Creds = credentials.NewIAM(storeCfg.Credential.IAMEndpoint)
	case Static:
		opts.Creds = credentials.NewStaticV4(storeCfg.Credential.AK, storeCfg.Credential.SK, storeCfg.Credential.Token)
	case MinioCredProvider:
		opts.Creds = credentials.New(storeCfg.Credential.MinioCredProvider)
	default:
		return nil, fmt.Errorf("storage: minio unsupported credential type: %s", storeCfg.Credential.Type.String())
	}

	return newInternalMinio(storeCfg, &opts)
}

func newInternalMinio(storeCfg Config, opts *minio.Options) (*MinioClient, error) {
	cli, err := minio.New(storeCfg.Endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create %s client: %w", storeCfg.Provider, err)
	}

	core, err := minio.NewCore(storeCfg.Endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create %s client: %w", storeCfg.Provider, err)
	}

	logger := log.L().With(zap.String("provider", storeCfg.Provider), zap.String("endpoint", storeCfg.Endpoint))

	return &MinioClient{cfg: storeCfg, cli: cli, core: core, logger: logger}, nil
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

// minioBacked is the capability CopyObject probes in the copy source: a
// client served by minio-go, exposing the config a server-side copy needs
// (source bucket, provider). *MinioClient satisfies it directly; the GCP
// wrapper re-declares it by hand because embedding the Client interface
// hides every method outside Client's own set.
type minioBacked interface {
	Client
	minioCfg() Config
}

func (m *MinioClient) minioCfg() Config { return m.cfg }

func (m *MinioClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(minioBacked)
	if !ok {
		return fmt.Errorf("storage: minio copy object only supports a minio source client")
	}
	srcCfg := srcCli.minioCfg()

	// gcp does not support multipart copy
	threshold := m.multipartCopyThreshold()
	if i.SrcAttr.Length >= threshold && srcCfg.Provider != cfg.ProviderGCP {
		m.logger.Debug("copy object by multipart", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
		return m.multiPartCopy(ctx, srcCfg, i)
	}

	m.logger.Debug("copy object by single part", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
	return m.copyObject(ctx, srcCfg, i)
}

func (m *MinioClient) multipartCopyThreshold() int64 {
	if m.cfg.MultipartCopyThresholdMiB > 0 {
		return m.cfg.MultipartCopyThresholdMiB * _MiB
	}
	return 500 * _MiB
}

func (m *MinioClient) copyObject(ctx context.Context, srcCfg Config, i CopyObjectInput) error {
	dst := minio.CopyDestOptions{Bucket: m.cfg.Bucket, Object: i.DestKey}
	src := minio.CopySrcOptions{Bucket: srcCfg.Bucket, Object: i.SrcAttr.Key}
	return retry.Do(ctx, func() error {
		info, err := m.cli.CopyObject(ctx, dst, src)
		if err != nil {
			return fmt.Errorf("storage: %s copy from %s / %s to %s / %s: %w", m.cfg.Provider, srcCfg.Bucket, i.SrcAttr.Key, m.cfg.Bucket, i.DestKey, err)
		}

		// S3 documents that a failed CopyObject can still answer 200 OK with an
		// <Error> document embedded in the body. minio-go's CopyObject decodes that
		// body into a field-less copyObjectResult and reports success, leaving an
		// empty ETag for a copy that never happened. Treat the empty ETag as a
		// failure so the retry kicks in instead of silently dropping the object.
		if info.ETag == "" {
			return fmt.Errorf("storage: %s copy from %s / %s to %s / %s got empty etag, "+
				"the copy likely failed with an embedded error", m.cfg.Provider,
				srcCfg.Bucket, i.SrcAttr.Key, m.cfg.Bucket, i.DestKey)
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

func newCopyPartInput(srcCfg Config, srcKey, destKey, uploadID string, part part) copyPartInput {
	return copyPartInput{
		SrcBucket: srcCfg.Bucket,
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
			return fmt.Errorf("storage: %s copy part: %w", m.cfg.Provider, err)
		}

		return nil
	})

	return output, err
}

// abortMultipartUpload cleans up an unfinished multipart upload.
//
// The caller reaches here precisely when something went wrong, and the most common
// reason is that ctx was canceled -- the user canceled the backup, or a sibling copy
// failed and tore down the shared context. Aborting with that ctx would fail
// immediately and leave the already uploaded parts behind, billed until a bucket
// lifecycle rule reaps them. So detach from the caller's cancellation and give the
// abort its own deadline instead.
func (m *MinioClient) abortMultipartUpload(ctx context.Context, destKey, uploadID string) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _abortTimeout)
	defer cancel()

	err := retry.Do(abortCtx, func() error {
		if err := m.core.AbortMultipartUpload(abortCtx, m.cfg.Bucket, destKey, uploadID); err != nil {
			return fmt.Errorf("storage: %s abort multipart upload: %w", m.cfg.Provider, err)
		}
		return nil
	}, retry.Attempts(_abortAttempts))
	if err != nil {
		m.logger.Error("abort multipart upload failed", zap.Error(err),
			zap.String("dest_key", destKey), zap.String("upload_id", uploadID))
	}
}

type sortableCompletedParts []minio.CompletePart

func (a sortableCompletedParts) Len() int           { return len(a) }
func (a sortableCompletedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortableCompletedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

func (m *MinioClient) multiPartCopy(ctx context.Context, srcCfg Config, i CopyObjectInput) error {
	parts, err := splitIntoParts(i.SrcAttr.Length)
	if err != nil {
		return fmt.Errorf("storage: %s split into parts: %w", m.cfg.Provider, err)
	}

	uploadID, err := m.core.NewMultipartUpload(ctx, m.cfg.Bucket, i.DestKey, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("storage: %s new multipart upload: %w", m.cfg.Provider, err)
	}

	// do not use if err := ...; err != nil { return err } because we need to abort the multipart upload when error
	defer func() {
		if err != nil {
			m.logger.Error("multi part copy failed, abort multipart upload", zap.Error(err), zap.String("upload_id", uploadID))
			m.abortMultipartUpload(ctx, i.DestKey, uploadID)
		}
	}()

	completedParts := make([]minio.CompletePart, 0, len(parts))
	var mu sync.Mutex
	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_maxCopyPartParallelism)
	for _, p := range parts {
		g.Go(func() error {
			input := newCopyPartInput(srcCfg, i.SrcAttr.Key, i.DestKey, uploadID, p)
			completePart, err := m.copyPart(subCtx, input)
			if err != nil {
				return fmt.Errorf("storage: %s copy part: %w", m.cfg.Provider, err)
			}
			mu.Lock()
			completedParts = append(completedParts, completePart)
			mu.Unlock()

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return fmt.Errorf("storage: %s wait for copy part: %w", m.cfg.Provider, err)
	}

	sort.Sort(sortableCompletedParts(completedParts))
	_, err = m.core.CompleteMultipartUpload(ctx, m.cfg.Bucket, i.DestKey, uploadID, completedParts, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("storage: %s complete multipart upload: %w", m.cfg.Provider, err)
	}

	return nil
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
			return fmt.Errorf("storage: %s delete object: %w", m.cfg.Provider, err)
		}

		return nil
	})
}

// S3 DeleteObjects takes at most 1000 keys per request; minio-go chunks to
// the same ceiling, so one DeleteObjects call is one request.
const _deleteObjectsBatchSize = 1000

func (m *MinioClient) DeleteObjectsBatchSize() int { return _deleteObjectsBatchSize }

// DeleteObjects deletes a batch of keys with one S3 DeleteObjects request,
// via minio-go's RemoveObjects. A request-level failure (the whole batch
// rejected) and per-key failures inside a 200 response both arrive on
// minio-go's error channel and are joined into the returned error.
func (m *MinioClient) DeleteObjects(ctx context.Context, keys []string) error {
	return retry.Do(ctx, func() error {
		err := m.deleteObjects(ctx, keys)
		if isBatchDeleteUnsupported(err) {
			// A gateway without multi-object delete will never learn it, so
			// skip the retries and tell DeleteWithCallback to degrade to
			// per-key deletion for the rest of the run.
			return retry.Unrecoverable(fmt.Errorf("%w: %w", errBatchDeleteUnsupported, err))
		}
		return err
	})
}

func (m *MinioClient) deleteObjects(ctx context.Context, keys []string) error {
	// Buffered and closed up front: minio-go's reader goroutine can never
	// block on a sender that already returned.
	objectsCh := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objectsCh <- minio.ObjectInfo{Key: key}
	}
	close(objectsCh)

	var errs []error
	for removeErr := range m.cli.RemoveObjects(ctx, m.cfg.Bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		errs = append(errs, fmt.Errorf("storage: %s delete object %s: %w", m.cfg.Provider, removeErr.ObjectName, removeErr.Err))
	}

	return errors.Join(errs...)
}

// isBatchDeleteUnsupported reports whether err is a NotImplemented rejection
// of the multi-object delete request itself. Per-key failures decoded from a
// 200 response carry only a Code and Message, no StatusCode, so they do not
// trip the StatusCode check.
func isBatchDeleteUnsupported(err error) bool {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		return resp.StatusCode == http.StatusNotImplemented || resp.Code == "NotImplemented"
	}
	return false
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

type part struct {
	Index  int
	Offset int64
	Size   int64
}

func splitIntoParts(totalSize int64) ([]part, error) {
	if totalSize <= _minPartSize {
		return nil, fmt.Errorf("storage: total size %d is not greater than min part size %d", totalSize, _minPartSize)
	}

	if totalSize > _maxMultiCopySize {
		return nil, fmt.Errorf("storage: total size %d is greater than max copy size %d", totalSize, _maxMultiCopySize)
	}

	ceilDiv := func(a, b int64) int64 { return (a + b - 1) / b }
	partSize := max(_minPartSize, ceilDiv(totalSize, _maxParts))

	var parts []part
	var offset int64
	index := 1
	for offset < totalSize {
		remaining := totalSize - offset
		size := min(remaining, partSize)
		parts = append(parts, part{Index: index, Offset: offset, Size: size})
		offset += size
		index++
	}

	return parts, nil
}
