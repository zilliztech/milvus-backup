package storage

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscred "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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

	// S3 DeleteObjects takes at most 1000 keys per request, so one
	// DeleteObjects call is one request.
	_deleteObjectsBatchSize = 1000
)

// uploads larger than this are split by the SDK's transfer manager into parts
// of this size, matching the part size the minio client used for large writes.
// A var only so tests can shrink it; production code must not write it.
var _uploadMultipartThreshold = int64(64 * _MiB)

var _ Client = (*S3Client)(nil)
var _ batchDeleter = (*S3Client)(nil)

// S3Client implements Client against any S3-compatible API — AWS S3, MinIO,
// self-hosted gateways, and the vendor endpoints of aliyun, tencent and hwc —
// using aws-sdk-go-v2.
type S3Client struct {
	cfg Config

	logger *zap.Logger

	cli *s3.Client
	tm  *transfermanager.Client
}

// newS3Client builds the client for the aws/s3/minio providers. Bucket
// resolution follows the rule the minio client applied: AWS and GCS endpoints
// resolve by virtual host, every other endpoint by path.
func newS3Client(ctx context.Context, cfg Config) (*S3Client, error) {
	var provider aws.CredentialsProvider
	switch cfg.Credential.Type {
	case IAM:
		p, err := iamCredProvider(cfg.Credential)
		if err != nil {
			return nil, err
		}
		provider = p
	case Static:
		provider = staticS3CredProvider(cfg.Credential)
	case AWSCredProvider:
		if cfg.Credential.AWSCredProvider == nil {
			return nil, fmt.Errorf("storage: s3 credential type %s requires a provider", cfg.Credential.Type.String())
		}
		provider = cfg.Credential.AWSCredProvider
	default:
		return nil, fmt.Errorf("storage: s3 unsupported credential type: %s", cfg.Credential.Type.String())
	}

	return newS3ClientWithCreds(ctx, cfg, provider, !isVirtualHostEndpoint(cfg.Endpoint))
}

// newS3ClientWithCreds is the constructor the vendor adapters delegate to. The
// vendor constructors resolve their own credentials and pin the bucket
// resolution style; the TLS/endpoint/checksum options and the region probe are
// shared here. optFns exist for tests to swap the HTTP client.
func newS3ClientWithCreds(ctx context.Context, cfg Config, provider aws.CredentialsProvider, usePathStyle bool, optFns ...func(*s3.Options)) (*S3Client, error) {
	opts, err := s3Options(cfg, provider, usePathStyle)
	if err != nil {
		return nil, err
	}
	for _, fn := range optFns {
		fn(&opts)
	}

	// The S3 API needs a signing region up front. The minio client discovered
	// it with GetBucketLocation for a config that leaves region empty; do the
	// same probe once, here.
	if opts.Region == "" {
		region, err := discoverRegion(ctx, cfg, opts)
		if err != nil {
			return nil, err
		}
		opts.Region = region
		cfg.Region = region
	}

	logger := log.L().With(zap.String("provider", cfg.Provider), zap.String("endpoint", cfg.Endpoint))
	cli := s3.New(opts)
	tm := transfermanager.New(cli, func(o *transfermanager.Options) {
		o.PartSizeBytes = _uploadMultipartThreshold
		o.MultipartUploadThreshold = _uploadMultipartThreshold
		// Old MinIO, Ceph and OSS releases reject the checksum headers a
		// default-on transfer manager would add.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})

	return &S3Client{cfg: cfg, logger: logger, cli: cli, tm: tm}, nil
}

func s3Options(cfg Config, provider aws.CredentialsProvider, usePathStyle bool) (s3.Options, error) {
	opts := s3.Options{
		Region:      cfg.Region,
		Credentials: provider,
		// Old MinIO, Ceph and OSS releases reject the checksum headers the
		// SDK adds by default, so only send and validate them when the
		// backend asks for it.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	if cfg.Endpoint != "" {
		scheme := "https"
		if !cfg.UseSSL {
			scheme = "http"
		}
		opts.BaseEndpoint = aws.String(scheme + "://" + cfg.Endpoint)
	}
	if cfg.CACertPath != "" {
		// The SDK's TLS configuration is reachable only through the transport,
		// so trusting a private CA means supplying one.
		tr, err := tlsTransport(cfg.CACertPath)
		if err != nil {
			return s3.Options{}, fmt.Errorf("storage: %s create tls transport: %w", cfg.Provider, err)
		}
		opts.HTTPClient = &http.Client{Transport: tr}
	}
	if usePathStyle {
		opts.UsePathStyle = true
	}

	// The SDK replaced the classic Content-MD5 of DeleteObjects with an
	// x-amz-checksum-crc32 header, which old MinIO releases reject with
	// MissingContentMD5. Restore the header so batch delete works across
	// backends.
	opts.APIOptions = append(opts.APIOptions, func(stack *smithymiddleware.Stack) error {
		return stack.Build.Add(contentMD5{}, smithymiddleware.After)
	})

	return opts, nil
}

// contentMD5 sets the Content-MD5 header on DeleteObjects requests. The body
// is the serialized XML by the time Build runs, small enough (≤1000 keys) to
// hash in one pass; rewinding it keeps the sender and any retry reading the
// same bytes.
type contentMD5 struct{}

func (contentMD5) ID() string { return "milvus-backup:ContentMD5" }

func (contentMD5) HandleBuild(ctx context.Context, in smithymiddleware.BuildInput, next smithymiddleware.BuildHandler) (smithymiddleware.BuildOutput, smithymiddleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if ok && req.Method == http.MethodPost && req.URL.Query().Has("delete") && req.Header.Get("Content-MD5") == "" {
		body, err := io.ReadAll(req.GetStream())
		if err != nil {
			return smithymiddleware.BuildOutput{}, smithymiddleware.Metadata{}, fmt.Errorf("storage: s3 content md5 read body: %w", err)
		}
		if err := req.RewindStream(); err != nil {
			return smithymiddleware.BuildOutput{}, smithymiddleware.Metadata{}, fmt.Errorf("storage: s3 content md5 rewind body: %w", err)
		}

		sum := md5.Sum(body)
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
	}

	return next.HandleBuild(ctx, in)
}

// isVirtualHostEndpoint mirrors the bucket-lookup rule the minio client
// applied by default: only AWS and GCS endpoints resolve buckets by virtual
// host, everything else (MinIO, Ceph, gateways) by path.
func isVirtualHostEndpoint(endpoint string) bool {
	host := endpoint
	if h, _, err := net.SplitHostPort(endpoint); err == nil {
		host = h
	}
	return strings.Contains(host, ".amazonaws.com") || strings.Contains(host, ".googleapis.com")
}

// staticS3CredProvider turns a static key pair into the SDK provider. Empty
// keys mean anonymous access, like the minio client skipping the signature.
func staticS3CredProvider(cred Credential) aws.CredentialsProvider {
	if cred.AK == "" && cred.SK == "" {
		return aws.AnonymousCredentials{}
	}
	return awscred.NewStaticCredentialsProvider(cred.AK, cred.SK, cred.Token)
}

// iamCredProvider hands the request to the SDK's default chain: environment,
// shared config files, then the instance or pod role — the chain the minio
// client's IAM credentials walked. IAMEndpoint overrides the EC2 metadata
// service endpoint when set.
func iamCredProvider(cred Credential) (aws.CredentialsProvider, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error
	if cred.IAMEndpoint != "" {
		loadOpts = append(loadOpts, awsconfig.WithEC2IMDSEndpoint(cred.IAMEndpoint))
	}
	acfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("storage: s3 load default iam config: %w", err)
	}

	return acfg.Credentials, nil
}

// discoverRegion resolves the signing region for a bucket whose config leaves
// region empty, like the minio client's GetBucketLocation call. A bucket that
// does not exist yet (NewBackupStorage creates it after the client) or an
// unauthorized probe cannot name a region, so those resolve to us-east-1,
// matching the minio client's fallback; other probe failures fail construction.
func discoverRegion(ctx context.Context, cfg Config, opts s3.Options) (string, error) {
	opts.Region = "us-east-1"
	probe := s3.New(opts)

	out, err := probe.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(cfg.Bucket)})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			switch ae.ErrorCode() {
			case "NoSuchBucket", "AccessDenied", "AuthorizationHeaderMalformed", "InvalidRegion":
				return "us-east-1", nil
			}
		}
		return "", fmt.Errorf("storage: %s get bucket location: %w", cfg.Provider, err)
	}

	// The location XML is empty for us-east-1, and legacy buckets report 'EU'.
	region := string(out.LocationConstraint)
	if region == "" || region == "EU" {
		return "us-east-1", nil
	}

	return region, nil
}

// s3Backed is the capability CopyObject probes in the copy source: a client
// served by the shared S3 adapter, exposing the config a server-side copy
// needs (source bucket, provider). *S3Client satisfies it directly; the GCP
// wrapper re-declares it by hand because embedding the Client interface
// hides every method outside Client's own set.
type s3Backed interface {
	Client
	s3Cfg() Config
}

func (c *S3Client) s3Cfg() Config { return c.cfg }

func (c *S3Client) Config() Config { return c.cfg }

func (c *S3Client) multipartCopyThreshold() int64 {
	if c.cfg.MultipartCopyThresholdMiB > 0 {
		return c.cfg.MultipartCopyThresholdMiB * _MiB
	}
	return 500 * _MiB
}

func (c *S3Client) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(s3Backed)
	if !ok {
		return fmt.Errorf("storage: s3 copy object only supports an s3-compatible source client")
	}
	srcCfg := srcCli.s3Cfg()

	// gcp does not support multipart copy
	threshold := c.multipartCopyThreshold()
	if i.SrcAttr.Length >= threshold && srcCfg.Provider != cfg.ProviderGCP {
		c.logger.Debug("copy object by multipart", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
		return c.multiPartCopy(ctx, srcCfg, i)
	}

	c.logger.Debug("copy object by single part", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
	return c.copyObject(ctx, srcCfg, i)
}

// copySource renders the x-amz-copy-source value: bucket and key joined by a
// slash, percent-escaped per path segment but with the separators kept.
func copySource(bucket, key string) string {
	u := url.URL{Path: bucket + "/" + key}
	return u.EscapedPath()
}

func (c *S3Client) copyObject(ctx context.Context, srcCfg Config, i CopyObjectInput) error {
	return retry.Do(ctx, func() error {
		// The SDK detects the S3 "200 OK with an embedded <Error>" failure
		// mode itself for every S3 operation and returns it as an error here.
		_, err := c.cli.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(c.cfg.Bucket),
			Key:        aws.String(i.DestKey),
			CopySource: aws.String(copySource(srcCfg.Bucket, i.SrcAttr.Key)),
		})
		if err != nil {
			return fmt.Errorf("storage: %s copy from %s / %s to %s / %s: %w", c.cfg.Provider, srcCfg.Bucket, i.SrcAttr.Key, c.cfg.Bucket, i.DestKey, err)
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

func (c *S3Client) copyPart(ctx context.Context, i copyPartInput) (types.CompletedPart, error) {
	var completed types.CompletedPart

	c.logger.Debug("copy part", zap.Any("input", i))
	err := retry.Do(ctx, func() error {
		out, err := c.cli.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:          aws.String(c.cfg.Bucket),
			Key:             aws.String(i.DestKey),
			CopySource:      aws.String(copySource(i.SrcBucket, i.SrcKey)),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", i.Part.Offset, i.Part.Offset+i.Part.Size-1)),
			PartNumber:      aws.Int32(int32(i.Part.Index)),
			UploadId:        aws.String(i.UploadID),
		})
		if err != nil {
			return fmt.Errorf("storage: %s copy part: %w", c.cfg.Provider, err)
		}

		etag := ""
		if out.CopyPartResult != nil {
			etag = aws.ToString(out.CopyPartResult.ETag)
		}
		if etag == "" {
			// A 200 answer without an ETag carried the failure in its body in a
			// form the SDK could not decode; retry rather than complete with a
			// part that the backend would reject.
			return fmt.Errorf("storage: %s copy part %d: missing etag", c.cfg.Provider, i.Part.Index)
		}

		completed = types.CompletedPart{PartNumber: aws.Int32(int32(i.Part.Index)), ETag: aws.String(etag)}

		return nil
	})

	return completed, err
}

// abortMultipartUpload cleans up an unfinished multipart upload.
//
// The caller reaches here precisely when something went wrong, and the most common
// reason is that ctx was canceled -- the user canceled the backup, or a sibling copy
// failed and tore down the shared context. Aborting with that ctx would fail
// immediately and leave the already uploaded parts behind, billed until a bucket
// lifecycle rule reaps them. So detach from the caller's cancellation and give the
// abort its own deadline instead.
func (c *S3Client) abortMultipartUpload(ctx context.Context, destKey, uploadID string) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _abortTimeout)
	defer cancel()

	err := retry.Do(abortCtx, func() error {
		_, err := c.cli.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(c.cfg.Bucket),
			Key:      aws.String(destKey),
			UploadId: aws.String(uploadID),
		})
		if err != nil {
			return fmt.Errorf("storage: %s abort multipart upload: %w", c.cfg.Provider, err)
		}
		return nil
	}, retry.Attempts(_abortAttempts))
	if err != nil {
		c.logger.Error("abort multipart upload failed", zap.Error(err),
			zap.String("dest_key", destKey), zap.String("upload_id", uploadID))
	}
}

type sortableCompletedParts []types.CompletedPart

func (a sortableCompletedParts) Len() int      { return len(a) }
func (a sortableCompletedParts) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortableCompletedParts) Less(i, j int) bool {
	return aws.ToInt32(a[i].PartNumber) < aws.ToInt32(a[j].PartNumber)
}

func (c *S3Client) multiPartCopy(ctx context.Context, srcCfg Config, i CopyObjectInput) error {
	parts, err := splitIntoParts(i.SrcAttr.Length)
	if err != nil {
		return fmt.Errorf("storage: %s split into parts: %w", c.cfg.Provider, err)
	}

	out, err := c.cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(i.DestKey),
	})
	if err != nil {
		return fmt.Errorf("storage: %s new multipart upload: %w", c.cfg.Provider, err)
	}
	uploadID := aws.ToString(out.UploadId)

	// do not use if err := ...; err != nil { return err } because we need to abort the multipart upload when error
	defer func() {
		if err != nil {
			c.logger.Error("multi part copy failed, abort multipart upload", zap.Error(err), zap.String("upload_id", uploadID))
			c.abortMultipartUpload(ctx, i.DestKey, uploadID)
		}
	}()

	completedParts := make([]types.CompletedPart, 0, len(parts))
	var mu sync.Mutex
	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_maxCopyPartParallelism)
	for _, p := range parts {
		g.Go(func() error {
			input := newCopyPartInput(srcCfg, i.SrcAttr.Key, i.DestKey, uploadID, p)
			completePart, err := c.copyPart(subCtx, input)
			if err != nil {
				return fmt.Errorf("storage: %s copy part: %w", c.cfg.Provider, err)
			}
			mu.Lock()
			completedParts = append(completedParts, completePart)
			mu.Unlock()

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return fmt.Errorf("storage: %s wait for copy part: %w", c.cfg.Provider, err)
	}

	sort.Sort(sortableCompletedParts(completedParts))
	_, err = c.cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(c.cfg.Bucket),
		Key:             aws.String(i.DestKey),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	if err != nil {
		return fmt.Errorf("storage: %s complete multipart upload: %w", c.cfg.Provider, err)
	}

	return nil
}

func (c *S3Client) HeadObject(ctx context.Context, key string) (ObjectAttr, error) {
	out, err := c.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: %s head object: %w", c.cfg.Provider, err)
	}

	// S3 does not echo the key back on HEAD, so the attribute carries the
	// key that was asked for.
	return ObjectAttr{Key: key, Length: aws.ToInt64(out.ContentLength)}, nil
}

func (c *S3Client) GetObject(ctx context.Context, key string) (*Object, error) {
	out, err := c.cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object: %w", c.cfg.Provider, err)
	}

	return &Object{Length: aws.ToInt64(out.ContentLength), Body: out.Body}, nil
}

func (c *S3Client) UploadObject(ctx context.Context, i UploadObjectInput) error {
	var contentLength *int64
	if i.Size > 0 {
		contentLength = aws.Int64(i.Size)
	}

	// The transfer manager picks the request shape: a known size under the
	// multipart threshold is one PutObject, an unknown or large stream is
	// buffered into fixed-size parts, uploaded in parallel and aborted whole
	// on failure.
	_, err := c.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:        aws.String(c.cfg.Bucket),
		Key:           aws.String(i.Key),
		Body:          i.Body,
		ContentLength: contentLength,
	})
	if err != nil {
		return fmt.Errorf("storage: %s upload object: %w", c.cfg.Provider, err)
	}

	return nil
}

func (c *S3Client) DeleteObject(ctx context.Context, key string) error {
	// The SDK accepts any 2xx answer and rewrites a 200 OK with an embedded
	// <Error> body into an error, so no per-backend salvage is needed here.
	return retry.Do(ctx, func() error {
		_, err := c.cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(c.cfg.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("storage: %s delete object: %w", c.cfg.Provider, err)
		}

		return nil
	})
}

func (c *S3Client) DeleteObjectsBatchSize() int { return _deleteObjectsBatchSize }

// DeleteObjects deletes a batch of keys with one S3 DeleteObjects request. A
// request-level failure (the whole batch rejected) and per-key failures inside
// a 200 response both arrive on the returned error.
func (c *S3Client) DeleteObjects(ctx context.Context, keys []string) error {
	return retry.Do(ctx, func() error {
		err := c.deleteObjects(ctx, keys)
		if isBatchDeleteUnsupported(err) {
			// A gateway without multi-object delete will never learn it, so
			// skip the retries and tell DeleteWithCallback to degrade to
			// per-key deletion for the rest of the run.
			return retry.Unrecoverable(fmt.Errorf("%w: %w", errBatchDeleteUnsupported, err))
		}
		return err
	})
}

func (c *S3Client) deleteObjects(ctx context.Context, keys []string) error {
	ids := make([]types.ObjectIdentifier, len(keys))
	for idx, key := range keys {
		ids[idx] = types.ObjectIdentifier{Key: aws.String(key)}
	}

	out, err := c.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(c.cfg.Bucket),
		Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return fmt.Errorf("storage: %s delete objects: %w", c.cfg.Provider, err)
	}

	var errs []error
	for idx := range out.Errors {
		e := &out.Errors[idx]
		// The per-key error shape is a plain struct, not an error, so re-emit
		// it as an API error: that keeps the code reachable for the
		// NotImplemented degrade check below and reads well in the join.
		errs = append(errs, fmt.Errorf("storage: %s delete object %s: %w", c.cfg.Provider, aws.ToString(e.Key),
			&smithy.GenericAPIError{Code: aws.ToString(e.Code), Message: aws.ToString(e.Message)}))
	}

	return errors.Join(errs...)
}

// isBatchDeleteUnsupported reports whether err is a NotImplemented rejection
// of the multi-object delete request itself. Per-key failures decoded from a
// 200 response surface as plain API errors with only a code, so they trip the
// code check, while per-key failures of any other kind never read as
// "unsupported".
func isBatchDeleteUnsupported(err error) bool {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotImplemented {
		return true
	}

	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "NotImplemented" {
		return true
	}

	return false
}

func isNoSuchBucket(err error) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket"
}

// NewObjectIter lists objects in the bucket under prefix. The listing is
// driven synchronously by the range: each iteration pulls the next page when
// the previous one is consumed, so ending the range early — break, return, or
// panic — simply stops fetching and releases nothing, because no background
// goroutine ever existed. A non-recursive listing yields directory prefixes
// after the objects of each page, like the minio client did: some callers
// (Exist) only need to know that anything lives under a prefix, directories
// included.
func (c *S3Client) NewObjectIter(ctx context.Context, prefix string, recursive bool) iter.Seq2[ObjectAttr, error] {
	return func(yield func(ObjectAttr, error) bool) {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(c.cfg.Bucket),
			Prefix: aws.String(prefix),
		}
		if !recursive {
			input.Delimiter = aws.String("/")
		}

		pager := s3.NewListObjectsV2Paginator(c.cli, input)
		for pager.HasMorePages() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				yield(ObjectAttr{}, fmt.Errorf("storage: %s list prefix: %w", c.cfg.Provider, err))
				return
			}

			for _, obj := range page.Contents {
				if !yield(ObjectAttr{Key: aws.ToString(obj.Key), Length: aws.ToInt64(obj.Size)}, nil) {
					return
				}
			}
			for _, cp := range page.CommonPrefixes {
				if !yield(ObjectAttr{Key: aws.ToString(cp.Prefix)}, nil) {
					return
				}
			}
		}
	}
}

// BucketExist checks if the bucket exists by listing a single object.
// We use ListObjects instead of HeadBucket to minimize
// required S3 permissions. MaxKeys=1 avoids iterating the entire bucket,
// which can timeout on large buckets, while preserving caller context
// propagation.
func (c *S3Client) BucketExist(ctx context.Context, prefix string) (bool, error) {
	_, err := c.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.cfg.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		if isNoSuchBucket(err) {
			return false, nil
		}
		return false, fmt.Errorf("storage: %s list objects: %w", c.cfg.Provider, err)
	}

	return true, nil
}

func (c *S3Client) CreateBucket(ctx context.Context) error {
	input := &s3.CreateBucketInput{Bucket: aws.String(c.cfg.Bucket)}
	// us-east-1 rejects a location constraint; every other region requires one.
	if region := c.cfg.Region; region != "" && region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}

	if _, err := c.cli.CreateBucket(ctx, input); err != nil {
		return fmt.Errorf("storage: %s create bucket: %w", c.cfg.Provider, err)
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
