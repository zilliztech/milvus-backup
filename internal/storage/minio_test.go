package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSplitIntoParts(t *testing.T) {
	t.Run("TotalSizeLEMinPartSize", func(t *testing.T) {
		_, err := splitIntoParts(1024)
		assert.Error(t, err)

		_, err = splitIntoParts(_minPartSize)
		assert.Error(t, err)
	})

	t.Run("TotalSizeGTMinPartSize", func(t *testing.T) {
		// min + 1
		size := _minPartSize + 1
		parts, err := splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 1GB
		size = 1 * _GiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 10GB
		size = 10 * _GiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), size)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 100GB
		size = 100 * _GiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 1TB
		size = 1 * _TiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// max - 1
		size = _maxMultiCopySize - 1
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))
	})

	t.Run("TotalSizeGTMaxPartSize", func(t *testing.T) {
		_, err := splitIntoParts(_maxMultiCopySize + 1)
		assert.Error(t, err)
	})
}

func TestIsDeleteSuccessful(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"NilError", nil, true},
		{"Status200", minio.ErrorResponse{StatusCode: http.StatusOK}, true},
		{"Status404", minio.ErrorResponse{StatusCode: http.StatusNotFound}, false},
		{"Status403", minio.ErrorResponse{StatusCode: http.StatusForbidden}, false},
		{"Status500", minio.ErrorResponse{StatusCode: http.StatusInternalServerError}, false},
		{"NonMinioError", errors.New("error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDeleteSuccessful(tt.err))
		})
	}
}

func TestBucketExistStopsAfterFirstResult(t *testing.T) {
	var listReqCount atomic.Int32
	var firstReqPath string
	var firstReqMaxKeys string
	var firstReqContinuationToken string
	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "test-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if _, ok := r.URL.Query()["location"]; ok {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/xml"}},
					Body:       io.NopCloser(strings.NewReader(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)),
					Request:    r,
				}, nil
			}

			if r.URL.Query().Get("list-type") != "2" {
				return nil, errors.New("unexpected request")
			}

			if token := r.URL.Query().Get("continuation-token"); token != "" {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}

			listReqCount.Add(1)
			firstReqPath = r.URL.Path
			firstReqMaxKeys = r.URL.Query().Get("max-keys")
			firstReqContinuationToken = r.URL.Query().Get("continuation-token")

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <MaxKeys>1</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <Contents>
    <Key>first-object</Key>
    <Size>1</Size>
  </Contents>
  <NextContinuationToken>next-page</NextContinuationToken>
</ListBucketResult>`)),
				Request: r,
			}, nil
		}),
	})
	require.NoError(t, err)

	exists, err := cli.BucketExist(context.Background(), "")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.EqualValues(t, 1, listReqCount.Load())
	assert.Equal(t, "/test-bucket/", firstReqPath)
	assert.Equal(t, "1", firstReqMaxKeys)
	assert.Empty(t, firstReqContinuationToken)
}

func TestBucketExistReturnsFalseForNoSuchBucket(t *testing.T) {
	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "missing-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>missing-bucket</BucketName>
  <Resource>/missing-bucket/</Resource>
  <RequestId>req</RequestId>
  <HostId>host</HostId>
</Error>`)),
				Request: r,
			}, nil
		}),
	})
	require.NoError(t, err)

	exists, err := cli.BucketExist(context.Background(), "")
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestMinioObjectIterEarlyStopStopsGoroutine verifies that leaving the range
// before the listing is exhausted stops minio-go's background listing
// goroutine. The cleanup is three lines of defer in NewObjectIter, nothing
// else goes red when they are deleted, and a long-running server leaks one
// goroutine per early-stopped copy or verify task — so guard it directly as
// a leak check instead of observing cancellation through the transport.
//
// The follow-up page must actually be fetched: minio-go buffers one object,
// so a single-page listing lets the goroutine exit on its own and a leak
// would go undetected. With a truncated listing the goroutine blocks inside
// the page-two request unless the canceled context kills it.
func TestMinioObjectIterEarlyStopStopsGoroutine(t *testing.T) {
	defer goleak.VerifyNone(t)

	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "test-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if _, ok := r.URL.Query()["location"]; ok {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/xml"}},
					Body:       io.NopCloser(strings.NewReader(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)),
					Request:    r,
				}, nil
			}

			if r.URL.Query().Get("list-type") != "2" {
				return nil, errors.New("unexpected request")
			}

			// Page two never answers on its own; only the canceled request
			// context releases it. Without the cancel-and-drain defer the
			// listing goroutine is stuck here forever.
			if token := r.URL.Query().Get("continuation-token"); token != "" {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <IsTruncated>true</IsTruncated>
  <Contents>
    <Key>first-object</Key>
    <Size>1</Size>
  </Contents>
  <NextContinuationToken>next-page</NextContinuationToken>
</ListBucketResult>`)),
				Request: r,
			}, nil
		}),
	})
	require.NoError(t, err)

	var yielded bool
	for attr, err := range cli.NewObjectIter(context.Background(), "prefix/", true) {
		require.NoError(t, err)
		assert.Equal(t, "first-object", attr.Key)
		yielded = true
		break
	}
	assert.True(t, yielded)
}

func TestBucketExistPropagatesContextCancellation(t *testing.T) {
	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "test-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			return nil, context.Canceled
		}),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	exists, err := cli.BucketExist(ctx, "")
	assert.False(t, exists)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestCopyObjectEmbeddedError simulates the documented S3 behavior where a
// failed CopyObject still answers 200 OK but embeds an <Error> document in the
// body. minio-go's Client.CopyObject decodes that body into a field-less
// copyObjectResult and reports success with an empty ETag, so the copy layer
// must treat an empty ETag as a failure instead of trusting the status code.
func TestCopyObjectEmbeddedError(t *testing.T) {
	src := &MinioClient{cfg: Config{Bucket: "src-bucket"}}

	var copyReqCount atomic.Int32
	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "dest-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if _, ok := r.URL.Query()["location"]; ok {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/xml"}},
					Body:       io.NopCloser(strings.NewReader(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)),
					Request:    r,
				}, nil
			}

			copyReqCount.Add(1)
			// S3's documented "200 OK with an embedded error" response for a
			// CopyObject that failed midway.
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>InternalError</Code>
  <Message>We encountered an internal error. Please try again.</Message>
</Error>`)),
				Request: r,
			}, nil
		}),
	})
	require.NoError(t, err)

	err = cli.copyObject(context.Background(), src.cfg, CopyObjectInput{
		SrcCli:  src,
		SrcAttr: ObjectAttr{Key: "src-key", Length: 1},
		DestKey: "dest-key",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty etag")
	assert.Positive(t, copyReqCount.Load())
}

// TestCopyObjectValidETag pins the happy path: a copy that returns a
// well-formed CopyObjectResult with a real ETag must succeed, so the empty-ETag
// guard does not misfire on genuine copies.
func TestCopyObjectValidETag(t *testing.T) {
	src := &MinioClient{cfg: Config{Bucket: "src-bucket"}}

	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "dest-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if _, ok := r.URL.Query()["location"]; ok {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/xml"}},
					Body:       io.NopCloser(strings.NewReader(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)),
					Request:    r,
				}, nil
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <LastModified>2026-09-14T01:00:00.000Z</LastModified>
  <ETag>"9dd7d2e2f0a63a6e6f8e8c0a2b3c4d5e"</ETag>
</CopyObjectResult>`)),
				Request: r,
			}, nil
		}),
	})
	require.NoError(t, err)

	err = cli.copyObject(context.Background(), src.cfg, CopyObjectInput{
		SrcCli:  src,
		SrcAttr: ObjectAttr{Key: "src-key", Length: 1},
		DestKey: "dest-key",
	})
	require.NoError(t, err)
}

func TestDeleteObjects(t *testing.T) {
	var deleteReqCount atomic.Int32
	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "test-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if _, ok := r.URL.Query()["location"]; ok {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/xml"}},
					Body:       io.NopCloser(strings.NewReader(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)),
					Request:    r,
				}, nil
			}

			if _, ok := r.URL.Query()["delete"]; !ok {
				return nil, errors.New("unexpected request")
			}
			deleteReqCount.Add(1)

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted><Key>a/b/c</Key></Deleted>
  <Deleted><Key>a/b/d</Key></Deleted>
</DeleteResult>`)),
				Request: r,
			}, nil
		}),
	})
	require.NoError(t, err)

	err = cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/d"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleteReqCount.Load(), "one batch is one DeleteObjects request")
}

// TestDeleteObjectsNotImplemented pins the degrade contract: a backend
// answering 501 to the multi-object delete request gets the error wrapped
// with errBatchDeleteUnsupported, and the request is not retried.
func TestDeleteObjectsNotImplemented(t *testing.T) {
	var deleteReqCount atomic.Int32
	cli, err := newInternalMinio(Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Bucket:   "test-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}, &minio.Options{
		Secure: true,
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if _, ok := r.URL.Query()["location"]; ok {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/xml"}},
					Body:       io.NopCloser(strings.NewReader(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)),
					Request:    r,
				}, nil
			}

			deleteReqCount.Add(1)
			return &http.Response{
				StatusCode: http.StatusNotImplemented,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NotImplemented</Code>
  <Message>This gateway does not support multi-object delete.</Message>
</Error>`)),
				Request: r,
			}, nil
		}),
	})
	require.NoError(t, err)

	err = cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/d"})
	assert.ErrorIs(t, err, errBatchDeleteUnsupported)
	assert.EqualValues(t, 1, deleteReqCount.Load(), "NotImplemented must not be retried")
}

func TestIsBatchDeleteUnsupported(t *testing.T) {
	t.Run("StatusNotImplemented", func(t *testing.T) {
		err := minio.ErrorResponse{Code: "NotImplemented", StatusCode: http.StatusNotImplemented}
		assert.True(t, isBatchDeleteUnsupported(err))
	})

	t.Run("CodeOnlyNotImplemented", func(t *testing.T) {
		err := minio.ErrorResponse{Code: "NotImplemented"}
		assert.True(t, isBatchDeleteUnsupported(err))
	})

	t.Run("Wrapped", func(t *testing.T) {
		err := errors.Join(fmt.Errorf("storage: s3 delete object a/b/c %w",
			minio.ErrorResponse{Code: "NotImplemented", StatusCode: http.StatusNotImplemented}))
		assert.True(t, isBatchDeleteUnsupported(err))
	})

	t.Run("PerKeyFailureIsNotUnsupported", func(t *testing.T) {
		// Per-key failures decoded from a 200 response carry a Code but no
		// StatusCode, so an AccessDenied key must not read as "unsupported".
		err := minio.ErrorResponse{Code: "AccessDenied"}
		assert.False(t, isBatchDeleteUnsupported(err))
	})

	t.Run("PlainError", func(t *testing.T) {
		assert.False(t, isBatchDeleteUnsupported(assert.AnError))
	})
}
