package storage

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func xmlResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/xml"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func headResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"ETag": []string{`"deadbeef"`}},
		Body:       http.NoBody,
	}
}

func staticS3Cfg(bucket string) Config {
	return Config{
		Provider: "s3",
		Endpoint: "example.com",
		UseSSL:   true,
		Region:   "us-east-1",
		Bucket:   bucket,
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	}
}

// newTestS3Client builds an S3Client whose requests run through the stub
// round tripper. The config must carry a region so no GetBucketLocation
// probe goes out; discovery has tests of its own.
func newTestS3Client(t *testing.T, cfg Config, tr roundTripperFunc) *S3Client {
	t.Helper()

	provider := staticS3CredProvider(cfg.Credential)
	cli, err := newS3ClientWithCreds(t.Context(), cfg, provider, !isVirtualHostEndpoint(cfg.Endpoint),
		func(o *s3.Options) { o.HTTPClient = &http.Client{Transport: tr} })
	require.NoError(t, err)

	return cli
}

func TestBucketExistStopsAfterFirstResult(t *testing.T) {
	var listReqCount atomic.Int32
	var firstReqPath string
	var firstReqMaxKeys string
	var firstReqContinuationToken string
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
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

		return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
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
</ListBucketResult>`), nil
	})

	exists, err := cli.BucketExist(context.Background(), "")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.EqualValues(t, 1, listReqCount.Load())
	assert.Equal(t, "/test-bucket", firstReqPath)
	assert.Equal(t, "1", firstReqMaxKeys)
	assert.Empty(t, firstReqContinuationToken)
}

func TestBucketExistReturnsFalseForNoSuchBucket(t *testing.T) {
	cli := newTestS3Client(t, staticS3Cfg("missing-bucket"), func(r *http.Request) (*http.Response, error) {
		return xmlResponse(http.StatusNotFound, `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>missing-bucket</BucketName>
  <Resource>/missing-bucket/</Resource>
  <RequestId>req</RequestId>
  <HostId>host</HostId>
</Error>`), nil
	})

	exists, err := cli.BucketExist(context.Background(), "")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestBucketExistPropagatesContextCancellation(t *testing.T) {
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		return nil, context.Canceled
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	exists, err := cli.BucketExist(ctx, "")
	assert.False(t, exists)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestObjectIterEarlyStopFetchesNoSecondPage verifies that leaving the range
// before the listing is exhausted stops fetching. The iteration is
// synchronous — each page is pulled only when the previous one is consumed —
// so an early break has no background goroutine to leak (the goleak check
// below pins that property of the SDK path too); what must hold is that the
// broken range never issues the follow-up page request.
func TestObjectIterEarlyStopFetchesNoSecondPage(t *testing.T) {
	defer goleak.VerifyNone(t)

	var listReqCount atomic.Int32
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		listReqCount.Add(1)

		// A truncated listing: pulling page two is the only way to fetch it.
		return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <IsTruncated>true</IsTruncated>
  <Contents>
    <Key>first-object</Key>
    <Size>1</Size>
  </Contents>
  <NextContinuationToken>next-page</NextContinuationToken>
</ListBucketResult>`), nil
	})

	var yielded bool
	for attr, err := range cli.NewObjectIter(context.Background(), "prefix/", true) {
		require.NoError(t, err)
		assert.Equal(t, "first-object", attr.Key)
		yielded = true
		break
	}
	assert.True(t, yielded)
	assert.EqualValues(t, 1, listReqCount.Load(), "the broken range must not fetch page two")
}

// TestObjectIterYieldsCommonPrefixes pins the non-recursive shape: the
// delimiter goes out on the request, and directory prefixes are yielded after
// the objects of each page — Exist counts a directory as existing, exactly
// like the minio client's listing did.
func TestObjectIterYieldsCommonPrefixes(t *testing.T) {
	var delimiter string
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		delimiter = r.URL.Query().Get("delimiter")

		return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>test-bucket</Name>
  <Prefix>backups/</Prefix>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>backups/backup.meta</Key>
    <Size>10</Size>
  </Contents>
  <CommonPrefixes>
    <Prefix>backups/sub/</Prefix>
  </CommonPrefixes>
</ListBucketResult>`), nil
	})

	var attrs []ObjectAttr
	for attr, err := range cli.NewObjectIter(context.Background(), "backups/", false) {
		require.NoError(t, err)
		attrs = append(attrs, attr)
	}

	assert.Equal(t, "/", delimiter)
	require.Len(t, attrs, 2)
	assert.Equal(t, "backups/backup.meta", attrs[0].Key)
	assert.Equal(t, int64(10), attrs[0].Length)
	assert.Equal(t, "backups/sub/", attrs[1].Key)
}

// TestCopyObjectEmbeddedError simulates the documented S3 behavior where a
// failed CopyObject still answers 200 OK but embeds an <Error> document in the
// body. The SDK decodes that body into an error for every S3 operation, and
// the copy layer must surface it so the retry kicks in instead of silently
// dropping the object.
func TestCopyObjectEmbeddedError(t *testing.T) {
	// The SDK's internal retries on the embedded error back off on a real
	// clock; run the bubble on a fake one.
	synctest.Test(t, func(t *testing.T) {
		src := &S3Client{cfg: Config{Bucket: "src-bucket"}}

		var copyReqCount atomic.Int32
		cli := newTestS3Client(t, staticS3Cfg("dest-bucket"), func(r *http.Request) (*http.Response, error) {
			copyReqCount.Add(1)
			// S3's documented "200 OK with an embedded error" response for a
			// CopyObject that failed midway.
			return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>InternalError</Code>
  <Message>We encountered an internal error. Please try again.</Message>
</Error>`), nil
		})

		err := cli.copyObject(context.Background(), src.cfg, CopyObjectInput{
			SrcCli:  src,
			SrcAttr: ObjectAttr{Key: "src-key", Length: 1},
			DestKey: "dest-key",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InternalError")
		assert.Positive(t, copyReqCount.Load())
	})
}

// TestCopyObject pins the happy path: a copy that returns a well-formed
// CopyObjectResult succeeds, addressed by an escaped x-amz-copy-source.
func TestCopyObject(t *testing.T) {
	src := &S3Client{cfg: Config{Bucket: "src-bucket"}}

	var copySourceHeader string
	cli := newTestS3Client(t, staticS3Cfg("dest-bucket"), func(r *http.Request) (*http.Response, error) {
		copySourceHeader = r.Header.Get("x-amz-copy-source")

		return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <LastModified>2026-09-14T01:00:00.000Z</LastModified>
  <ETag>"9dd7d2e2f0a63a6e6f8e8c0a2b3c4d5e"</ETag>
</CopyObjectResult>`), nil
	})

	err := cli.copyObject(context.Background(), src.cfg, CopyObjectInput{
		SrcCli:  src,
		SrcAttr: ObjectAttr{Key: "src key/with spaces", Length: 1},
		DestKey: "dest-key",
	})
	require.NoError(t, err)
	assert.Equal(t, "src-bucket/src%20key/with%20spaces", copySourceHeader)
}

const _multipartUploadID = "upload-id-1"

// TestMultiPartCopy pins the hand-written multipart copy against the stubbed
// S3 flow: create, one UploadPartCopy per split part, then a complete request.
func TestMultiPartCopy(t *testing.T) {
	src := &S3Client{cfg: Config{Bucket: "src-bucket"}}

	var mu sync.Mutex
	var ranges []string
	cli := newTestS3Client(t, staticS3Cfg("dest-bucket"), func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		switch {
		case q.Has("uploads"):
			return xmlResponse(http.StatusOK, fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>dest-bucket</Bucket>
  <Key>dest-key</Key>
  <UploadId>%s</UploadId>
</InitiateMultipartUploadResult>`, _multipartUploadID)), nil
		case q.Get("uploadId") == _multipartUploadID && r.Method == http.MethodPost:
			return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>https://dest-bucket.example.com/dest-key</Location>
  <Bucket>dest-bucket</Bucket>
  <Key>dest-key</Key>
  <ETag>"done"</ETag>
</CompleteMultipartUploadResult>`), nil
		case q.Get("uploadId") == _multipartUploadID:
			mu.Lock()
			ranges = append(ranges, r.Header.Get("x-amz-copy-source-range"))
			mu.Unlock()
			return xmlResponse(http.StatusOK, fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <ETag>"etag-%s"</ETag>
</CopyPartResult>`, q.Get("partNumber"))), nil
		default:
			return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
	})

	// 11 MiB splits into a 10 MiB part and a 1 MiB part.
	err := cli.multiPartCopy(context.Background(), src.cfg, CopyObjectInput{
		SrcCli:  src,
		SrcAttr: ObjectAttr{Key: "src-key", Length: 11 * _MiB},
		DestKey: "dest-key",
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bytes=0-10485759", "bytes=10485760-11534335"}, ranges)
}

// TestCopyPartMissingETagRetries absorbs the guard PR #1367 pinned on the
// minio path: a 200 answer whose body carries no ETag is a failure the SDK
// cannot decode, and copyPart must reject it so the retry loop reruns the
// part instead of completing the upload with an invalid part.
func TestCopyPartMissingETagRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &S3Client{cfg: Config{Bucket: "src-bucket"}}

		var partReqCount atomic.Int32
		cli := newTestS3Client(t, staticS3Cfg("dest-bucket"), func(r *http.Request) (*http.Response, error) {
			q := r.URL.Query()
			switch {
			case q.Has("uploads"):
				return xmlResponse(http.StatusOK, fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>dest-bucket</Bucket>
  <Key>dest-key</Key>
  <UploadId>%s</UploadId>
</InitiateMultipartUploadResult>`, _multipartUploadID)), nil
			case q.Get("uploadId") == _multipartUploadID && r.Method == http.MethodPost:
				return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>dest-bucket</Bucket>
  <Key>dest-key</Key>
</CompleteMultipartUploadResult>`), nil
			case q.Get("uploadId") == _multipartUploadID:
				partReqCount.Add(1)
				// A well-formed 200 whose CopyPartResult carries no ETag —
				// the shape that used to slip through as a silently skipped
				// part (an <Error> body would be caught by the SDK itself).
				return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></CopyPartResult>`), nil
			default:
				return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL)
			}
		})

		err := cli.multiPartCopy(context.Background(), src.cfg, CopyObjectInput{
			SrcCli:  src,
			SrcAttr: ObjectAttr{Key: "src-key", Length: 11 * _MiB},
			DestKey: "dest-key",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing etag")
		assert.Positive(t, partReqCount.Load())
	})
}

func TestDeleteObjects(t *testing.T) {
	var deleteReqCount atomic.Int32
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		if _, ok := r.URL.Query()["delete"]; !ok {
			return nil, errors.New("unexpected request")
		}
		deleteReqCount.Add(1)

		return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted><Key>a/b/c</Key></Deleted>
  <Deleted><Key>a/b/d</Key></Deleted>
</DeleteResult>`), nil
	})

	err := cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/d"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleteReqCount.Load(), "one batch is one DeleteObjects request")
}

// TestDeleteObjectsNotImplemented pins the degrade contract: a backend
// answering 501 to the multi-object delete request gets the error wrapped
// with errBatchDeleteUnsupported, and the request is not retried.
func TestDeleteObjectsNotImplemented(t *testing.T) {
	var deleteReqCount atomic.Int32
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		deleteReqCount.Add(1)
		return xmlResponse(http.StatusNotImplemented, `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NotImplemented</Code>
  <Message>This gateway does not support multi-object delete.</Message>
</Error>`), nil
	})

	err := cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/d"})
	assert.ErrorIs(t, err, errBatchDeleteUnsupported)
	assert.EqualValues(t, 1, deleteReqCount.Load(), "NotImplemented must not be retried")
}

// TestDeleteObjectsPerKeyErrorsSurface pins that per-key failures inside a 200
// DeleteResult are joined into the returned error, with the key in the message.
func TestDeleteObjectsPerKeyErrorsSurface(t *testing.T) {
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Error><Key>a/b/c</Key><Code>AccessDenied</Code><Message>no</Message></Error>
</DeleteResult>`), nil
	})

	err := cli.DeleteObjects(context.Background(), []string{"a/b/c"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a/b/c")

	var ae smithy.APIError
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "AccessDenied", ae.ErrorCode())
}

// TestDeleteObjectsCarriesContentMD5 pins the compat middleware: old MinIO
// releases reject DeleteObjects without the classic Content-MD5 header, and
// the SDK stopped sending it. The header must equal the MD5 of the body.
func TestDeleteObjectsCarriesContentMD5(t *testing.T) {
	var md5Header string
	var body []byte
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		if _, ok := r.URL.Query()["delete"]; !ok {
			return nil, errors.New("unexpected request")
		}
		md5Header = r.Header.Get("Content-MD5")
		body, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()

		return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`), nil
	})

	err := cli.DeleteObjects(context.Background(), []string{"a/b/c"})
	require.NoError(t, err)

	require.NotEmpty(t, md5Header)
	sum := md5.Sum(body)
	assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), md5Header)
}

type fakeAPIError struct{ code string }

func (e *fakeAPIError) Error() string                 { return e.code }
func (e *fakeAPIError) ErrorCode() string             { return e.code }
func (e *fakeAPIError) ErrorMessage() string          { return e.code }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func TestIsBatchDeleteUnsupported(t *testing.T) {
	t.Run("StatusNotImplemented", func(t *testing.T) {
		err := &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotImplemented}},
			Err:      errors.New("no multi delete"),
		}
		assert.True(t, isBatchDeleteUnsupported(err))
	})

	t.Run("CodeOnlyNotImplemented", func(t *testing.T) {
		err := &fakeAPIError{code: "NotImplemented"}
		assert.True(t, isBatchDeleteUnsupported(err))
	})

	t.Run("Wrapped", func(t *testing.T) {
		err := errors.Join(fmt.Errorf("storage: s3 delete object a/b/c: %w",
			&fakeAPIError{code: "NotImplemented"}))
		assert.True(t, isBatchDeleteUnsupported(err))
	})

	t.Run("PerKeyFailureIsNotUnsupported", func(t *testing.T) {
		// Per-key failures decoded from a 200 response carry a Code but no
		// StatusCode, so an AccessDenied key must not read as "unsupported".
		err := &fakeAPIError{code: "AccessDenied"}
		assert.False(t, isBatchDeleteUnsupported(err))
	})

	t.Run("PlainError", func(t *testing.T) {
		assert.False(t, isBatchDeleteUnsupported(assert.AnError))
	})
}

func TestUploadObjectSmallKnownSizeSinglePut(t *testing.T) {
	var putReqCount atomic.Int32
	var contentLength int64
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		putReqCount.Add(1)
		contentLength = r.ContentLength

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"ETag": []string{`"deadbeef"`}},
			Body:       http.NoBody,
		}, nil
	})

	err := cli.UploadObject(context.Background(), UploadObjectInput{
		Key:  "meta/backup.meta",
		Body: strings.NewReader("hello"),
		Size: 5,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, putReqCount.Load())
	assert.Equal(t, int64(5), contentLength)
}

// TestUploadObjectUnknownSizeMultipart pins the native-uploader path: a
// stream of unknown size is split by the SDK into fixed-size parts, each
// uploaded with UploadPart, then completed.
func TestUploadObjectUnknownSizeMultipart(t *testing.T) {
	old := _uploadMultipartThreshold
	// The uploader rejects part sizes under 5 MiB, so shrink to that floor
	// and stream more than two parts' worth.
	_uploadMultipartThreshold = 5 * 1024 * 1024
	t.Cleanup(func() { _uploadMultipartThreshold = old })

	var mu sync.Mutex
	var partNumbers []string
	cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		switch {
		case q.Has("uploads"):
			return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>test-bucket</Bucket>
  <Key>big-object</Key>
  <UploadId>upload-id-2</UploadId>
</InitiateMultipartUploadResult>`), nil
		case q.Get("uploadId") == "upload-id-2" && r.Method == http.MethodPost:
			return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>test-bucket</Bucket>
  <Key>big-object</Key>
</CompleteMultipartUploadResult>`), nil
		case q.Get("uploadId") == "upload-id-2":
			mu.Lock()
			partNumbers = append(partNumbers, q.Get("partNumber"))
			mu.Unlock()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"ETag": []string{`"part"`}},
				Body:       http.NoBody,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
	})

	// 12 MiB over 5 MiB parts: three UploadPart calls.
	body := strings.Repeat("a", 12*1024*1024)
	err := cli.UploadObject(context.Background(), UploadObjectInput{
		Key:  "big-object",
		Body: strings.NewReader(body),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, partNumbers)
}

// TestNewS3ClientDiscoversRegion pins the auto-discovery the minio client
// performed: with an empty config region, a GetBucketLocation probe at
// construction names the signing region of every later request.
func TestNewS3ClientDiscoversRegion(t *testing.T) {
	var authHeader atomic.Pointer[string]
	probeDone := false
	provider := staticS3CredProvider(staticS3Cfg("test-bucket").Credential)

	cfg := staticS3Cfg("test-bucket")
	cfg.Region = ""
	cli, err := newS3ClientWithCreds(t.Context(), cfg, provider, true,
		func(o *s3.Options) {
			o.HTTPClient = &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Query().Has("location") && !probeDone {
					probeDone = true
					return xmlResponse(http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">eu-west-2</LocationConstraint>`), nil
				}

				authHeader.Store(lo.ToPtr(r.Header.Get("Authorization")))
				return headResponse(), nil
			})}
		})
	require.NoError(t, err)
	assert.True(t, probeDone, "construction must probe GetBucketLocation once")

	_, err = cli.HeadObject(context.Background(), "key")
	require.NoError(t, err)
	assert.Contains(t, *authHeader.Load(), "/eu-west-2/", "later requests sign with the discovered region")
}

// TestNewS3ClientRegionFallbackOnMissingBucket pins the fallback the minio
// client had: a probe on a bucket that does not exist yet (NewBackupStorage
// creates it after the client) must not fail construction — it falls back to
// us-east-1 signing.
func TestNewS3ClientRegionFallbackOnMissingBucket(t *testing.T) {
	var authHeader atomic.Pointer[string]
	provider := staticS3CredProvider(staticS3Cfg("test-bucket").Credential)

	cfg := staticS3Cfg("test-bucket")
	cfg.Region = ""
	cli, err := newS3ClientWithCreds(t.Context(), cfg, provider, true,
		func(o *s3.Options) {
			o.HTTPClient = &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Query().Has("location") {
					return xmlResponse(http.StatusNotFound, `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
</Error>`), nil
				}

				authHeader.Store(lo.ToPtr(r.Header.Get("Authorization")))
				return headResponse(), nil
			})}
		})
	require.NoError(t, err)

	_, err = cli.HeadObject(context.Background(), "key")
	require.NoError(t, err)
	assert.Contains(t, *authHeader.Load(), "/us-east-1/", "the fallback region signs later requests")
}

// TestS3ClientPathStyle pins the bucket-resolution rule the minio client
// applied: only AWS and GCS endpoints resolve by virtual host, everything
// else by path.
func TestS3ClientPathStyle(t *testing.T) {
	t.Run("CustomEndpointUsesPath", func(t *testing.T) {
		var requestHost, requestPath string
		cli := newTestS3Client(t, staticS3Cfg("test-bucket"), func(r *http.Request) (*http.Response, error) {
			requestHost = r.URL.Host
			requestPath = r.URL.Path
			return headResponse(), nil
		})

		_, err := cli.HeadObject(context.Background(), "key")
		require.NoError(t, err)
		assert.Equal(t, "example.com", requestHost)
		assert.Equal(t, "/test-bucket/key", requestPath)
	})

	t.Run("AWSEndpointUsesVirtualHost", func(t *testing.T) {
		var requestHost, requestPath string
		cfg := staticS3Cfg("test-bucket")
		cfg.Endpoint = "s3.us-east-1.amazonaws.com"
		cli := newTestS3Client(t, cfg, func(r *http.Request) (*http.Response, error) {
			requestHost = r.URL.Host
			requestPath = r.URL.Path
			return headResponse(), nil
		})

		_, err := cli.HeadObject(context.Background(), "key")
		require.NoError(t, err)
		assert.Equal(t, "test-bucket.s3.us-east-1.amazonaws.com", requestHost)
		assert.Equal(t, "/key", requestPath)
	})
}

func TestIsVirtualHostEndpoint(t *testing.T) {
	assert.True(t, isVirtualHostEndpoint("s3.us-east-1.amazonaws.com"))
	assert.True(t, isVirtualHostEndpoint("s3.cn-north-1.amazonaws.com.cn"))
	assert.True(t, isVirtualHostEndpoint("storage.googleapis.com:443"))
	assert.False(t, isVirtualHostEndpoint("minio.example.com:9000"))
	assert.False(t, isVirtualHostEndpoint("localhost:9000"))
}

// TestS3ClientSelfSignedTLS drives a real TLS handshake against a server
// presenting a self-signed certificate: the configured CA must be what
// admits the connection, and its absence must fail with an x509 error.
func TestS3ClientSelfSignedTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
			return
		}
		w.Header().Set("ETag", `"deadbeef"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, certPEM, 0o600))

	endpoint := strings.TrimPrefix(srv.URL, "https://")

	newCfg := func() Config {
		cfg := staticS3Cfg("test-bucket")
		cfg.Endpoint = endpoint
		return cfg
	}

	t.Run("WithoutTheCA", func(t *testing.T) {
		provider := staticS3CredProvider(newCfg().Credential)

		cli, err := newS3ClientWithCreds(t.Context(), newCfg(), provider, true)
		require.NoError(t, err)

		_, err = cli.HeadObject(context.Background(), "key")
		assert.ErrorContains(t, err, "x509")
	})

	t.Run("WithTheCA", func(t *testing.T) {
		trusted := newCfg()
		trusted.CACertPath = caPath

		provider := staticS3CredProvider(trusted.Credential)

		cli, err := newS3ClientWithCreds(t.Context(), trusted, provider, true)
		require.NoError(t, err)

		_, err = cli.HeadObject(context.Background(), "key")
		assert.NoError(t, err)
	})
}
