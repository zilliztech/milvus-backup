package storage

import (
	"context"
	"errors"
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
