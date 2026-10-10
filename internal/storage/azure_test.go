package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAzureMultipartCopyThreshold(t *testing.T) {
	t.Run("DefaultThreshold", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{}}
		assert.Equal(t, _azureMaxSyncCopySize, cli.multipartCopyThreshold())
	})

	t.Run("ConfiguredBelowLimit", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{MultipartCopyThresholdMiB: 100}}
		assert.Equal(t, int64(100*_MiB), cli.multipartCopyThreshold())
	})

	t.Run("ConfiguredAtLimit", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{MultipartCopyThresholdMiB: 256}}
		assert.Equal(t, _azureMaxSyncCopySize, cli.multipartCopyThreshold())
	})

	t.Run("ConfiguredAboveLimitCapped", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{MultipartCopyThresholdMiB: 500}}
		assert.Equal(t, _azureMaxSyncCopySize, cli.multipartCopyThreshold())
	})
}

// TestAzureIteratePagerSurfacesPaginationError locks the iterator contract
// that a pagination error must not be silently swallowed: the sequence yields
// the error as its last value, so a range loop propagates it instead of
// ending and reporting success (e.g. copy/delete/verify tasks returning nil).
func TestAzureIteratePagerSurfacesPaginationError(t *testing.T) {
	listErr := errors.New("azure list blobs failed")

	flatPager := runtime.NewPager(runtime.PagingHandler[azblob.ListBlobsFlatResponse]{
		More: func(azblob.ListBlobsFlatResponse) bool { return true },
		Fetcher: func(context.Context, *azblob.ListBlobsFlatResponse) (azblob.ListBlobsFlatResponse, error) {
			return azblob.ListBlobsFlatResponse{}, listErr
		},
	})
	hierPager := runtime.NewPager(runtime.PagingHandler[container.ListBlobsHierarchyResponse]{
		More: func(container.ListBlobsHierarchyResponse) bool { return true },
		Fetcher: func(context.Context, *container.ListBlobsHierarchyResponse) (container.ListBlobsHierarchyResponse, error) {
			return container.ListBlobsHierarchyResponse{}, listErr
		},
	})

	flatSeq := func(yield func(ObjectAttr, error) bool) {
		iteratePager(context.Background(), yield, flatPager, func(azblob.ListBlobsFlatResponse) []ObjectAttr { return nil })
	}
	hierSeq := func(yield func(ObjectAttr, error) bool) {
		iteratePager(context.Background(), yield, hierPager, func(container.ListBlobsHierarchyResponse) []ObjectAttr { return nil })
	}

	tests := []struct {
		name string
		seq  iter.Seq2[ObjectAttr, error]
	}{
		{"Flat", flatSeq},
		{"Hierarchy", hierSeq},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A standard range loop must receive the error and propagate it
			// instead of completing silently with a nil return.
			var got error
			for _, err := range tt.seq {
				if err != nil {
					got = err
					break
				}
			}
			require.Error(t, got) // got is dereferenced below
			assert.Contains(t, got.Error(), "azure list blobs failed")
		})
	}
}

// TestAzureIteratePagerSkipsEmptyPage locks the empty-page handling: a pager
// that yields a page with no objects before exhausting must end the sequence
// cleanly, not spin or error.
func TestAzureIteratePagerSkipsEmptyPage(t *testing.T) {
	fetched := 0
	pager := runtime.NewPager(runtime.PagingHandler[azblob.ListBlobsFlatResponse]{
		More: func(azblob.ListBlobsFlatResponse) bool {
			// only one page worth of content, then exhausted
			return fetched == 0
		},
		Fetcher: func(context.Context, *azblob.ListBlobsFlatResponse) (azblob.ListBlobsFlatResponse, error) {
			fetched++
			return azblob.ListBlobsFlatResponse{}, nil
		},
	})
	seq := func(yield func(ObjectAttr, error) bool) {
		iteratePager(context.Background(), yield, pager, func(azblob.ListBlobsFlatResponse) []ObjectAttr { return nil })
	}

	var count int
	for _, err := range seq {
		assert.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
	assert.Equal(t, 1, fetched)
}

// azureBatchPart describes one sub-response in a fake Blob Batch reply.
type azureBatchPart struct {
	contentID int
	status    string // e.g. "204 No Content"
	errorCode string // x-ms-error-code, empty for a success sub-response
}

// azureTransportFunc adapts a handler func to azcore's policy.Transporter.
type azureTransportFunc func(*http.Request) (*http.Response, error)

func (f azureTransportFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

// newAzureBatchTestClient builds an AzureClient whose HTTP layer is fn.
func newAzureBatchTestClient(t *testing.T, fn azureTransportFunc) *AzureClient {
	t.Helper()
	cred, err := azblob.NewSharedKeyCredential("account", base64.StdEncoding.EncodeToString([]byte("sk")))
	require.NoError(t, err)
	cli, err := azblob.NewClientWithSharedKeyCredential("https://account.blob.core.windows.net/", cred, &azblob.ClientOptions{
		ClientOptions: policy.ClientOptions{Transport: fn},
	})
	require.NoError(t, err)
	return &AzureClient{cfg: Config{Bucket: "bucket"}, cli: cli}
}

// azureBatchResponse renders a 202 Blob Batch response carrying parts.
func azureBatchResponse(r *http.Request, parts []azureBatchPart) *http.Response {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, p := range parts {
		h := textproto.MIMEHeader{}
		h.Set("Content-Type", "application/http")
		h.Set("Content-ID", strconv.Itoa(p.contentID))
		pw, err := w.CreatePart(h)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(pw, "HTTP/1.1 %s\r\n", p.status)
		if p.errorCode != "" {
			fmt.Fprintf(pw, "x-ms-error-code: %s\r\n", p.errorCode)
		}
		fmt.Fprintf(pw, "x-ms-request-id: req-%d\r\n", p.contentID)
		fmt.Fprintf(pw, "x-ms-version: 2023-11-03\r\n\r\n")
		if p.errorCode != "" {
			fmt.Fprintf(pw, `<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>msg</Message></Error>`, p.errorCode)
		}
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	return &http.Response{
		StatusCode: http.StatusAccepted,
		Header:     http.Header{"Content-Type": []string{"multipart/mixed; boundary=" + w.Boundary()}},
		Body:       io.NopCloser(&buf),
		Request:    r,
	}
}

// TestAzureDeleteObjects pins the happy path: a batch of keys goes out as one
// Blob Batch request carrying one delete sub-request per key.
func TestAzureDeleteObjects(t *testing.T) {
	var batchReqCount atomic.Int32
	cli := newAzureBatchTestClient(t, azureTransportFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("comp") != "batch" {
			return nil, errors.New("unexpected request")
		}
		batchReqCount.Add(1)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		// Blob names travel URL-escaped in the sub-request path.
		assert.Equal(t, 2, bytes.Count(body, []byte("\nDELETE ")), "one delete sub-request per key")
		assert.Contains(t, string(body), "DELETE /bucket/a%2Fb%2Fc")
		assert.Contains(t, string(body), "DELETE /bucket/a%2Fb%2Fd")

		return azureBatchResponse(r, []azureBatchPart{
			{contentID: 0, status: "204 No Content"},
			{contentID: 1, status: "204 No Content"},
		}), nil
	}))

	err := cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/d"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, batchReqCount.Load(), "one batch is one Blob Batch request")
}

// TestAzureDeleteObjectsNotFoundSwallowed pins the idempotency contract: a
// key that is already gone answers BlobNotFound inside the 202 response and
// must not fail the batch, or a retried batch would choke on the keys its
// previous attempt already deleted.
func TestAzureDeleteObjectsNotFoundSwallowed(t *testing.T) {
	cli := newAzureBatchTestClient(t, azureTransportFunc(func(r *http.Request) (*http.Response, error) {
		return azureBatchResponse(r, []azureBatchPart{
			{contentID: 0, status: "204 No Content"},
			{contentID: 1, status: "404 The specified blob does not exist.", errorCode: string(bloberror.BlobNotFound)},
		}), nil
	}))

	err := cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/gone"})
	assert.NoError(t, err)
}

// TestAzureDeleteObjectsPerItemFailureRetried pins the failure contract: a
// failed sub-response inside a 202 fails the batch, and the whole batch is
// retried. synctest fast-forwards the retry backoff.
func TestAzureDeleteObjectsPerItemFailureRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var batchReqCount atomic.Int32
		cli := newAzureBatchTestClient(t, azureTransportFunc(func(r *http.Request) (*http.Response, error) {
			batchReqCount.Add(1)
			return azureBatchResponse(r, []azureBatchPart{
				{contentID: 0, status: "204 No Content"},
				{contentID: 1, status: "500 Internal Server Error", errorCode: "InternalError"},
			}), nil
		}))

		err := cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/d"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "azure delete object a/b/d")
		assert.Greater(t, batchReqCount.Load(), int32(1), "a failed sub-response retries the batch")
	})
}

// TestAzureDeleteObjectsNotImplemented pins the degrade contract: a backend
// answering 501 to the Blob Batch request gets the error wrapped with
// errBatchDeleteUnsupported, and the request is not retried.
func TestAzureDeleteObjectsNotImplemented(t *testing.T) {
	var batchReqCount atomic.Int32
	cli := newAzureBatchTestClient(t, azureTransportFunc(func(r *http.Request) (*http.Response, error) {
		batchReqCount.Add(1)
		return &http.Response{
			StatusCode: http.StatusNotImplemented,
			Header:     http.Header{"Content-Type": []string{"application/xml"}},
			Body: io.NopCloser(bytes.NewReader([]byte(
				`<?xml version="1.0" encoding="utf-8"?><Error><Code>NotImplemented</Code><Message>Blob Batch is not supported.</Message></Error>`))),
			Request: r,
		}, nil
	}))

	err := cli.DeleteObjects(context.Background(), []string{"a/b/c", "a/b/d"})
	assert.ErrorIs(t, err, errBatchDeleteUnsupported)
	assert.EqualValues(t, 1, batchReqCount.Load(), "NotImplemented must not be retried")
}

func TestIsAzureBatchDeleteUnsupported(t *testing.T) {
	t.Run("StatusNotImplemented", func(t *testing.T) {
		err := &azcore.ResponseError{StatusCode: http.StatusNotImplemented}
		assert.True(t, isAzureBatchDeleteUnsupported(err))
	})

	t.Run("Wrapped", func(t *testing.T) {
		err := fmt.Errorf("storage: azure submit batch: %w", &azcore.ResponseError{StatusCode: http.StatusNotImplemented})
		assert.True(t, isAzureBatchDeleteUnsupported(err))
	})

	t.Run("OtherStatus", func(t *testing.T) {
		err := &azcore.ResponseError{StatusCode: http.StatusForbidden}
		assert.False(t, isAzureBatchDeleteUnsupported(err))
	})

	t.Run("PlainError", func(t *testing.T) {
		assert.False(t, isAzureBatchDeleteUnsupported(assert.AnError))
	})
}

func TestIsBlobNotFound(t *testing.T) {
	t.Run("BlobNotFound", func(t *testing.T) {
		err := &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}
		assert.True(t, isBlobNotFound(err))
	})

	t.Run("Wrapped", func(t *testing.T) {
		err := fmt.Errorf("wrap: %w", &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)})
		assert.True(t, isBlobNotFound(err))
	})

	t.Run("OtherCode", func(t *testing.T) {
		err := &azcore.ResponseError{ErrorCode: "InternalError"}
		assert.False(t, isBlobNotFound(err))
	})

	t.Run("PlainError", func(t *testing.T) {
		assert.False(t, isBlobNotFound(assert.AnError))
	})
}
