package storage

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
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
