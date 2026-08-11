package storage

import (
	"context"
	"errors"
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

// TestAzureObjectIteratorSurfacesPaginationError locks the iterator contract
// that a pagination error must not be silently swallowed: Next returns
// (zero, false, err) after a failed NextPage, so the caller loop propagates
// the error instead of ending and reporting success (e.g. copy/delete/verify
// tasks returning nil).
func TestAzureObjectIteratorSurfacesPaginationError(t *testing.T) {
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

	tests := []struct {
		name string
		iter ObjectIterator
	}{
		{"Flat", &AzureObjectFlatIterator{pageIterator: pageIterator[azblob.ListBlobsFlatResponse]{pager: flatPager}}},
		{"Hierarchy", &AzureObjectHierarchyIterator{pageIterator: pageIterator[container.ListBlobsHierarchyResponse]{pager: hierPager}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Next must surface the pagination error instead of ending the loop.
			_, ok, err := tt.iter.Next(context.Background())
			assert.False(t, ok)
			require.Error(t, err) // err is dereferenced below
			assert.Contains(t, err.Error(), "azure list blobs failed")

			// The standard caller loop must propagate the error rather than
			// complete silently with a nil return.
			walkErr := func(iter ObjectIterator) error {
				for {
					_, ok, err := iter.Next(context.Background())
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}
			}
			assert.Error(t, walkErr(tt.iter))
		})
	}
}

// TestAzureObjectIteratorSkipsEmptyPage locks the empty-page handling: a pager
// that yields a page with no objects before exhausting must terminate cleanly
// with ok=false, not spin or error.
func TestAzureObjectIteratorSkipsEmptyPage(t *testing.T) {
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
	iter := &AzureObjectFlatIterator{pageIterator: pageIterator[azblob.ListBlobsFlatResponse]{
		pager: pager,
		toAttrs: func(page azblob.ListBlobsFlatResponse) []ObjectAttr {
			return nil
		},
	}}

	_, ok, err := iter.Next(context.Background())
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, 1, fetched)
}
