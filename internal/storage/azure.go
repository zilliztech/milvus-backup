package storage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/retry"
)

var _ io.ReadCloser = (*AzureReader)(nil)

type AzureReader struct {
	cli    *blockblob.Client
	length int64
	pos    int64
}

func (a *AzureReader) Read(p []byte) (int, error) {
	if a.pos >= a.length {
		return 0, io.EOF
	}
	count := int64(len(p))
	if a.pos+count >= a.length {
		count = a.length - a.pos
	}

	opt := &azblob.DownloadBufferOptions{Range: azblob.HTTPRange{Offset: a.pos, Count: count}}
	n, err := a.cli.DownloadBuffer(context.Background(), p, opt)
	a.pos += n

	if err != nil {
		return int(n), fmt.Errorf("storage: read azure download buffer %w pos:%d count:%d file-len:%d buff-len:%d", err, a.pos, count, a.length, len(p))
	}

	return int(n), nil
}

func (a *AzureReader) Close() error { return nil }

var _ Client = (*AzureClient)(nil)

func newAzureClient(cfg Config) (*AzureClient, error) {
	// backwards compatible, don't know why we kept the "blob" in the code instead of letting it be input externally.
	// Remove standard HTTPS port :443 from endpoint if present
	endpoint := strings.TrimSuffix(cfg.Endpoint, ":443")
	ep := fmt.Sprintf("https://%s.blob.%s", cfg.Credential.AzureAccountName, endpoint)
	logger := log.L().With(zap.String("provider", cfg.Provider), zap.String("endpoint", endpoint))
	switch cfg.Credential.Type {
	case IAM:
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure default azure credential %w", err)
		}
		cli, err := azblob.NewClient(ep, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure client %w", err)
		}
		sasCli, err := service.NewClient(ep, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure service client %w", err)
		}

		return &AzureClient{cfg: cfg, cli: cli, sasCli: sasCli, logger: logger}, nil
	case Static:
		cred, err := azblob.NewSharedKeyCredential(cfg.Credential.AK, cfg.Credential.SK)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure shared key credential %w", err)
		}
		cli, err := azblob.NewClientWithSharedKeyCredential(ep, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure client %w", err)
		}
		sasCli, err := service.NewClientWithSharedKeyCredential(ep, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure service client %w", err)
		}
		return &AzureClient{cfg: cfg, cli: cli, sasCli: sasCli, logger: logger}, nil
	default:
		return nil, fmt.Errorf("storage: azure unsupported credential type: %s", cfg.Credential.Type.String())
	}
}

type AzureClient struct {
	cfg Config

	cli *azblob.Client

	// sasCli is used to generate SAS token.
	// When we want to copy object under two different service accounts, AD auth is not supported.
	// So we need to use AD auth to generate SAS token and use SAS token to copy object.
	sasCli *service.Client

	logger *zap.Logger
}

func (a *AzureClient) getSAS(ctx context.Context, srcCli *AzureClient) (*sas.QueryParameters, error) {
	if srcCli.cfg.Credential.Type == IAM {
		return a.getSASByUserDelegation(ctx, srcCli)
	}
	return a.getSASBySharedKeyCredential(srcCli)
}

func (a *AzureClient) getSASBySharedKeyCredential(srcCli *AzureClient) (*sas.QueryParameters, error) {
	credential, err := azblob.NewSharedKeyCredential(srcCli.cfg.Credential.AK, srcCli.cfg.Credential.SK)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared key credential: %w", err)
	}

	sasQueryParams, err := sas.AccountSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   to.Ptr(sas.AccountPermissions{Read: true, List: true}).String(),
		ResourceTypes: to.Ptr(sas.AccountResourceTypes{Container: true, Object: true}).String(),
	}.SignWithSharedKey(credential)

	if err != nil {
		return nil, fmt.Errorf("failed to sign SAS with shared key: %w", err)
	}

	return &sasQueryParams, nil
}

func (a *AzureClient) getSASByUserDelegation(ctx context.Context, srcCli *AzureClient) (*sas.QueryParameters, error) {
	// Set current and pastime and create key
	now := time.Now().Add(-10 * time.Second)
	expiry := now.Add(48 * time.Hour)
	info := service.KeyInfo{
		Start:  to.Ptr(now.Format(sas.TimeFormat)),
		Expiry: to.Ptr(expiry.Format(sas.TimeFormat)),
	}
	udc, err := srcCli.sasCli.GetUserDelegationCredential(ctx, info, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: azure get user delegation credential %w", err)
	}
	// Create Blob Signature Values with desired permissions and sign with user delegation credential
	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now,
		ExpiryTime:    expiry,
		Permissions:   to.Ptr(sas.ContainerPermissions{Read: true, List: true}).String(),
		ContainerName: srcCli.cfg.Bucket,
	}.SignWithUserDelegation(udc)
	if err != nil {
		return nil, fmt.Errorf("storage: azure sign with user delegation %w", err)
	}
	return &sasQueryParams, nil
}

func (a *AzureClient) Config() Config { return a.cfg }

func (a *AzureClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(*AzureClient)
	if !ok {
		return fmt.Errorf("storage: azure copy object src client is not azure")
	}

	threshold := a.multipartCopyThreshold()
	if i.SrcAttr.Length >= threshold {
		a.logger.Debug("copy object by multipart", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
		return a.multiPartCopy(ctx, srcCli, i)
	}

	a.logger.Debug("copy object by single part", zap.String("src_key", i.SrcAttr.Key), zap.String("dest_key", i.DestKey))
	return a.copyObject(ctx, srcCli, i)
}

const _azureMaxSyncCopySize int64 = 256 * _MiB

func (a *AzureClient) multipartCopyThreshold() int64 {
	if a.cfg.MultipartCopyThresholdMiB > 0 {
		threshold := a.cfg.MultipartCopyThresholdMiB * _MiB
		// Azure CopyFromURL (synchronous copy) has a 256 MB limit,
		// so we cap the threshold to avoid errors
		if threshold > _azureMaxSyncCopySize {
			return _azureMaxSyncCopySize
		}
		return threshold
	}
	return _azureMaxSyncCopySize
}

func (a *AzureClient) copyObject(ctx context.Context, srcCli *AzureClient, i CopyObjectInput) error {
	return retry.Do(ctx, func() error {
		// Remove standard HTTPS port :443 from endpoint if present
		endpoint := strings.TrimSuffix(srcCli.cfg.Endpoint, ":443")
		srcURL := fmt.Sprintf("https://%s.blob.%s/%s/%s", srcCli.cfg.Credential.AzureAccountName, endpoint, srcCli.cfg.Bucket, i.SrcAttr.Key)
		// Azure CopyFromURL always requires authentication for the source URL, even for same account
		// Generate SAS token for source URL
		srcSAS, err := a.getSAS(ctx, srcCli)
		if err != nil {
			return err
		}
		srcURL += "?" + srcSAS.Encode()

		blobCli := a.cli.ServiceClient().NewContainerClient(a.cfg.Bucket).NewBlockBlobClient(i.DestKey)
		// we need to abort the previous copy operation before copy from url
		abortErr := func() error {
			blobProperties, err := blobCli.BlobClient().GetProperties(ctx, nil)
			if err != nil {
				return fmt.Errorf("storage: azure get properties %w", err)
			}
			if blobProperties.CopyID != nil {
				if _, err = blobCli.AbortCopyFromURL(ctx, *blobProperties.CopyID, nil); err != nil {
					return fmt.Errorf("storage: azure abort copy from url %w", err)
				}
			}
			return nil
		}()

		if _, err := blobCli.CopyFromURL(ctx, srcURL, nil); err != nil {
			return fmt.Errorf("storage: azure copy from url %w abort previous %w", err, abortErr)
		}

		return nil
	})
}

type azureBlockInfo struct {
	blockID string
	index   int
}

func (a *AzureClient) multiPartCopy(ctx context.Context, srcCli *AzureClient, i CopyObjectInput) error {
	parts, err := splitIntoParts(i.SrcAttr.Length)
	if err != nil {
		return fmt.Errorf("storage: azure split into parts %w", err)
	}

	// Generate SAS token for source URL
	endpoint := strings.TrimSuffix(srcCli.cfg.Endpoint, ":443")
	srcBaseURL := fmt.Sprintf("https://%s.blob.%s/%s/%s", srcCli.cfg.Credential.AzureAccountName, endpoint, srcCli.cfg.Bucket, i.SrcAttr.Key)
	srcSAS, err := a.getSAS(ctx, srcCli)
	if err != nil {
		return fmt.Errorf("storage: azure get sas %w", err)
	}
	srcURL := srcBaseURL + "?" + srcSAS.Encode()

	blobCli := a.cli.ServiceClient().NewContainerClient(a.cfg.Bucket).NewBlockBlobClient(i.DestKey)

	blockInfos := make([]azureBlockInfo, 0, len(parts))
	var mu sync.Mutex
	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_maxCopyPartParallelism)

	for _, p := range parts {
		g.Go(func() error {
			// Block ID must be base64 encoded and all block IDs must have the same length
			blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("block-%08d", p.Index)))

			a.logger.Debug("stage block from url",
				zap.String("dest_key", i.DestKey),
				zap.String("block_id", blockID),
				zap.Int("index", p.Index),
				zap.Int64("offset", p.Offset),
				zap.Int64("size", p.Size))

			err := retry.Do(subCtx, func() error {
				_, err := blobCli.StageBlockFromURL(subCtx, blockID, srcURL, &blockblob.StageBlockFromURLOptions{
					Range: azblob.HTTPRange{Offset: p.Offset, Count: p.Size},
				})
				if err != nil {
					return fmt.Errorf("storage: azure stage block from url %w", err)
				}
				return nil
			})
			if err != nil {
				return err
			}

			mu.Lock()
			blockInfos = append(blockInfos, azureBlockInfo{blockID: blockID, index: p.Index})
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("storage: azure wait for stage block %w", err)
	}

	// Sort block IDs by index to maintain order
	sort.Slice(blockInfos, func(i, j int) bool {
		return blockInfos[i].index < blockInfos[j].index
	})

	blockIDs := make([]string, len(blockInfos))
	for i, info := range blockInfos {
		blockIDs[i] = info.blockID
	}

	// Commit all blocks
	_, err = blobCli.CommitBlockList(ctx, blockIDs, nil)
	if err != nil {
		return fmt.Errorf("storage: azure commit block list %w", err)
	}

	return nil
}

func (a *AzureClient) HeadObject(ctx context.Context, key string) (ObjectAttr, error) {
	resp, err := a.cli.ServiceClient().NewContainerClient(a.cfg.Bucket).NewBlobClient(key).
		GetProperties(ctx, nil)
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: azure get properties %w", err)
	}

	return ObjectAttr{Key: key, Length: *resp.ContentLength}, nil
}

func (a *AzureClient) GetObject(ctx context.Context, key string) (*Object, error) {
	blobCli := a.cli.ServiceClient().NewContainerClient(a.cfg.Bucket).NewBlockBlobClient(key)
	props, err := blobCli.GetProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: azure get properties %w", err)
	}

	return &Object{Body: &AzureReader{cli: blobCli, length: *props.ContentLength}, Length: *props.ContentLength}, nil
}

func (a *AzureClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	if _, err := a.cli.UploadStream(ctx, a.cfg.Bucket, i.Key, i.Body, nil); err != nil {
		return fmt.Errorf("storage: azure upload stream %w", err)
	}

	return nil
}

type AzureObjectFlatIterator struct {
	cli *AzureClient

	pager *runtime.Pager[azblob.ListBlobsFlatResponse]

	currPage []ObjectAttr
	nextIdx  int

	err error
}

func (flatIter *AzureObjectFlatIterator) HasNext() bool {
	// current page has more entries
	if flatIter.nextIdx < len(flatIter.currPage) {
		return true
	}

	// Notice: the first call to `pager.More()` returns `true` even if the current prefix has no content.
	if !flatIter.pager.More() {
		return false
	}

	// try to get next page
	page, err := flatIter.pager.NextPage(context.Background())
	if err != nil {
		flatIter.err = err
		return false
	}
	flatIter.currPage = flatIter.currPage[:0]
	flatIter.nextIdx = 0
	for _, blob := range page.Segment.BlobItems {
		attr := ObjectAttr{Key: *blob.Name, Length: *blob.Properties.ContentLength}
		flatIter.currPage = append(flatIter.currPage, attr)
	}

	// - In Azure's SDK, the first call to `pager.More()` returns `true` even if the current prefix has no content.
	// - `ListBlobsHierarchy` can yield empty pages (no blobs and no prefixes).
	// - To ensure the iterator reports `true` only when there is actual content, we keep fetching pages recursively
	//   until either:
	//   • `currPage` becomes non-empty, or
	//   • `pager.More()` returns `false`.
	if len(flatIter.currPage) == 0 {
		return flatIter.HasNext()
	}

	return true
}

func (flatIter *AzureObjectFlatIterator) Next() (ObjectAttr, error) {
	if flatIter.err != nil {
		return ObjectAttr{}, flatIter.err
	}

	attr := flatIter.currPage[flatIter.nextIdx]
	flatIter.nextIdx += 1

	return attr, nil
}

type AzureObjectHierarchyIterator struct {
	cli *AzureClient

	pager *runtime.Pager[container.ListBlobsHierarchyResponse]

	currPage []ObjectAttr
	nextIdx  int

	err error
}

func (hierIter *AzureObjectHierarchyIterator) HasNext() bool {
	// current page still has more entries
	if hierIter.nextIdx < len(hierIter.currPage) {
		return true
	}

	// Notice: the first call to `pager.More()` returns `true` even if the current prefix has no content.
	if !hierIter.pager.More() {
		return false
	}

	// try to get next page
	page, err := hierIter.pager.NextPage(context.Background())
	if err != nil {
		// put error into err field, it will be returned in next call of Next()
		// so we need to return true here, the caller will check err in Next()
		hierIter.err = err
		return true
	}
	hierIter.currPage = hierIter.currPage[:0]
	hierIter.nextIdx = 0
	for _, blob := range page.Segment.BlobItems {
		attr := ObjectAttr{Key: *blob.Name, Length: *blob.Properties.ContentLength}
		hierIter.currPage = append(hierIter.currPage, attr)
	}
	for _, prefix := range page.Segment.BlobPrefixes {
		hierIter.currPage = append(hierIter.currPage, ObjectAttr{Key: *prefix.Name})
	}

	// - In Azure's SDK, the first call to `pager.More()` returns `true` even if the current prefix has no content.
	// - `ListBlobsHierarchy` can yield empty pages (no blobs and no prefixes).
	// - To ensure the iterator reports `true` only when there is actual content, we keep fetching pages recursively
	//   until either:
	//   • `currPage` becomes non-empty, or
	//   • `pager.More()` returns `false`.
	if len(hierIter.currPage) == 0 {
		return hierIter.HasNext()
	}

	return true
}

func (hierIter *AzureObjectHierarchyIterator) Next() (ObjectAttr, error) {
	if hierIter.err != nil {
		return ObjectAttr{}, hierIter.err
	}

	attr := hierIter.currPage[hierIter.nextIdx]
	hierIter.nextIdx += 1

	return attr, nil
}

func (a *AzureClient) ListPrefix(_ context.Context, prefix string, recursive bool) (ObjectIterator, error) {
	if recursive {
		return a.listPrefixRecursive(prefix)
	}
	return a.listPrefixNonRecursive(prefix)
}

func (a *AzureClient) listPrefixRecursive(prefix string) (*AzureObjectFlatIterator, error) {
	pager := a.cli.NewListBlobsFlatPager(a.cfg.Bucket, &azblob.ListBlobsFlatOptions{Prefix: to.Ptr(prefix)})

	return &AzureObjectFlatIterator{cli: a, pager: pager}, nil
}

func (a *AzureClient) listPrefixNonRecursive(prefix string) (*AzureObjectHierarchyIterator, error) {
	pager := a.cli.ServiceClient().
		NewContainerClient(a.cfg.Bucket).
		NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{Prefix: to.Ptr(prefix)})

	return &AzureObjectHierarchyIterator{cli: a, pager: pager}, nil
}

func (a *AzureClient) DeleteObject(ctx context.Context, prefix string) error {
	if _, err := a.cli.DeleteBlob(ctx, a.cfg.Bucket, prefix, nil); err != nil {
		return fmt.Errorf("storage: azure delete blob %w", err)
	}

	return nil
}

func (a *AzureClient) BucketExist(ctx context.Context, _ string) (bool, error) {
	_, err := a.cli.ServiceClient().NewContainerClient(a.cfg.Bucket).
		GetProperties(ctx, &container.GetPropertiesOptions{})

	if err != nil {
		var azErr *azcore.ResponseError
		ok := errors.As(err, &azErr)
		if !ok {
			return false, fmt.Errorf("storage: azure get container properties %w", err)
		}

		if azErr.ErrorCode == string(bloberror.ContainerNotFound) {
			return false, nil
		}
	}

	return true, nil
}

func (a *AzureClient) CreateBucket(ctx context.Context) error {
	_, err := a.cli.ServiceClient().NewContainerClient(a.cfg.Bucket).Create(ctx, &azblob.CreateContainerOptions{})
	if err != nil {
		return fmt.Errorf("storage: azure create container %w", err)
	}

	return nil
}
