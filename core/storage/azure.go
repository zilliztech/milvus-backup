package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	"github.com/zilliztech/milvus-backup/internal/log"
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
	if cfg.UseIAM {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure default azure credential %w", err)
		}
		cli, err := azblob.NewClient(cfg.Endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure client %w", err)
		}
		sasCli, err := service.NewClient(cfg.Endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure service client %w", err)
		}

		return &AzureClient{cfg: cfg, cli: cli, sasCli: sasCli}, nil
	} else {
		cred, err := azblob.NewSharedKeyCredential(cfg.AK, cfg.SK)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure shared key credential %w", err)
		}
		cli, err := azblob.NewClientWithSharedKeyCredential(cfg.Endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("storage: new azure client %w", err)
		}
		sasCli, err := service.NewClientWithSharedKeyCredential(cfg.Endpoint, cred, nil)
		return &AzureClient{cfg: cfg, cli: cli, sasCli: sasCli}, nil
	}
}

type AzureClient struct {
	cfg Config

	cli *azblob.Client

	// sasCli is used to generate SAS token.
	// When we want to copy object under two different service accounts, AD auth is not supported.
	// So we need to use AD auth to generate SAS token and use SAS token to copy object.
	sasCli *service.Client
}

func (a *AzureClient) getSAS(ctx context.Context, srcCli *AzureClient) (*sas.QueryParameters, error) {
	if srcCli.cfg.UseIAM {
		return a.getSASByUserDelegation(ctx, srcCli)
	}
	return a.getSASBySharedKeyCredential(srcCli)
}

func (a *AzureClient) getSASBySharedKeyCredential(srcCli *AzureClient) (*sas.QueryParameters, error) {
	credential, err := azblob.NewSharedKeyCredential(srcCli.cfg.AK, srcCli.cfg.SK)
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
	// Set current and past time and create key
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

	srcURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", srcCli.cfg.AK, srcCli.cfg.Bucket, i.SrcKey)
	// if src and dest are in different account, we need to generate SAS token
	if a.cfg.AK != srcCli.cfg.AK {
		srcSAS, err := a.getSAS(ctx, srcCli)
		if err != nil {
			return err
		}
		srcURL += "?" + srcSAS.Encode()
	}

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

type AzureObjectIterator struct {
	cli *AzureClient

	pager     *runtime.Pager[azblob.ListBlobsFlatResponse]
	currPage  azblob.ListBlobsFlatResponse
	currIndex int
}

func (a *AzureObjectIterator) HasNext() bool {
	// current page has more entries
	if a.currIndex < len(a.currPage.Segment.BlobItems) {
		return true
	}

	// current page is the last page, try to get next page
	if !a.pager.More() {
		return false
	}

	page, err := a.pager.NextPage(context.Background())
	if err != nil {
		log.Warn("failed to get next page", zap.Error(err))
		return false
	}
	a.currPage = page
	a.currIndex = 0
	return true
}

func (a *AzureObjectIterator) Next() (ObjectAttr, error) {
	blob := a.currPage.Segment.BlobItems[a.currIndex]
	a.currIndex++

	return ObjectAttr{Key: *blob.Name, Length: *blob.Properties.ContentLength}, nil
}

func (a *AzureClient) ListPrefix(_ context.Context, prefix string, _ bool) (ObjectIterator, error) {
	// currently only support list prefix recursively
	pager := a.cli.NewListBlobsFlatPager(a.cfg.Bucket, &azblob.ListBlobsFlatOptions{Prefix: to.Ptr(prefix)})
	return &AzureObjectIterator{cli: a, pager: pager}, nil
}

func (a *AzureClient) DeleteObject(ctx context.Context, prefix string) error {
	if _, err := a.cli.DeleteBlob(ctx, a.cfg.Bucket, prefix, nil); err != nil {
		return fmt.Errorf("storage: azure delete blob %w", err)
	}

	return nil
}

func (a *AzureClient) BucketExist(ctx context.Context, prefix string) (bool, error) {
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
