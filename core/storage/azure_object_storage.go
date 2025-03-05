// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

const sasSignMinute = 60

type innerAzureClient struct {
	client *service.Client

	bucketName        string
	accessKeyID       string
	secretAccessKeyID string
	createBucket      bool
}

type AzureObjectStorage struct {
	//Client *service.Client
	clients map[string]*innerAzureClient
	//StorageConfig *StorageConfig
}

//func NewAzureClient(ctx context.Context, cfg *StorageConfig) (*azblob.Client, error) {
//	cred, err := azblob.NewSharedKeyCredential(cfg.AccessKeyID, cfg.SecretAccessKeyID)
//	if err != nil {
//		return nil, fmt.Errorf("storage: new azure shared key credential %w", err)
//	}
//	endpoint := fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AccessKeyID)
//	cli, err := azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
//	if err != nil {
//		return nil, fmt.Errorf("storage: new azure aos %w", err)
//	}
//
//	return cli, nil
//}

func newAzureObjectStorageWithConfig(ctx context.Context, c *StorageConfig) (*AzureObjectStorage, error) {
	client, err := newAzureObjectClient(ctx, c.Address, c.AccessKeyID, c.SecretAccessKeyID, c.BucketName, c.UseIAM, c.CreateBucket)
	if err != nil {
		return nil, err
	}
	backupClient, err := newAzureObjectClient(ctx, c.Address, c.backupAccessKeyID, c.backupSecretAccessKeyID, c.backupBucketName, c.UseIAM, c.CreateBucket)
	if err != nil {
		return nil, err
	}
	clients := map[string]*innerAzureClient{
		c.BucketName:       client,
		c.backupBucketName: backupClient,
	}
	return &AzureObjectStorage{
		clients: clients,
		//StorageConfig: c,
	}, nil
}

func newAzureObjectClient(ctx context.Context, address, accessKeyID, secretAccessKeyID, bucketName string, useIAM, createBucket bool) (*innerAzureClient, error) {
	var client *service.Client
	var err error
	if useIAM {
		cred, credErr := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:      os.Getenv("AZURE_CLIENT_ID"),
			TenantID:      os.Getenv("AZURE_TENANT_ID"),
			TokenFilePath: os.Getenv("AZURE_FEDERATED_TOKEN_FILE"),
		})
		if credErr != nil {
			return nil, credErr
		}
		client, err = service.NewClient("https://"+accessKeyID+".blob."+address+"/", cred, &service.ClientOptions{})
	} else {
		connectionString := "DefaultEndpointsProtocol=https;AccountName=" + accessKeyID +
			";AccountKey=" + secretAccessKeyID + ";EndpointSuffix=" + address
		client, err = service.NewClientFromConnectionString(connectionString, &service.ClientOptions{})
	}
	if err != nil {
		return nil, err
	}
	if bucketName == "" {
		return nil, fmt.Errorf("invalid bucket name")
	}
	// check valid in first query
	checkBucketFn := func() error {
		_, err := client.NewContainerClient(bucketName).GetProperties(ctx, &container.GetPropertiesOptions{})
		if err != nil {
			switch err := err.(type) {
			case *azcore.ResponseError:
				if createBucket && err.ErrorCode == string(bloberror.ContainerNotFound) {
					_, createErr := client.NewContainerClient(bucketName).Create(ctx, &azblob.CreateContainerOptions{})
					if createErr != nil {
						return createErr
					}
					return nil
				}
			}
		}
		return err
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return &innerAzureClient{
		client:            client,
		bucketName:        bucketName,
		accessKeyID:       accessKeyID,
		secretAccessKeyID: secretAccessKeyID,
		createBucket:      createBucket,
	}, nil
}

func (aos *AzureObjectStorage) getClient(ctx context.Context, bucketName string) *service.Client {
	return aos.clients[bucketName].client
}

func (aos *AzureObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	opts := azblob.DownloadStreamOptions{}
	if offset > 0 {
		opts.Range = azblob.HTTPRange{
			Offset: offset,
			Count:  size,
		}
	}
	object, err := aos.clients[bucketName].client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).DownloadStream(ctx, &opts)

	if err != nil {
		return nil, err
	}
	return object.Body, nil
}

func (aos *AzureObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := aos.clients[bucketName].client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).UploadStream(ctx, reader, &azblob.UploadStreamOptions{})
	return err
}

func (aos *AzureObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := aos.clients[bucketName].client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return 0, err
	}
	return *info.ContentLength, nil
}

func (aos *AzureObjectStorage) ListObjects(ctx context.Context, bucketName string, prefix string, recursive bool) (map[string]int64, error) {
	pager := aos.clients[bucketName].client.NewContainerClient(bucketName).NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	// pager := aos.Client.NewContainerClient(BucketName).NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
	// 	Prefix: &prefix,
	// })

	objects := map[string]int64{}
	if pager.More() {
		pageResp, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, err
		}
		for _, blob := range pageResp.Segment.BlobItems {
			objects[*blob.Name] = *blob.Properties.ContentLength
		}
	}
	return objects, nil
}

func (aos *AzureObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	_, err := aos.clients[bucketName].client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).Delete(ctx, &blob.DeleteOptions{})
	return err
}

func (aos *AzureObjectStorage) CopyObject(ctx context.Context, fromBucketName, toBucketName, fromPath, toPath string) error {
	var blobCli *blockblob.Client
	var fromPathUrl string
	srcSAS, err := aos.getSAS(fromBucketName)
	if err != nil {
		return err
	}
	fromPathUrl = fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s", aos.clients[fromBucketName].accessKeyID, fromBucketName, fromPath, srcSAS.Encode())
	blobCli = aos.clients[toBucketName].client.NewContainerClient(toBucketName).NewBlockBlobClient(toPath)

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

	// Check if fromPath is a folder or a file
	isFile, err := aos.isFile(ctx, fromBucketName, fromPath)
	if err != nil {
		return err
	}
	if isFile {
		if _, err := blobCli.CopyFromURL(ctx, fromPathUrl, nil); err != nil {
			return fmt.Errorf("storage: azure copy from url %w abort previous %w", err, abortErr)
		}
	}
	return nil
}

func (aos *AzureObjectStorage) getSAS(bucket string) (*sas.QueryParameters, error) {
	credential, err := azblob.NewSharedKeyCredential(aos.clients[bucket].accessKeyID, aos.clients[bucket].secretAccessKeyID)
	if err != nil {
		return nil, err
	}
	sasQueryParams, err := sas.AccountSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour), // 48-hours before expiration,
		Permissions:   to.Ptr(sas.AccountPermissions{Read: true, List: true}).String(),
		ResourceTypes: to.Ptr(sas.AccountResourceTypes{Container: true, Object: true}).String(),
	}.SignWithSharedKey(credential)
	if err != nil {
		return nil, err
	}
	return &sasQueryParams, nil
}

func (aos *AzureObjectStorage) isFile(ctx context.Context, bucketName, path string) (bool, error) {
	// Prefix ends with a slash for directory-like behavior
	directoryPrefix := path
	if !strings.HasSuffix(directoryPrefix, "/") {
		directoryPrefix += "/"
	}

	objects, err := aos.ListObjects(ctx, bucketName, directoryPrefix, false)
	if err != nil {
		return false, err
	}
	// If ListObjects called with end file name, it returns file name itself
	if len(objects) == 1 {
		if _, found := objects[path]; found {
			return true, nil
		}
	}
	return false, nil
}
