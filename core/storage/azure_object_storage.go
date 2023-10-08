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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

type AzureObjectStorage struct {
	Client *service.Client
	config *config
}

type AzureClient struct {
	cli *azblob.Client
}

func NewAzureClient(ctx context.Context, cfg *config) (*azblob.Client, error) {
	cred, err := azblob.NewSharedKeyCredential(cfg.accessKeyID, cfg.secretAccessKeyID)
	if err != nil {
		return nil, fmt.Errorf("storage: new azure shared key credential %w", err)
	}
	endpoint := fmt.Sprintf("https://%s.blob.core.windows.net", cfg.accessKeyID)
	cli, err := azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: new azure client %w", err)
	}

	return cli, nil
}

func newAzureObjectStorageWithConfig(ctx context.Context, c *config) (*AzureObjectStorage, error) {
	var client *service.Client
	var err error
	if c.useIAM {
		cred, credErr := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:      os.Getenv("AZURE_CLIENT_ID"),
			TenantID:      os.Getenv("AZURE_TENANT_ID"),
			TokenFilePath: os.Getenv("AZURE_FEDERATED_TOKEN_FILE"),
		})
		if credErr != nil {
			return nil, credErr
		}
		client, err = service.NewClient("https://"+c.accessKeyID+".blob."+c.address+"/", cred, &service.ClientOptions{})
	} else {
		connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		if connectionString == "" {
			connectionString = "DefaultEndpointsProtocol=https;AccountName=" + c.accessKeyID +
				";AccountKey=" + c.secretAccessKeyID + ";EndpointSuffix=" + c.address
		}
		client, err = service.NewClientFromConnectionString(connectionString, &service.ClientOptions{})
	}
	if err != nil {
		return nil, err
	}
	if c.bucketName == "" {
		return nil, fmt.Errorf("invalid bucket name")
	}
	// check valid in first query
	checkBucketFn := func() error {
		_, err := client.NewContainerClient(c.bucketName).GetProperties(ctx, &container.GetPropertiesOptions{})
		if err != nil {
			switch err := err.(type) {
			case *azcore.ResponseError:
				if c.createBucket && err.ErrorCode == string(bloberror.ContainerNotFound) {
					_, createErr := client.NewContainerClient(c.bucketName).Create(ctx, &azblob.CreateContainerOptions{})
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
	return &AzureObjectStorage{Client: client, config: c}, nil
}

func (aos *AzureObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	opts := azblob.DownloadStreamOptions{}
	if offset > 0 {
		opts.Range = azblob.HTTPRange{
			Offset: offset,
			Count:  size,
		}
	}
	object, err := aos.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).DownloadStream(ctx, &opts)

	if err != nil {
		return nil, err
	}
	return object.Body, nil
}

func (aos *AzureObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := aos.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).UploadStream(ctx, reader, &azblob.UploadStreamOptions{})
	return err
}

func (aos *AzureObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := aos.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return 0, err
	}
	return *info.ContentLength, nil
}

func (aos *AzureObjectStorage) ListObjects(ctx context.Context, bucketName string, prefix string, recursive bool) (map[string]int64, error) {
	pager := aos.Client.NewContainerClient(bucketName).NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	// pager := aos.Client.NewContainerClient(bucketName).NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
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
	_, err := aos.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).Delete(ctx, &blob.DeleteOptions{})
	return err
}

func (aos *AzureObjectStorage) CopyObject(ctx context.Context, fromBucketName, toBucketName, fromPath, toPath string) error {
	fromPathUrl := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", aos.config.accessKeyID, fromBucketName, fromPath)
	_, err := aos.Client.NewContainerClient(toBucketName).NewBlockBlobClient(toPath).StartCopyFromURL(ctx, fromPathUrl, nil)
	return err
}
