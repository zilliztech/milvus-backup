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
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
	"go.uber.org/zap"
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

// Helper function to convert string to *string
func toPtr(s string) *string {
	return &s
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
	//log.Debug("GIFI ListObjects", zap.String("bucketName", bucketName), zap.String("prefix", prefix))
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
	// if aos.clients[fromBucketName].accessKeyID == aos.clients[toBucketName].accessKeyID {
	// 	fromPathUrl = fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", aos.clients[fromBucketName].accessKeyID, fromBucketName, fromPath)
	// 	blobCli = aos.clients[toBucketName].client.NewContainerClient(toBucketName).NewBlockBlobClient(toPath)
	// } else {
	srcSAS, err := aos.getSASupdated(fromBucketName)
	if err != nil {
		return err
	}
	fromPathUrl = fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s", aos.clients[fromBucketName].accessKeyID, fromBucketName, fromPath, srcSAS.Encode())
	blobCli = aos.clients[toBucketName].client.NewContainerClient(toBucketName).NewBlockBlobClient(toPath)
	// }
	// GIFI added from here
	// Check if fromPath is a folder or a file GIFI
	isDir, err := aos.checkPathType(ctx, fromBucketName, fromPath)
	if err != nil {
		return err
	}
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
	if isDir != "directory" {
		// log.Debug("GIFI FILE", zap.String("frompath", fromPath))
		if _, err := blobCli.CopyFromURL(ctx, fromPathUrl, nil); err != nil {
			return fmt.Errorf("storage: azure copy from url %w abort previous %w", err, abortErr)
		}
	}
	// GIFI END
	return nil
}

func (aos *AzureObjectStorage) getSAS(bucket string) (*sas.QueryParameters, error) {
	srcSvcCli := aos.clients[bucket].client
	// Set current and past time and create key
	now := time.Now().UTC().Add(-10 * time.Second)
	expiry := now.Add(48 * time.Hour)
	info := service.KeyInfo{
		Start:  to.Ptr(now.UTC().Format(sas.TimeFormat)),
		Expiry: to.Ptr(expiry.UTC().Format(sas.TimeFormat)),
	}
	udc, err := srcSvcCli.GetUserDelegationCredential(context.Background(), info, nil)
	if err != nil {
		log.Debug("GetUserDelegationCredential err", zap.Error(err))
		return nil, err
	} else {
		log.Debug("GetUserDelegationCredential no err")
	}
	// Create Blob Signature Values with desired permissions and sign with user delegation credential
	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(time.Second * -10),
		ExpiryTime:    time.Now().UTC().Add(time.Duration(sasSignMinute * time.Minute)),
		Permissions:   to.Ptr(sas.ContainerPermissions{Read: true, List: true}).String(),
		ContainerName: bucket,
	}.SignWithUserDelegation(udc)
	if err != nil {
		return nil, err
	}
	return &sasQueryParams, nil
}

func (aos *AzureObjectStorage) getSASupdated(bucket string) (*sas.QueryParameters, error) {
	//srcSvcCli := aos.clients[bucket].client
	credential, err := azblob.NewSharedKeyCredential(aos.clients[bucket].accessKeyID, aos.clients[bucket].secretAccessKeyID)
	if err != nil {
		log.Debug("Failed NewSharedKeyCredential", zap.Error(err))
	}
	sasQueryParams, err := sas.AccountSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour), // 48-hours before expiration,
		Permissions:   to.Ptr(sas.AccountPermissions{Read: true, List: true}).String(),
		ResourceTypes: to.Ptr(sas.AccountResourceTypes{Container: true, Object: true}).String(),
	}.SignWithSharedKey(credential)
	if err != nil {
		log.Debug("Failed AccountSignatureValues:", zap.Error(err))
		return nil, err
	}
	return &sasQueryParams, nil
}

func (aos *AzureObjectStorage) checkPathType(ctx context.Context, bucketName, path string) (string, error) {
	containerClient := aos.clients[bucketName].client.NewContainerClient(bucketName)

	// Ensure the prefix ends with a slash for directory check
	directoryPrefix := path
	if !strings.HasSuffix(directoryPrefix, "/") {
		directoryPrefix += "/"
	}

	// List blobs with the specified prefix
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: toPtr(directoryPrefix),
	})

	hasBlobs := false
	for pager.More() {
		pageResp, err := pager.NextPage(ctx)
		if err != nil {
			return "", err // Error while fetching the next page
		}

		// Iterate over BlobItems in the response
		for _, blobItem := range pageResp.Segment.BlobItems {
			// Dereference the pointer to string
			if blobItem.Name != nil && strings.HasPrefix(*blobItem.Name, directoryPrefix) {
				hasBlobs = true
				break
			}
		}
		if hasBlobs {
			break // Found at least one blob, path is a directory
		}
	}

	// If no blobs found with the prefix, check if the path itself is a blob
	if !hasBlobs {
		// Check if the path exists as a blob (without trailing slash)
		blobClient := containerClient.NewBlockBlobClient(path)
		_, err := blobClient.GetProperties(ctx, nil)
		if err != nil {
			if strings.Contains(err.Error(), "404") {
				return "not found", fmt.Errorf("path does not exist")
			}
			return "", err // Some other error occurred
		}
		return "file", nil // Path exists as a file
	}
	return "directory", nil // Path is a directory
}
