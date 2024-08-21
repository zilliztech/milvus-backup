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
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/errorutil"
)

// AzureChunkManager is responsible for read and write data stored in minio.
type AzureChunkManager struct {
	aos *AzureObjectStorage

	config *StorageConfig
	//cli *azblob.Client
	//	ctx        context.Context
	//BucketName string
	//RootPath   string
}

func (mcm *AzureChunkManager) UploadObject(ctx context.Context, i UploadObjectInput) error {
	//TODO implement me
	panic("implement me")
}

func (mcm *AzureChunkManager) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	//TODO implement me
	panic("implement me")
}

func (mcm *AzureChunkManager) HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error) {
	//TODO implement me
	panic("implement me")
}

func (mcm *AzureChunkManager) ListObjectsPage(ctx context.Context, bucket, prefix string) (ListObjectsPaginator, error) {
	//TODO implement me
	panic("implement me")
}

var _ ChunkManager = (*AzureChunkManager)(nil)

func NewAzureChunkManager(ctx context.Context, config *StorageConfig) (*AzureChunkManager, error) {
	aos, err := newAzureObjectStorageWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	//cli, err := NewAzureClient(ctx, c)
	//if err != nil {
	//	return nil, err
	//}
	mcm := &AzureChunkManager{
		aos:    aos,
		config: config,
		//cli:        cli,
		//BucketName: c.BucketName,
		//RootPath:   strings.TrimLeft(c.RootPath, "/"),
	}
	log.Info("Azure chunk manager init success.")
	return mcm, nil
}

func (mcm *AzureChunkManager) Config() *StorageConfig {
	return mcm.config
}

func (mcm *AzureChunkManager) Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error {
	objectkeys, _, err := mcm.ListWithPrefix(ctx, fromBucketName, fromPath, true)
	if err != nil {
		log.Warn("listWithPrefix error", zap.String("prefix", fromPath), zap.Error(err))
		return err
	}
	for _, objectkey := range objectkeys {
		dstObjectKey := strings.Replace(objectkey, fromPath, toPath, 1)
		err := mcm.aos.CopyObject(ctx, fromBucketName, toBucketName, objectkey, dstObjectKey)
		if err != nil {
			log.Error("copyObject error", zap.String("srcObjectKey", objectkey), zap.String("dstObjectKey", dstObjectKey), zap.Error(err))
			return err
		}
	}
	return nil
}

// Path returns the path of minio data if exists.
func (mcm *AzureChunkManager) Path(ctx context.Context, bucketName string, filePath string) (string, error) {
	exist, err := mcm.Exist(ctx, bucketName, filePath)
	if err != nil {
		return "", err
	}
	if !exist {
		return "", errors.New("minio file manage cannot be found with filePath:" + filePath)
	}
	return filePath, nil
}

// Reader returns the path of minio data if exists.
func (mcm *AzureChunkManager) Reader(ctx context.Context, bucketName string, filePath string) (FileReader, error) {
	reader, err := mcm.getObject(ctx, bucketName, filePath, int64(0), int64(0))
	if err != nil {
		log.Warn("failed to get object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return reader, nil
}

func (mcm *AzureChunkManager) Size(ctx context.Context, bucketName string, filePath string) (int64, error) {
	objectInfo, err := mcm.getObjectSize(ctx, bucketName, filePath)
	if err != nil {
		log.Warn("failed to stat object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return 0, err
	}

	return objectInfo, nil
}

// Write writes the data to minio storage.
func (mcm *AzureChunkManager) Write(ctx context.Context, bucketName string, filePath string, content []byte) error {
	err := mcm.putObject(ctx, bucketName, filePath, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		log.Warn("failed to put object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}

	return nil
}

// MultiWrite saves multiple objects, the path is the key of @kvs.
// The object value is the value of @kvs.
func (mcm *AzureChunkManager) MultiWrite(ctx context.Context, bucketName string, kvs map[string][]byte) error {
	var el error
	for key, value := range kvs {
		err := mcm.Write(ctx, bucketName, key, value)
		if err != nil {
			el = errors.New(fmt.Sprintf("failed to write %s", key))
		}
	}
	return el
}

// Exist checks whether chunk is saved to minio storage.
func (mcm *AzureChunkManager) Exist(ctx context.Context, bucketName string, filePath string) (bool, error) {
	objs, err := mcm.aos.ListObjects(ctx, bucketName, filePath, true)
	if err != nil {
		if IsErrNoSuchKey(err) {
			return false, nil
		}
		log.Warn("failed to stat object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return false, err
	}
	if len(objs) > 0 {
		return true, nil
	} else {
		return false, nil
	}
}

// Read reads the minio storage data if exists.
func (mcm *AzureChunkManager) Read(ctx context.Context, bucketName string, filePath string) ([]byte, error) {
	object, err := mcm.getObject(ctx, bucketName, filePath, int64(0), int64(0))
	if err != nil {
		log.Warn("failed to get object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	// Prefetch object data
	var empty []byte
	_, err = object.Read(empty)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	size, err := mcm.getObjectSize(ctx, bucketName, filePath)
	if err != nil {
		log.Warn("failed to stat object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	data, err := Read(object, size)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, nil
}

func (mcm *AzureChunkManager) MultiRead(ctx context.Context, bucketName string, keys []string) ([][]byte, error) {
	var el error
	var objectsValues [][]byte
	for _, key := range keys {
		objectValue, err := mcm.Read(ctx, bucketName, key)
		if err != nil {
			el = errors.New(fmt.Sprintf("failed to read %s %s", bucketName, key))
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, el
}

func (mcm *AzureChunkManager) ReadWithPrefix(ctx context.Context, bucketName string, prefix string) ([]string, [][]byte, error) {
	objectsKeys, _, err := mcm.ListWithPrefix(ctx, bucketName, prefix, true)
	if err != nil {
		return nil, nil, err
	}
	objectsValues, err := mcm.MultiRead(ctx, bucketName, objectsKeys)
	if err != nil {
		return nil, nil, err
	}

	return objectsKeys, objectsValues, nil
}

// Remove deletes an object with @key.
func (mcm *AzureChunkManager) Remove(ctx context.Context, bucketName string, filePath string) error {
	err := mcm.removeObject(ctx, bucketName, filePath)
	if err != nil {
		log.Warn("failed to remove object", zap.String("bucket", bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}
	return nil
}

// MultiRemove deletes a objects with @keys.
func (mcm *AzureChunkManager) MultiRemove(ctx context.Context, bucketName string, keys []string) error {
	var el errorutil.ErrorList
	for _, key := range keys {
		err := mcm.Remove(ctx, bucketName, key)
		if err != nil {
			el = append(el, err)
		}
	}
	if len(el) == 0 {
		return nil
	}
	return el
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from minio.
func (mcm *AzureChunkManager) RemoveWithPrefix(ctx context.Context, bucketName string, prefix string) error {
	objects, err := mcm.listObjects(ctx, bucketName, prefix, true)
	if err != nil {
		return err
	}
	removeKeys := make([]string, 0)
	for key := range objects {
		removeKeys = append(removeKeys, key)
	}
	i := 0
	maxGoroutine := 10
	for i < len(removeKeys) {
		runningGroup, groupCtx := errgroup.WithContext(ctx)
		for j := 0; j < maxGoroutine && i < len(removeKeys); j++ {
			key := removeKeys[i]
			runningGroup.Go(func() error {
				err := mcm.removeObject(groupCtx, bucketName, key)
				if err != nil {
					log.Warn("failed to remove object", zap.String("bucket", bucketName), zap.String("path", key), zap.Error(err))
					return err
				}
				return nil
			})
			i++
		}
		if err := runningGroup.Wait(); err != nil {
			return err
		}
	}
	return nil
}

// ListWithPrefix returns objects with provided prefix.
func (mcm *AzureChunkManager) ListWithPrefix(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []int64, error) {
	objects, err := mcm.listObjects(ctx, bucketName, prefix, true)
	if err != nil {
		return nil, nil, err
	}
	if recursive {
		var objectsKeys []string
		var sizes []int64
		for object, contentLength := range objects {
			objectsKeys = append(objectsKeys, object)
			sizes = append(sizes, contentLength)
		}
		return objectsKeys, sizes, nil
	} else {
		var objectsKeys []string
		sizesDict := make(map[string]int64, 0)
		objectsKeysDict := make(map[string]bool, 0)
		for object, size := range objects {
			keyWithoutPrefix := strings.Replace(object, prefix, "", 1)
			if strings.Contains(keyWithoutPrefix, "/") {
				var key string
				if strings.HasPrefix(keyWithoutPrefix, "/") {
					key = prefix + "/" + strings.Split(keyWithoutPrefix, "/")[1] + "/"
				} else {
					key = prefix + strings.Split(keyWithoutPrefix, "/")[0] + "/"
				}
				if _, exist := objectsKeysDict[key]; !exist {
					objectsKeys = append(objectsKeys, key)
					sizesDict[key] = size
					objectsKeysDict[key] = true
				} else {
					sizesDict[key] = size + sizesDict[key]
				}
			} else {
				key := prefix + keyWithoutPrefix
				if _, exist := objectsKeysDict[key]; !exist {
					objectsKeys = append(objectsKeys, key)
					sizesDict[key] = size
					objectsKeysDict[key] = true
				} else {
					sizesDict[key] = size + sizesDict[key]
				}
			}
		}
		var sizes []int64
		for _, objectKey := range objectsKeys {
			sizes = append(sizes, sizesDict[objectKey])
		}

		return objectsKeys, sizes, nil
	}
}

func (mcm *AzureChunkManager) getObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	//resp, err := mcm.cli.DownloadStream(ctx, BucketName, objectName, nil)
	//if err != nil {
	//	return nil, fmt.Errorf("storage: azure download stream %w", err)
	//}
	//return resp.Body, nil

	reader, err := mcm.aos.GetObject(ctx, bucketName, objectName, offset, size)
	switch err := err.(type) {
	case *azcore.ResponseError:
		if err.ErrorCode == string(bloberror.BlobNotFound) {
			return nil, WrapErrNoSuchKey(objectName)
		}
		//case minio.ErrorResponse:
		//	if err.Code == "NoSuchKey" {
		//		return nil, WrapErrNoSuchKey(objectName)
		//	}
	}
	return reader, err
}

func (mcm *AzureChunkManager) putObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	err := mcm.aos.PutObject(ctx, bucketName, objectName, reader, objectSize)
	return err
}

func (mcm *AzureChunkManager) getObjectSize(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := mcm.aos.StatObject(ctx, bucketName, objectName)

	switch err := err.(type) {
	case *azcore.ResponseError:
		if err.ErrorCode == string(bloberror.BlobNotFound) {
			return info, WrapErrNoSuchKey(objectName)
		}
		//case minio.ErrorResponse:
		//	if err.Code == "NoSuchKey" {
		//		return nil, WrapErrNoSuchKey(objectName)
		//	}
	}

	return info, err
}

func (mcm *AzureChunkManager) listObjects(ctx context.Context, bucketName string, prefix string, recursive bool) (map[string]int64, error) {
	res, err := mcm.aos.ListObjects(ctx, bucketName, prefix, recursive)
	return res, err
}

func (mcm *AzureChunkManager) removeObject(ctx context.Context, bucketName, objectName string) error {
	err := mcm.aos.RemoveObject(ctx, bucketName, objectName)
	return err
}
