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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type GCPNativeChunkManager struct {
	client *storage.Client
	config *StorageConfig
}

func (gcm *GCPNativeChunkManager) Config() *StorageConfig {
	return gcm.config
}

func newGCPNativeChunkManagerWithConfig(ctx context.Context, config *StorageConfig) (*GCPNativeChunkManager, error) {
	var client *storage.Client
	var err error
	var opts []option.ClientOption
	var projectId string
	if config.Address != "" {
		complete_address := "https://"
		if !config.UseSSL {
			complete_address = "http://"
		}
		complete_address = complete_address + config.Address + "/storage/v1/"
		opts = append(opts, option.WithEndpoint(complete_address))
	}
	// Read the credentials file
	jsonData, err := os.ReadFile(config.GcpCredentialJSON)
	if err != nil {
		return nil, fmt.Errorf("unable to read credentials file:: %v", err)
	}

	creds, err := google.CredentialsFromJSON(ctx, jsonData, storage.ScopeReadWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to create credentials from JSON: %v", err)

	}
	projectId, err = getProjectId(config.GcpCredentialJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to get project ID: %v", err)
	}
	opts = append(opts, option.WithCredentials(creds))

	client, err = storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %v", err)
	}

	if config.BucketName == "" {
		return nil, fmt.Errorf("invalid empty bucket name")
	}
	// Check bucket validity
	checkBucketFn := func() error {
		bucket := client.Bucket(config.BucketName)
		_, err := bucket.Attrs(ctx)
		if err == storage.ErrBucketNotExist && config.CreateBucket {
			log.Info("gcs bucket does not exist, create bucket.", zap.String("bucket name", config.BucketName))
			err = client.Bucket(config.BucketName).Create(ctx, projectId, nil)
			if err != nil {
				return fmt.Errorf("failed to create bucket: %v", err)
			}
			return nil
		}
		return err
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, fmt.Errorf("bucket check failed: %v", err)
	}

	log.Info("GCP native chunk manager init success.", zap.String("bucketname", config.BucketName))
	return &GCPNativeChunkManager{
		client: client,
		config: config,
	}, nil
}

func getProjectId(credentialPath string) (string, error) {
	if credentialPath == "" {
		return "", errors.New("the path to the JSON file is empty")
	}

	// Read the credentials file
	jsonData, err := os.ReadFile(credentialPath)
	if err != nil {
		return "", fmt.Errorf("failed to read credentials file: %v", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return "", errors.New("failed to parse Google Cloud credentials as JSON")
	}

	propertyValue, ok := data["project_id"]
	if !ok {
		return "", errors.New("project_id doesn't exist")
	}

	projectId := fmt.Sprintf("%v", propertyValue)
	return projectId, nil
}

// Write writes data to filePath in the specified GCS bucket
func (gcm *GCPNativeChunkManager) Write(ctx context.Context, bucketName string, filePath string, content []byte) error {
	wc := gcm.client.Bucket(bucketName).Object(filePath).NewWriter(ctx)
	defer func() {
		if err := wc.Close(); err != nil {
			log.Error("Failed to close writer", zap.Error(err))
		}
	}()

	_, err := wc.Write(content)
	if err != nil {
		return err
	}
	return nil
}

// Exist checks whether chunk is saved to GCS storage.
func (gcm *GCPNativeChunkManager) Exist(ctx context.Context, bucketName string, filePath string) (bool, error) {
	objectKeys, _, err := gcm.ListWithPrefix(ctx, bucketName, filePath, false)
	if err != nil {
		return false, err
	}

	if len(objectKeys) > 0 {
		return true, nil
	}

	return false, nil
}

// Read reads the storage data if exists.
func (gcm *GCPNativeChunkManager) Read(ctx context.Context, bucketName string, filePath string) ([]byte, error) {
	rc, err := gcm.client.Bucket(bucketName).Object(filePath).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error("Failed to close reader", zap.Error(err))
		}
	}()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// ListWithPrefix returns objects with provided prefix.
func (gcm *GCPNativeChunkManager) ListWithPrefix(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []int64, error) {
	var objectKeys []string
	var sizes []int64
	delimiter := ""
	if !recursive {
		delimiter = "/"
	}

	it := gcm.client.Bucket(bucketName).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: delimiter,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list objects: %w", err)
		}
		// Check if it's an object (not a directory)
		if attrs.Name != "" {
			objectKeys = append(objectKeys, attrs.Name)
			sizes = append(sizes, attrs.Size)
		} else if attrs.Prefix != "" {
			// If recursive is false, directories are handled via attrs.Prefix (not attrs.Name)
			// For directories, we only add the directory name (attrs.Prefix) once
			objectKeys = append(objectKeys, attrs.Prefix)
			sizes = append(sizes, 0) // No size for directories
		}
	}
	return objectKeys, sizes, nil
}

// Remove deletes an object with @key.
func (gcm *GCPNativeChunkManager) Remove(ctx context.Context, bucketName string, filePath string) error {
	err := gcm.client.Bucket(bucketName).Object(filePath).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (gcm *GCPNativeChunkManager) RemoveWithPrefix_OLD(ctx context.Context, bucketName string, prefix string) error {
	objectKeys, _, err := gcm.ListWithPrefix(ctx, bucketName, prefix, false)
	if err != nil {
		return err
	}
	// Group objects by their depth (number of / in the key)
	groupedByLevel := make(map[int][]string)
	var maxLevel int
	for _, key := range objectKeys {
		level := strings.Count(key, "/")
		groupedByLevel[level] = append(groupedByLevel[level], key)
		if level > maxLevel {
			maxLevel = level
		}
	}

	for level := maxLevel; level >= 0; level-- {
		// Get the objects at this level
		keysAtLevel, exists := groupedByLevel[level]
		if !exists || len(keysAtLevel) == 0 {
			continue
		}

		// Dynamically adjust maxGoroutines based on the number of objects at this level
		maxGoroutines := 10
		if len(keysAtLevel) < maxGoroutines {
			maxGoroutines = len(keysAtLevel)
		}
		i := 0
		for i < len(keysAtLevel) {
			runningGroup, groupCtx := errgroup.WithContext(context.Background())
			for j := 0; j < maxGoroutines && i < len(keysAtLevel); j++ {
				key := keysAtLevel[i]
				runningGroup.Go(func(key string) func() error {
					return func() error {
						err := gcm.Remove(groupCtx, bucketName, key)
						if err != nil {
							log.Warn("failed to remove object", zap.String("bucket", bucketName), zap.String("path", key), zap.Error(err))
							return err
						}
						return nil
					}
				}(key))
				i++
			}
			if err := runningGroup.Wait(); err != nil {
				return err
			}
		}
	}
	err = gcm.Remove(ctx, bucketName, strings.TrimSuffix(prefix, "/"))
	if err != nil {
		log.Warn("failed to remove object", zap.String("bucket", bucketName), zap.String("path", prefix), zap.Error(err))
		return err
	}
	return nil
}

func (gcm *GCPNativeChunkManager) RemoveWithPrefix(ctx context.Context, bucketName string, prefix string) error {
	objectKeys, _, err := gcm.ListWithPrefix(ctx, bucketName, prefix, false)
	if err != nil {
		return err
	}
	// Remove objects in parallel
	i := 0
	maxGoroutine := 10
	for i < len(objectKeys) {
		runningGroup, groupCtx := errgroup.WithContext(ctx)
		for j := 0; j < maxGoroutine && i < len(objectKeys); j++ {
			key := objectKeys[i]
			runningGroup.Go(func() error {
				err := gcm.Remove(groupCtx, bucketName, key)
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

// Copy files from fromPath into toPath recursively
func (gcm *GCPNativeChunkManager) Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error {
	objectKeys, _, err := gcm.ListWithPrefix(ctx, fromBucketName, fromPath, true)
	if err != nil {
		log.Error("Error listing objects", zap.Error(err))
		return err
	}
	for _, srcObjectKey := range objectKeys {
		dstObjectKey := strings.Replace(srcObjectKey, fromPath, toPath, 1)

		srcReader, err := gcm.client.Bucket(fromBucketName).Object(srcObjectKey).NewReader(ctx)
		if err != nil {
			log.Error("Error creating reader for object", zap.String("srcObjectKey", srcObjectKey), zap.Error(err))
			return err
		}

		defer func(srcObjectKey string, srcReader *storage.Reader) {
			if err := srcReader.Close(); err != nil {
				log.Error("Error closing source reader", zap.String("srcObjectKey", srcObjectKey), zap.Error(err))
			}
		}(srcObjectKey, srcReader)

		dstWriter := gcm.client.Bucket(toBucketName).Object(dstObjectKey).NewWriter(ctx)
		defer func(dstObjectKey string, dstWriter *storage.Writer) {
			if err := dstWriter.Close(); err != nil {
				log.Error("Error closing destination writer", zap.String("dstObjectKey", dstObjectKey), zap.Error(err))
			}
		}(dstObjectKey, dstWriter)

		if _, err := io.Copy(dstWriter, srcReader); err != nil {
			log.Error("Error copying object", zap.String("from", srcObjectKey), zap.String("to", dstObjectKey), zap.Error(err))
			return err
		}
	}
	return nil
}

func (gcm *GCPNativeChunkManager) ListObjectsPage(ctx context.Context, bucket, prefix string) (ListObjectsPaginator, error) {
	panic("not implemented")
}

func (gcm *GCPNativeChunkManager) HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error) {
	panic("not implemented")
}

func (gcm *GCPNativeChunkManager) GetObject(ctx context.Context, bucketName, key string) (*Object, error) {
	panic("not implemented")
}

func (gcm *GCPNativeChunkManager) UploadObject(ctx context.Context, i UploadObjectInput) error {
	panic("not implemented")
}
