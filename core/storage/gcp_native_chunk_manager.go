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
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type GCPNativeChunkManager struct {
	client *storage.Client
	config *StorageConfig
}

// type GCSReader struct {
//	reader   *storage.Reader
//	obj      *storage.ObjectHandle
//	position int64
// }

func (gcm *GCPNativeChunkManager) Config() *StorageConfig {
	return gcm.config
}

func newGCPNativeChunkManagerWithConfig(ctx context.Context, config *StorageConfig) (*GCPNativeChunkManager, error) {
	var client *storage.Client
	var err error
	// log.Debug("GIFI Inside newGCPNativeChunkManagerWithConfig", zap.String("config.GcpCredentialJSON", config.GcpCredentialJSON))
	var opts []option.ClientOption
	var projectId string
	if config.Address != "" {
		complete_address := "http://"
		if config.UseSSL {
			complete_address = "https://"
		}
		complete_address = complete_address + config.Address + "/storage/v1/"
		opts = append(opts, option.WithEndpoint(complete_address))
	}
	// if config.gcpNativeWithoutAuth {
	// 	opts = append(opts, option.WithoutAuthentication())
	// } else {
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
	// }

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
		if err == storage.ErrBucketNotExist /* && c.createBucket */ {
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
	query := &storage.Query{
		Prefix:    filePath,
		Delimiter: "/",
	}
	it := gcm.client.Bucket(bucketName).Objects(ctx, query)
	_, err := it.Next()

	if err == iterator.Done {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// Read reads the storage data if exists.
func (gcm *GCPNativeChunkManager) Read(ctx context.Context, bucketName string, filePath string) ([]byte, error) {
	rc, err := gcm.client.Bucket(bucketName).Object(filePath).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

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
		// When recursive is false, we should only skip objects where attrs.Name is empty (i.e., directories)
		// Check if it's an object (not a directory)
		if recursive || attrs.Name != "" {
			objectKeys = append(objectKeys, attrs.Name)
			sizes = append(sizes, attrs.Size)
		} else if !recursive && attrs.Prefix != "" {
			// If recursive is false, directories are handled via attrs.Prefix (not attrs.Name)
			// For directories, we only add the directory name (attrs.Prefix) once
			objectKeys = append(objectKeys, attrs.Prefix)
			sizes = append(sizes, 0) // No size for directories
		}
	}
	//log.Debug("ListWithPrefix", zap.Any("objectKeys", objectKeys))
	return objectKeys, sizes, nil
}

func (gcm *GCPNativeChunkManager) ListWithPrefixOld(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []int64, error) {
	var objectNames []string
	var sizes []int64
	query := &storage.Query{Prefix: prefix}
	//recursive = true
	if !recursive {
		query.Delimiter = "/"
	}
	log.Debug("GIFI ListWithPrefix", zap.Any("query.Prefix", query.Prefix))
	it := gcm.client.Bucket(bucketName).Objects(ctx, query)
	for {
		attr, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list objects: %w", err)
		} else {
			log.Debug("GIFI-list objects success", zap.Any("attr", attr))
		}
		// When recursive = false, we use attr.Prefix to check for directories
		if len(attr.Prefix) > 0 {
			// If recursive is false and it's a directory, append the directory name
			log.Debug("GIFI-list attr.Prefix")
			objectNames = append(objectNames, attr.Prefix)
			sizes = append(sizes, 0) // No size for directories
		}

		// If recursive is true, we use attr.Name to check for objects
		if len(attr.Name) >= len(prefix) {
			log.Debug("GIFI-list attr.Name")
			if recursive || strings.Count(attr.Name[len(prefix):], "/") == 0 {
				if strings.Count(attr.Name[len(prefix):], "/") == 0 {
					objectNames = append(objectNames, attr.Name)
					sizes = append(sizes, attr.Size)
				}
			}
		}
		// if len(attr.Name) >= len(prefix) && (recursive || strings.Count(attr.Name[len(prefix):], "/") == 0) {
		// 	objectNames = append(objectNames, attr.Name)
		// 	sizes = append(sizes, attr.Size)
		// }
	}

	// Check if the lengths match
	if len(objectNames) != len(sizes) {
		return nil, nil, fmt.Errorf("mismatched lengths: %d objects and %d sizes", len(objectNames), len(sizes))
	}
	log.Debug("GIFI", zap.Any("objectNames", objectNames))
	return objectNames, sizes, nil
}

// Remove deletes an object with @key.
func (gcm *GCPNativeChunkManager) Remove(ctx context.Context, bucketName string, filePath string) error {
	err := gcm.client.Bucket(bucketName).Object(filePath).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from storage.
func (gcm *GCPNativeChunkManager) RemoveWithPrefix(ctx context.Context, bucketName string, prefix string) error {
	objects := gcm.client.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: prefix})
	var deletionErrors []error

	for {
		attr, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		err = gcm.Remove(ctx, bucketName, attr.Name)
		if err != nil {
			deletionErrors = append(deletionErrors, err)
		}
	}

	if len(deletionErrors) > 0 {
		return fmt.Errorf("failed to remove some objects: %v", deletionErrors) // Return aggregated errors if any
	}
	return nil
}

// Copy files from fromPath into toPath recursively
func (gcm *GCPNativeChunkManager) Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error {
	bkt := gcm.client.Bucket(fromBucketName)
	it := bkt.Objects(ctx, &storage.Query{Prefix: fromPath})
	// log.Debug("GIFI-Copy()", zap.String("Prefix", fromPath))
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break // No more objects to copy
		}
		if err != nil {
			log.Error("Error listing objects", zap.Error(err))
			return err
		}

		srcObjectKey := objAttrs.Name
		dstObjectKey := strings.Replace(srcObjectKey, fromPath, toPath, 1)

		srcReader, err := bkt.Object(srcObjectKey).NewReader(ctx)
		if err != nil {
			log.Error("Error creating reader for object", zap.String("srcObjectKey", srcObjectKey), zap.Error(err))
			return err
		}

		defer func() {
			if err := srcReader.Close(); err != nil {
				log.Error("Error closing source reader", zap.String("srcObjectKey", srcObjectKey), zap.Error(err))
			}
		}()

		dstWriter := gcm.client.Bucket(toBucketName).Object(dstObjectKey).NewWriter(ctx)

		if _, err := io.Copy(dstWriter, srcReader); err != nil {
			log.Error("Error copying object", zap.String("from", srcObjectKey), zap.String("to", dstObjectKey), zap.Error(err))
			_ = dstWriter.Close()
			return err
		}

		if err := dstWriter.Close(); err != nil {
			log.Error("Error closing destination writer", zap.String("dstObjectKey", dstObjectKey), zap.Error(err))
			return err
		}
		// log.Debug("Copied object", zap.String("from", srcObjectKey), zap.String("to", dstObjectKey))
	}
	return nil
}

// ListObjectsPage paginate list of all objects
func (gcm *GCPNativeChunkManager) ListObjectsPage(ctx context.Context, bucket, prefix string) (ListObjectsPaginator, error) {
	// Implement pagination logic
	return nil, nil
}

// HeadObject determine if an object exists, and you have permission to access it.
func (gcm *GCPNativeChunkManager) HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error) {
	attr, err := gcm.client.Bucket(bucket).Object(key).Attrs(ctx)
	if err != nil {
		return ObjectAttr{}, err
	}
	return ObjectAttr{ //GIFI check here
		Length: attr.Size,
		Key:    attr.Name,
	}, nil
}

// GetObject get an object
func (gcm *GCPNativeChunkManager) GetObject(ctx context.Context, bucketName, key string) (*Object, error) {
	obj := gcm.client.Bucket(bucketName).Object(key)
	// Return the object with its length and the wrapped reader
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return &Object{
		Length: attrs.Size,
		Body:   NewGCSReader(obj), // Wrap the storage.Reader
	}, nil
}

// UploadObject stream upload an object
func (gcm *GCPNativeChunkManager) UploadObject(ctx context.Context, i UploadObjectInput) error {
	// Read the content from the io.Reader
	content, err := io.ReadAll(i.Body)
	if err != nil {
		return fmt.Errorf("failed to read content from body: %w", err)
	}
	return gcm.Write(ctx, i.Bucket, i.Key, content)
}

// GCSReader struct implementing SeekableReadCloser
type GCSReader struct {
	obj      *storage.ObjectHandle
	position int64
	data     []byte
	reader   *storage.Reader
	closed   bool
}

// NewGCSReader creates a new GCSReader
func NewGCSReader(obj *storage.ObjectHandle) *GCSReader {
	return &GCSReader{
		obj:      obj,
		position: 0,
		data:     make([]byte, 0),
		closed:   false,
	}
}

func (g *GCSReader) Read(p []byte) (n int, err error) {
	if g.closed {
		return 0, io.EOF
	}

	// Read until the buffer is filled or EOF
	if g.position < int64(len(g.data)) {
		n = copy(p, g.data[g.position:])
		g.position += int64(n)
		return n, nil
	}

	// Need to fetch more data from GCS
	if g.reader == nil {
		var err error
		g.reader, err = g.obj.NewReader(context.Background())
		if err != nil {
			return 0, err
		}
	}

	n, err = g.reader.Read(p)
	g.data = append(g.data, p[:n]...) // Store fetched data
	g.position += int64(n)

	if err == io.EOF {
		g.closed = true
	}
	return n, err
}

func (g *GCSReader) Close() error {
	if g.closed {
		return nil
	}
	g.closed = true
	if g.reader != nil {
		return g.reader.Close()
	}
	return nil
}

func (g *GCSReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		g.position = offset
	case io.SeekCurrent:
		g.position += offset
	case io.SeekEnd:
		attrs, err := g.obj.Attrs(context.Background())
		if err != nil {
			return 0, err
		}
		g.position = attrs.Size + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if g.position < 0 {
		return 0, fmt.Errorf("negative offset")
	}

	// Reset for new reads
	g.reader.Close() // Close the current reader if open
	g.reader = nil   // Clear the reader

	return g.position, nil
}

func (gr *GCSReader) ReadAt(p []byte, off int64) (n int, err error) {
	reader, err := gr.obj.NewRangeReader(context.Background(), off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.ReadFull(reader, p)
}
