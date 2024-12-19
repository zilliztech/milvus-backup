package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/storage/aliyun"
	"github.com/zilliztech/milvus-backup/core/storage/gcp"
	"github.com/zilliztech/milvus-backup/core/storage/tencent"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/errorutil"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

const NoSuchKey = "NoSuchKey"

var (
	ErrNoSuchKey = errors.New("NoSuchKey")
)

func WrapErrNoSuchKey(key string) error {
	return fmt.Errorf("%w(key=%s)", ErrNoSuchKey, key)
}

func IsErrNoSuchKey(err error) bool {
	return strings.HasPrefix(err.Error(), NoSuchKey)
}

var CheckBucketRetryAttempts uint = 20

// MinioChunkManager is responsible for read and write data stored in minio.
type MinioChunkManager struct {
	*minio.Client

	provider   string
	bucketName string
	rootPath   string

	config *StorageConfig
}

var _ ChunkManager = (*MinioChunkManager)(nil)

func NewMinioChunkManagerWithConfig(ctx context.Context, config *StorageConfig) (*MinioChunkManager, error) {
	var creds *credentials.Credentials
	var newMinioFn = minio.New
	var bucketLookupType = minio.BucketLookupAuto
	log.Info("GIFI-", zap.String("Address", config.Address))
	switch config.StorageType {
	case paramtable.CloudProviderAliyun:
		// auto doesn't work for aliyun, so we set to dns deliberately
		bucketLookupType = minio.BucketLookupDNS
		if config.UseIAM {
			newMinioFn = aliyun.NewMinioClient
		} else {
			creds = credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKeyID, "")
		}
	case paramtable.CloudProviderAli:
		// auto doesn't work for aliyun, so we set to dns deliberately
		bucketLookupType = minio.BucketLookupDNS
		if config.UseIAM {
			newMinioFn = aliyun.NewMinioClient
		} else {
			creds = credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKeyID, "")
		}
	case paramtable.CloudProviderGCP:
		newMinioFn = gcp.NewMinioClient
		if !config.UseIAM {
			creds = credentials.NewStaticV2(config.AccessKeyID, config.SecretAccessKeyID, "")
		}
	case paramtable.CloudProviderTencentShort:
		bucketLookupType = minio.BucketLookupDNS
		newMinioFn = tencent.NewMinioClient
		if !config.UseIAM {
			creds = credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKeyID, "")
		}
	case paramtable.CloudProviderTencent:
		bucketLookupType = minio.BucketLookupDNS
		newMinioFn = tencent.NewMinioClient
		if !config.UseIAM {
			creds = credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKeyID, "")
		}
	default: // aws, minio
		if config.UseIAM {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKeyID, "")
		}
	}
	minioOpts := &minio.Options{
		BucketLookup: bucketLookupType,
		Creds:        creds,
		Secure:       config.UseSSL,
	}
	minIOClient, err := newMinioFn(config.Address, minioOpts)
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, config.BucketName)
		if err != nil {
			log.Warn("failed to check blob bucket exist", zap.String("bucket", config.BucketName), zap.String("Port", config.Address), zap.Error(err))
			return err
		}
		if !bucketExists {
			if config.CreateBucket {
				log.Info("blob bucket not exist, create bucket.", zap.Any("bucket name", config.BucketName))
				err := minIOClient.MakeBucket(ctx, config.BucketName, minio.MakeBucketOptions{})
				if err != nil {
					log.Warn("failed to create blob bucket", zap.String("bucket", config.BucketName), zap.Error(err))
					return err
				}
			} else {
				return fmt.Errorf("bucket %s not Existed", config.BucketName)
			}
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}

	mcm := &MinioChunkManager{
		Client:     minIOClient,
		bucketName: config.BucketName,
		provider:   config.StorageType,
		config:     config,
	}
	mcm.rootPath = mcm.normalizeRootPath(config.RootPath)
	log.Info("minio chunk manager init success.", zap.String("bucketname", config.BucketName), zap.String("root", mcm.RootPath()))
	return mcm, nil
}

func (mcm *MinioChunkManager) Config() *StorageConfig {
	return mcm.config
}

// normalizeRootPath
func (mcm *MinioChunkManager) normalizeRootPath(rootPath string) string {
	// no leading "/"
	return strings.TrimLeft(rootPath, "/")
}

// SetVar set the variable value of mcm
func (mcm *MinioChunkManager) SetVar(bucketName string, rootPath string) {
	mcm.bucketName = bucketName
	mcm.rootPath = rootPath
}

// RootPath returns minio root path.
func (mcm *MinioChunkManager) RootPath() string {
	return mcm.rootPath
}

// Path returns the path of minio data if exists.
func (mcm *MinioChunkManager) Path(ctx context.Context, bucketName string, filePath string) (string, error) {
	exist, err := mcm.Exist(ctx, bucketName, filePath)
	if err != nil {
		return "", err
	}
	if !exist {
		return "", errors.New("minio file cannot be found with filePath:" + filePath)
	}
	return filePath, nil
}

// Reader returns the path of minio data if exists.
func (mcm *MinioChunkManager) Reader(ctx context.Context, bucketName string, filePath string) (FileReader, error) {
	reader, err := mcm.Client.GetObject(ctx, bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return reader, nil
}

func (mcm *MinioChunkManager) Size(ctx context.Context, bucketName string, filePath string) (int64, error) {
	objectInfo, err := mcm.Client.StatObject(ctx, bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		log.Warn("failed to stat object", zap.String("path", filePath), zap.Error(err))
		return 0, err
	}

	return objectInfo.Size, nil
}

// Write writes the data to minio storage.
func (mcm *MinioChunkManager) Write(ctx context.Context, bucketName string, filePath string, content []byte) error {
	_, err := mcm.Client.PutObject(ctx, bucketName, filePath, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})

	if err != nil {
		log.Warn("failed to put object", zap.String("path", filePath), zap.Error(err))
		return err
	}

	return nil
}

// MultiWrite saves multiple objects, the path is the key of @kvs.
// The object value is the value of @kvs.
func (mcm *MinioChunkManager) MultiWrite(ctx context.Context, bucketName string, kvs map[string][]byte) error {
	var el errorutil.ErrorList
	for key, value := range kvs {
		err := mcm.Write(ctx, bucketName, key, value)
		if err != nil {
			el = append(el, err)
		}
	}
	if len(el) == 0 {
		return nil
	}
	return el
}

// Exist checks whether chunk is saved to minio storage.
func (mcm *MinioChunkManager) Exist(ctx context.Context, bucketName string, filePath string) (bool, error) {
	//_, err := mcm.Client.StatObject(ctx, BucketName, filePath, minio.StatObjectOptions{})
	paths, _, err := mcm.ListWithPrefix(ctx, bucketName, filePath, false)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("MinioChunkManager Exist errResponse",
			zap.String("bucket", bucketName),
			zap.String("filePath", filePath),
			zap.Any("errResponse", errResponse),
		)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		log.Warn("failed to stat object", zap.String("path", filePath), zap.Error(err))
		return false, err
	}
	if len(paths) > 0 {
		return true, nil
	}
	return false, nil
}

// Read reads the minio storage data if exists.
func (mcm *MinioChunkManager) Read(ctx context.Context, bucketName string, filePath string) ([]byte, error) {
	//object, err := mcm.Client.GetObject(ctx, BucketName, filePath, minio.GetObjectOptions{})
	//if err != nil {
	//	log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
	//	return nil, err
	//}
	//defer object.Close()
	//
	//objectInfo, err := object.Stat()
	//if err != nil {
	//	log.Warn("failed to stat object", zap.String("path", filePath), zap.Error(err))
	//	errResponse := minio.ToErrorResponse(err)
	//	if errResponse.Code == "NoSuchKey" {
	//		return nil, WrapErrNoSuchKey(filePath)
	//	}
	//	return nil, err
	//}

	objectInfo, err := mcm.Client.StatObject(ctx, bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		log.Warn("failed to stat object", zap.String("path", filePath), zap.Error(err))
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		return nil, err
	}
	object, err := mcm.Client.GetObject(ctx, bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()
	data, err := Read(object, objectInfo.Size)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, nil
}

func (mcm *MinioChunkManager) MultiRead(ctx context.Context, bucketName string, keys []string) ([][]byte, error) {
	var el errorutil.ErrorList
	var objectsValues [][]byte
	for _, key := range keys {
		objectValue, err := mcm.Read(ctx, bucketName, key)
		if err != nil {
			el = append(el, err)
		}
		objectsValues = append(objectsValues, objectValue)
	}

	if len(el) == 0 {
		return objectsValues, nil
	}
	return objectsValues, el
}

func (mcm *MinioChunkManager) ReadWithPrefix(ctx context.Context, bucketName string, prefix string) ([]string, [][]byte, error) {
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

// ReadAt reads specific position data of minio storage if exists.
func (mcm *MinioChunkManager) ReadAt(ctx context.Context, bucketName string, filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}

	opts := minio.GetObjectOptions{}
	err := opts.SetRange(off, off+length-1)
	if err != nil {
		log.Warn("failed to set range", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}

	object, err := mcm.Client.GetObject(ctx, bucketName, filePath, opts)
	if err != nil {
		log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	data, err := Read(object, length)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, nil
}

// Remove deletes an object with @key.
func (mcm *MinioChunkManager) Remove(ctx context.Context, bucketName string, filePath string) error {
	err := mcm.Client.RemoveObject(ctx, bucketName, filePath, minio.RemoveObjectOptions{})
	if err != nil {
		log.Warn("failed to remove object", zap.String("path", filePath), zap.Error(err))
		return err
	}
	return nil
}

// MultiRemove deletes a objects with @keys.
func (mcm *MinioChunkManager) MultiRemove(ctx context.Context, bucketName string, keys []string) error {
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
func (mcm *MinioChunkManager) RemoveWithPrefix(ctx context.Context, bucketName string, prefix string) error {
	objects := mcm.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	i := 0
	maxGoroutine := 10
	removeKeys := make([]string, 0, len(objects))
	for object := range objects {
		if object.Err != nil {
			return object.Err
		}
		removeKeys = append(removeKeys, object.Key)
	}
	for i < len(removeKeys) {
		runningGroup, groupCtx := errgroup.WithContext(ctx)
		for j := 0; j < maxGoroutine && i < len(removeKeys); j++ {
			key := removeKeys[i]
			runningGroup.Go(func() error {
				err := mcm.Client.RemoveObject(groupCtx, bucketName, key, minio.RemoveObjectOptions{})
				if err != nil {
					log.Warn("failed to remove object", zap.String("path", key), zap.Error(err))
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

func (mcm *MinioChunkManager) ListWithPrefix(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []int64, error) {
	objects := mcm.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive})
	var objectsKeys []string
	var sizes []int64

	for object := range objects {
		if object.Err != nil {
			log.Warn("failed to list with prefix", zap.String("bucket", bucketName), zap.String("prefix", prefix), zap.Error(object.Err))
			return nil, nil, object.Err
		}
		objectsKeys = append(objectsKeys, object.Key)
		sizes = append(sizes, object.Size)
	}
	return objectsKeys, sizes, nil
}

func (mcm *MinioChunkManager) Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error {
	objectkeys, _, err := mcm.ListWithPrefix(ctx, fromBucketName, fromPath, true)
	if err != nil {
		log.Warn("listWithPrefix error", zap.String("bucket", fromBucketName), zap.String("prefix", fromPath), zap.Error(err))
		return err
	}
	for _, objectkey := range objectkeys {
		src := minio.CopySrcOptions{Bucket: fromBucketName, Object: objectkey}
		dstObjectKey := strings.Replace(objectkey, fromPath, toPath, 1)
		dst := minio.CopyDestOptions{Bucket: toBucketName, Object: dstObjectKey}

		_, err = mcm.Client.CopyObject(ctx, dst, src)
		if err != nil {
			log.Error("copyObject error",
				zap.String("srcObjectKey", objectkey),
				zap.String("dstObjectKey", dstObjectKey),
				zap.Error(err))
			return err
		}
	}
	return nil
}

const _defaultPageSize = 1000

type MinioListObjectPaginator struct {
	cli *minio.Client

	objCh    <-chan minio.ObjectInfo
	pageSize int32
	hasMore  bool
}

func (p *MinioListObjectPaginator) HasMorePages() bool { return p.hasMore }

func (p *MinioListObjectPaginator) NextPage(_ context.Context) (*Page, error) {
	if !p.hasMore {
		return nil, errors.New("storage: gcp no more pages")
	}

	contents := make([]ObjectAttr, 0, p.pageSize)
	for obj := range p.objCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("storage list objs %w", obj.Err)
		}
		contents = append(contents, ObjectAttr{Key: obj.Key, Length: obj.Size, ETag: obj.ETag})
		if len(contents) == int(p.pageSize) {
			return &Page{Contents: contents}, nil
		}
	}
	p.hasMore = false

	return &Page{Contents: contents}, nil
}

func (mcm *MinioChunkManager) HeadObject(ctx context.Context, bucket, key string) (ObjectAttr, error) {
	attr, err := mcm.Client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: %s head object %w", mcm.provider, err)
	}

	return ObjectAttr{Key: attr.Key, Length: attr.Size, ETag: attr.ETag}, nil
}

func (mcm *MinioChunkManager) ListObjectsPage(ctx context.Context, bucket, prefix string) (ListObjectsPaginator, error) {
	objCh := mcm.Client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	return &MinioListObjectPaginator{cli: mcm.Client, objCh: objCh, pageSize: _defaultPageSize, hasMore: true}, nil
}

func (mcm *MinioChunkManager) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	obj, err := mcm.Client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object %w", mcm.provider, err)
	}
	attr, err := obj.Stat()
	if err != nil {
		return nil, fmt.Errorf("storage: %s get object attr %w", mcm.provider, err)
	}
	return &Object{Length: attr.Size, Body: obj}, nil
}

func (mcm *MinioChunkManager) UploadObject(ctx context.Context, i UploadObjectInput) error {
	opt := minio.PutObjectOptions{}
	size := int64(-1)
	if i.Size > 0 {
		size = i.Size
	}
	if _, err := mcm.Client.PutObject(ctx, i.Bucket, i.Key, i.Body, size, opt); err != nil {
		return fmt.Errorf("storage: %s upload object %s %w", mcm.provider, i.Key, err)
	}

	return nil
}
