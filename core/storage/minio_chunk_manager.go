package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/storage/gcp"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/errorutil"
	"github.com/zilliztech/milvus-backup/internal/util/retry"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"
)

var (
	ErrNoSuchKey = errors.New("NoSuchKey")
)

func WrapErrNoSuchKey(key string) error {
	return fmt.Errorf("%w(key=%s)", ErrNoSuchKey, key)
}

var CheckBucketRetryAttempts uint = 20

// MinioChunkManager is responsible for read and write data stored in minio.
type MinioChunkManager struct {
	*minio.Client

	//	ctx        context.Context
	bucketName string
	rootPath   string
}

var _ ChunkManager = (*MinioChunkManager)(nil)

// NewMinioChunkManager create a new local manager object.
// Do not call this directly! Use factory.NewPersistentStorageChunkManager instead.
func NewMinioChunkManager(ctx context.Context, opts ...Option) (*MinioChunkManager, error) {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}

	return newMinioChunkManagerWithConfig(ctx, c)
}

func newMinioChunkManagerWithConfig(ctx context.Context, c *config) (*MinioChunkManager, error) {
	var creds *credentials.Credentials
	var newMinioFn = minio.New

	switch c.cloudProvider {
	case paramtable.CloudProviderGCP:
		newMinioFn = gcp.NewMinioClient
		if !c.useIAM {
			creds = credentials.NewStaticV2(c.accessKeyID, c.secretAccessKeyID, "")
		}
	default: // aws, minio
		if c.useIAM {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
		}
	}
	minioOpts := &minio.Options{
		Creds:  creds,
		Secure: c.useSSL,
	}
	minIOClient, err := newMinioFn(c.address, minioOpts)
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, c.bucketName)
		if err != nil {
			log.Warn("failed to check blob bucket exist", zap.String("bucket", c.bucketName), zap.Error(err))
			return err
		}
		if !bucketExists {
			if c.createBucket {
				log.Info("blob bucket not exist, create bucket.", zap.Any("bucket name", c.bucketName))
				err := minIOClient.MakeBucket(ctx, c.bucketName, minio.MakeBucketOptions{})
				if err != nil {
					log.Warn("failed to create blob bucket", zap.String("bucket", c.bucketName), zap.Error(err))
					return err
				}
			} else {
				return fmt.Errorf("bucket %s not Existed", c.bucketName)
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
		bucketName: c.bucketName,
	}
	mcm.rootPath = mcm.normalizeRootPath(c.rootPath)
	log.Info("minio chunk manager init success.", zap.String("bucketname", c.bucketName), zap.String("root", mcm.RootPath()))
	return mcm, nil
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
		return "", errors.New("minio file manage cannot be found with filePath:" + filePath)
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
	_, err := mcm.Client.StatObject(ctx, bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		log.Warn("failed to stat object", zap.String("path", filePath), zap.Error(err))
		return false, err
	}
	return true, nil
}

// Read reads the minio storage data if exists.
func (mcm *MinioChunkManager) Read(ctx context.Context, bucketName string, filePath string) ([]byte, error) {
	object, err := mcm.Client.GetObject(ctx, bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	objectInfo, err := object.Stat()
	if err != nil {
		log.Warn("failed to stat object", zap.String("path", filePath), zap.Error(err))
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		return nil, err
	}

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

func (mcm *MinioChunkManager) Mmap(ctx context.Context, bucketName string, filePath string) (*mmap.ReaderAt, error) {
	return nil, errors.New("this method has not been implemented")
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
	objects := mcm.Client.ListObjects(ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for rErr := range mcm.Client.RemoveObjects(ctx, bucketName, objects, minio.RemoveObjectsOptions{GovernanceBypass: false}) {
		if rErr.Err != nil {
			log.Warn("failed to remove objects", zap.String("prefix", prefix), zap.Error(rErr.Err))
			return rErr.Err
		}
	}
	return nil
}

func (mcm *MinioChunkManager) ListWithPrefix(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []time.Time, error) {
	objects := mcm.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive})
	var objectsKeys []string
	var modTimes []time.Time

	for object := range objects {
		if object.Err != nil {
			log.Warn("failed to list with prefix", zap.String("prefix", prefix), zap.Error(object.Err))
			return nil, nil, object.Err
		}
		objectsKeys = append(objectsKeys, object.Key)
		modTimes = append(modTimes, object.LastModified)
	}
	return objectsKeys, modTimes, nil
}

func (mcm *MinioChunkManager) Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error {
	objectkeys, _, err := mcm.ListWithPrefix(ctx, fromBucketName, fromPath, true)
	if err != nil {
		log.Warn("listWithPrefix error", zap.String("prefix", fromPath), zap.Error(err))
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

// Learn from file.ReadFile
func Read(r io.Reader, size int64) ([]byte, error) {
	data := make([]byte, 0, size)
	for {
		if len(data) >= cap(data) {
			d := append(data[:cap(data)], 0)
			data = d[:len(data)]
		}
		n, err := r.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, err
		}
	}
}
