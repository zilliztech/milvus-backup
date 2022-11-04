package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/errorutil"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

type FileReader interface {
	io.Reader
	io.Closer
}

// MilvusStorage is interface to operate milvus data
type MilvusStorage interface {
	// Path returns path of @filePath.
	Path(filePath string) (string, error)
	// Size returns path of @filePath.
	Size(filePath string) (int64, error)
	// Write writes @content to @filePath.
	Write(filePath string, content []byte) error
	// MultiWrite writes multi @content to @filePath.
	MultiWrite(contents map[string][]byte) error
	// Exist returns true if @filePath exists.
	Exist(filePath string) (bool, error)
	// Read reads @filePath and returns content.
	Read(filePath string) ([]byte, error)
	// Reader return a reader for @filePath
	Reader(filePath string) (FileReader, error)
	// MultiRead reads @filePath and returns content.
	MultiRead(filePaths []string) ([][]byte, error)
	ListWithPrefix(prefix string, recursive bool) ([]string, []time.Time, error)
	// ReadWithPrefix reads files with same @prefix and returns contents.
	ReadWithPrefix(prefix string) ([]string, [][]byte, error)
	// ReadAt reads @filePath by offset @off, content stored in @p, return @n as the number of bytes read.
	// if all bytes are read, @err is io.EOF.
	// return other error if read failed.
	ReadAt(filePath string, off int64, length int64) (p []byte, err error)
	// Remove delete @filePath.
	Remove(filePath string) error
	// MultiRemove delete @filePaths.
	MultiRemove(filePaths []string) error
	// RemoveWithPrefix remove files with same @prefix.
	RemoveWithPrefix(prefix string) error
	// Move move files from fromPath into toPath recursively
	Copy(fromPath string, toPath string) error
}

// makes sure MinioMilvusStorage implements `MilvusStorage`
var _ MilvusStorage = (*MinioMilvusStorage)(nil)

type MinioMilvusStorage struct {
	*minio.Client

	ctx        context.Context
	bucketName string
}

// NewMinioMilvusStorage create a new local manager object.
func NewMinioMilvusStorage(ctx context.Context, opts ...Option) (*MinioMilvusStorage, error) {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}

	return newMinioMilvusStorageWithConfig(ctx, c)
}

func newMinioMilvusStorageWithConfig(ctx context.Context, c *config) (*MinioMilvusStorage, error) {
	var creds *credentials.Credentials
	if c.useIAM {
		creds = credentials.NewIAM(c.iamEndpoint)
	} else {
		creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
	}
	minIOClient, err := minio.New(c.address, &minio.Options{
		Creds:  creds,
		Secure: c.useSSL,
	})
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
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(20))
	if err != nil {
		return nil, err
	}

	mcm := &MinioMilvusStorage{
		ctx:        ctx,
		Client:     minIOClient,
		bucketName: c.bucketName,
	}
	log.Info("Milvus minio storage manager init success.", zap.String("bucketname", c.bucketName), zap.String("root", c.rootPath))
	return mcm, nil
}

// Path returns the path of minio data if exists.
func (mcm *MinioMilvusStorage) Path(filePath string) (string, error) {
	exist, err := mcm.Exist(filePath)
	if err != nil {
		return "", err
	}
	if !exist {
		return "", errors.New("minio file manage cannot be found with filePath:" + filePath)
	}
	return filePath, nil
}

// Reader returns the path of minio data if exists.
func (mcm *MinioMilvusStorage) Reader(filePath string) (FileReader, error) {
	reader, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return reader, nil
}

func (mcm *MinioMilvusStorage) Size(filePath string) (int64, error) {
	objectInfo, err := mcm.Client.StatObject(mcm.ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		log.Warn("failed to stat object", zap.String("path", filePath), zap.Error(err))
		return 0, err
	}

	return objectInfo.Size, nil
}

// Write writes the data to minio storage.
func (mcm *MinioMilvusStorage) Write(filePath string, content []byte) error {
	_, err := mcm.Client.PutObject(mcm.ctx, mcm.bucketName, filePath, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})

	if err != nil {
		log.Warn("failed to put object", zap.String("path", filePath), zap.Error(err))
		return err
	}

	return nil
}

// MultiWrite saves multiple objects, the path is the key of @kvs.
// The object value is the value of @kvs.
func (mcm *MinioMilvusStorage) MultiWrite(kvs map[string][]byte) error {
	var el errorutil.ErrorList
	for key, value := range kvs {
		err := mcm.Write(key, value)
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
func (mcm *MinioMilvusStorage) Exist(filePath string) (bool, error) {
	_, err := mcm.Client.StatObject(mcm.ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
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

//func (mcm *MinioMilvusStorage) Exist(filePath string) (bool, error) {
//	objects := mcm.Client.ListObjects(mcm.ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: filePath, Recursive: false})
//	if strings.HasSuffix(filePath, "/") && len(objects) > 0 {
//		return true, nil
//	}
//	for object := range objects {
//		if object.Err != nil {
//			log.Warn("failed to list with prefix", zap.String("prefix", filePath), zap.Error(object.Err))
//			return false, object.Err
//		}
//		if strings.HasSuffix(filePath, "/") {
//
//		}
//		if object.Key == filePath {
//			return true, nil
//		}
//
//		if strings.Contains(object.Key, filePath) && strings.Split(object.Key, filePath)[1]
//	}
//	log.Warn("path not exist", zap.String("path", filePath))
//	return false, nil
//}

// Read reads the minio storage data if exists.
func (mcm *MinioMilvusStorage) Read(filePath string) ([]byte, error) {
	object, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	data, err := ioutil.ReadAll(object)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, errors.New("NoSuchKey")
		}
		log.Warn("failed to read object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, nil
}

func (mcm *MinioMilvusStorage) MultiRead(keys []string) ([][]byte, error) {
	var el errorutil.ErrorList
	var objectsValues [][]byte
	for _, key := range keys {
		objectValue, err := mcm.Read(key)
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

func (mcm *MinioMilvusStorage) ReadWithPrefix(prefix string) ([]string, [][]byte, error) {
	objectsKeys, _, err := mcm.ListWithPrefix(prefix, true)
	if err != nil {
		return nil, nil, err
	}
	objectsValues, err := mcm.MultiRead(objectsKeys)
	if err != nil {
		return nil, nil, err
	}

	return objectsKeys, objectsValues, nil
}

// ReadAt reads specific position data of minio storage if exists.
func (mcm *MinioMilvusStorage) ReadAt(filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}

	opts := minio.GetObjectOptions{}
	err := opts.SetRange(off, off+length-1)
	if err != nil {
		log.Warn("failed to set range", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}

	object, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, opts)
	if err != nil {
		log.Warn("failed to get object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()
	data, err := ioutil.ReadAll(object)
	if err != nil {
		log.Warn("failed to read object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, nil
}

// Remove deletes an object with @key.
func (mcm *MinioMilvusStorage) Remove(filePath string) error {
	err := mcm.Client.RemoveObject(mcm.ctx, mcm.bucketName, filePath, minio.RemoveObjectOptions{})
	if err != nil {
		log.Warn("failed to remove object", zap.String("path", filePath), zap.Error(err))
		return err
	}
	return nil
}

// MultiRemove deletes an objects with @keys.
func (mcm *MinioMilvusStorage) MultiRemove(keys []string) error {
	var el errorutil.ErrorList
	for _, key := range keys {
		err := mcm.Remove(key)
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
func (mcm *MinioMilvusStorage) RemoveWithPrefix(prefix string) error {
	objects := mcm.Client.ListObjects(mcm.ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for rErr := range mcm.Client.RemoveObjects(mcm.ctx, mcm.bucketName, objects, minio.RemoveObjectsOptions{GovernanceBypass: false}) {
		if rErr.Err != nil {
			log.Warn("failed to remove objects", zap.String("prefix", prefix), zap.Error(rErr.Err))
			return rErr.Err
		}
	}
	return nil
}

func (mcm *MinioMilvusStorage) ListWithPrefix(prefix string, recursive bool) ([]string, []time.Time, error) {
	objects := mcm.Client.ListObjects(mcm.ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive})
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

func (mcm *MinioMilvusStorage) Copy(fromPath string, toPath string) error {
	objectkeys, _, err := mcm.ListWithPrefix(fromPath, true)
	if err != nil {
		log.Warn("listWithPrefix error", zap.String("prefix", fromPath), zap.Error(err))
		return err
	}
	for _, objectkey := range objectkeys {
		src := minio.CopySrcOptions{Bucket: mcm.bucketName, Object: objectkey}
		dstObjectKey := strings.Replace(objectkey, fromPath, toPath, 1)
		dst := minio.CopyDestOptions{Bucket: mcm.bucketName, Object: dstObjectKey}

		_, err = mcm.Client.CopyObject(mcm.ctx, dst, src)
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
