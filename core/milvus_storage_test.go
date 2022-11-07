package core

import (
	"context"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/internal/log"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func newMinioChunkManager(ctx context.Context, bucketName string) (*storage.MinioChunkManager, error) {
	endPoint := getMinioAddress()
	accessKeyID, _ := Params.Load("minio.accessKeyID")
	secretAccessKey, _ := Params.Load("minio.secretAccessKey")
	useSSLStr, _ := Params.Load("minio.useSSL")
	useSSL, _ := strconv.ParseBool(useSSLStr)
	client, err := storage.NewMinioChunkManager(ctx,
		storage.Address(endPoint),
		storage.AccessKeyID(accessKeyID),
		storage.SecretAccessKeyID(secretAccessKey),
		storage.UseSSL(useSSL),
		storage.BucketName(bucketName),
		storage.UseIAM(false),
		storage.IAMEndpoint(""),
		storage.CreateBucket(true),
	)
	return client, err
}

func getMinioAddress() string {
	minioHost := Params.LoadWithDefault("minio.address", paramtable.DefaultMinioAddress)
	if strings.Contains(minioHost, ":") {
		return minioHost
	}
	port := Params.LoadWithDefault("minio.port", paramtable.DefaultMinioPort)
	return minioHost + ":" + port
}

func TestWriteAEmptyBackupFile(t *testing.T) {

	Params.Init()
	testBucket, err := Params.Load("minio.bucketName")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCM, err := newMinioChunkManager(ctx, testBucket)
	err = testCM.Write(ctx, "backup/test_backup6", nil)
	assert.NoError(t, err)
}

func TestReadBackupFiles(t *testing.T) {

	Params.Init()
	testBucket, err := Params.Load("minio.bucketName")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCM, err := newMinioChunkManager(ctx, testBucket)
	files, _, err := testCM.ListWithPrefix(ctx, "/backup", true)
	assert.NoError(t, err)

	for _, file := range files {
		log.Info("BackupFiles", zap.String("path", file))
	}

}

func TestReadMilvusData(t *testing.T) {

	Params.Init()
	testBucket, err := Params.Load("minio.bucketName")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCM, err := newMinioChunkManager(ctx, testBucket)

	//exist0, err := testCM.Exist("backup/my_backup/binlogs/insert_log/437133320401190919/437133320401190920/")
	//log.Info("exist", zap.Bool("exist", exist0))

	exist1, err := testCM.Exist(ctx, "backu")
	log.Info("exist", zap.Bool("exist", exist1))

	exist2, err := testCM.Exist(ctx, "files")
	log.Info("exist", zap.Bool("exist", exist2))

	exist3, err := testCM.Exist(ctx, "files/")
	log.Info("exist", zap.Bool("exist", exist3))

	files, _, err := testCM.ListWithPrefix(ctx, "files", false)
	log.Info("exist", zap.Strings("files", files))
	//
	//paths, _, _ := testCM.ListWithPrefix("", true)
	//log.Info("paths:", zap.Strings("paths", paths))
	//
	//testCM.RemoveWithPrefix("backup")
	//exist, err := testCM.Exist("backup")
	//log.Info("exist", zap.Bool("exist", exist))
	//err = testCM.Write("backup", nil)
	//assert.NoError(t, err)
	//exist2, err := testCM.Exist("backup")
	//log.Info("exist", zap.Bool("exist", exist2))
	//err = testCM.Remove("backup")
	//exist3, err := testCM.Exist("backup")
	//log.Info("exist", zap.Bool("exist", exist3))
	//
	//err = testCM.Remove("backup1")
	//err = testCM.Remove("backup2")
	//err = testCM.Write("backup1", []byte("hello backup"))
	//assert.NoError(t, err)
	//err = testCM.Copy("backup1", "backup2")
	//assert.NoError(t, err)
	//value, err := testCM.Read("backup2")
	//log.Info("value of backup2", zap.String("value", string(value)))
	//assert.NoError(t, err)
	//
	//err = testCM.Remove("backup1")
	//err = testCM.Remove("backup2")
	//
	//fromPath := "files/insert_log/435005460620509185/435005460620509186/435005461053308929/101/435005461721776130"
	//toPath := "backup/0_test_backup_1660893537/data/insert_log/435005460620509185/435005460620509186/435005461053308929/101/435005461721776130"
	//err = testCM.Copy(fromPath, toPath)
	//value, err = testCM.Read(toPath)
	//log.Info("value of toPath", zap.String("value", string(value)))
	//assert.NoError(t, err)
	//
	//testCM.RemoveWithPrefix("backup")
}
