package core

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/internal/log"
	"go.uber.org/zap"
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
	err = testCM.Write(ctx, testBucket, "backup/test_backup6", nil)
	assert.NoError(t, err)
}

func TestReadBackupFiles(t *testing.T) {

	Params.Init()
	testBucket, err := Params.Load("minio.bucketName")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCM, err := newMinioChunkManager(ctx, testBucket)
	files, _, err := testCM.ListWithPrefix(ctx, testBucket, "/backup", true)
	assert.NoError(t, err)

	for _, file := range files {
		log.Info("BackupFiles", zap.String("path", file))
	}

}

func TestReadMilvusData(t *testing.T) {
	var params paramtable.BackupParams
	params.GlobalInitWithYaml("backup.yaml")
	params.Init()

	context := context.Background()
	//backupContext := CreateBackupContext(context, params)

	client, err := CreateStorageClient(context, params)
	assert.NoError(t, err)
	paths, _, err := client.ListWithPrefix(context, params.MinioCfg.BucketName, params.MinioCfg.RootPath, true)
	assert.NoError(t, err)
	log.Info("paths", zap.Strings("paths", paths))

}
