package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestDestKey(t *testing.T) {
	// a non-local target keeps the key as-is, whatever the root path says
	minio := newTestStorageClient(t, v2.ProviderMinio)
	assert.Equal(t, "restore-temp-1/", destKey(minio, "/data", "restore-temp-1/"))

	// a local target resolves against the directory milvus-backup writes to,
	// keeping the key's trailing slash for the prefix replacement on copy
	local := newTestStorageClient(t, v2.ProviderLocal)
	assert.Equal(t, "/data/restore-temp-1/", destKey(local, "/data", "restore-temp-1/"))
	assert.Equal(t, "/data/insert", destKey(local, "/data", "insert"))

	assert.Equal(t, "", destKey(local, "/data", ""))
}

// newCopyTestCollTask builds a collDMLTask whose backup and milvus storage
// differ only in bucket, so each case picks the copy-or-not wiring it wants.
func newCopyTestCollTask(t *testing.T, backupBucket, milvusBucket string, streaming bool) *collDMLTask {
	newClient := func(bucket string) storage.Client {
		cfg := storage.Config{
			Provider:   v2.ProviderMinio,
			Endpoint:   "localhost:9000",
			Bucket:     bucket,
			Credential: storage.Credential{Type: storage.Static, AK: "a", SK: "b"},
		}
		cli, err := storage.NewClient(t.Context(), cfg)
		assert.NoError(t, err)
		return cli
	}

	return &collDMLTask{
		taskID:        "task1",
		target:        collref.New("db", "coll"),
		backupStorage: newClient(backupBucket),
		milvusStorage: newClient(milvusBucket),
		streaming:     streaming,
		copySem:       semaphore.NewWeighted(1),
		logger:        zap.NewNop(),
	}
}

func TestNewCopyTask(t *testing.T) {
	t.Run("SameBucketAndNotStreaming", func(t *testing.T) {
		ct := newCopyTestCollTask(t, "bucket", "bucket", false)
		assert.Nil(t, ct.newCopyTask())
	})

	t.Run("DifferentBucket", func(t *testing.T) {
		ct := newCopyTestCollTask(t, "bucket", "another", false)
		copyT := ct.newCopyTask()
		require.NotNil(t, copyT)
		assert.Contains(t, copyT.tempDir, "restore-temp-task1-db-coll-")
	})

	t.Run("StreamingForcesCopy", func(t *testing.T) {
		ct := newCopyTestCollTask(t, "bucket", "bucket", true)
		assert.NotNil(t, ct.newCopyTask())
	})

	t.Run("TempDirIsUniquePerTask", func(t *testing.T) {
		ct := newCopyTestCollTask(t, "bucket", "another", false)
		first, second := ct.newCopyTask(), ct.newCopyTask()
		assert.NotEqual(t, first.tempDir, second.tempDir)
	})
}
