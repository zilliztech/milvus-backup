package restore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestDestKey(t *testing.T) {
	// a non-local target keeps the key as-is, whatever the root path says
	minio := newTestStorageClient(t, cfg.ProviderMinio)
	assert.Equal(t, "restore-temp-1/", destKey(minio, "/data", "restore-temp-1/"))

	// a local target resolves against the directory milvus-backup writes to,
	// keeping the key's trailing slash for the prefix replacement on copy
	local := newTestStorageClient(t, cfg.ProviderLocal)
	assert.Equal(t, "/data/restore-temp-1/", destKey(local, "/data", "restore-temp-1/"))
	assert.Equal(t, "/data/insert", destKey(local, "/data", "insert"))

	assert.Equal(t, "", destKey(local, "/data", ""))
}

// newCopyTestArgs builds import task args whose backup and milvus storage
// differ only in bucket, so each case picks the staging wiring it wants.
func newCopyTestArgs(t *testing.T, staging bool) importTaskArgs {
	newClient := func(bucket string) storage.Client {
		storeCfg := storage.Config{
			Provider:   cfg.ProviderMinio,
			Endpoint:   "localhost:9000",
			Bucket:     bucket,
			Credential: storage.Credential{Type: storage.Static, AK: "a", SK: "b"},
		}
		cli, err := storage.NewClient(t.Context(), storeCfg)
		assert.NoError(t, err)
		return cli
	}

	return importTaskArgs{
		taskID:        "task1",
		target:        collref.New("db", "coll"),
		staging:       staging,
		backupStorage: newClient("backup-bucket"),
		copySem:       semaphore.NewWeighted(1),
		milvusStorage: newClient("milvus-bucket"),
		logger:        zap.NewNop(),
	}
}

func TestNewCopyTask(t *testing.T) {
	t.Run("NoStaging", func(t *testing.T) {
		assert.Nil(t, newCopyTask(newCopyTestArgs(t, false)))
	})

	t.Run("Staging", func(t *testing.T) {
		copyT := newCopyTask(newCopyTestArgs(t, true))
		require.NotNil(t, copyT)
		assert.Contains(t, copyT.tempDir, "restore-temp-task1-db-coll-")
	})

	t.Run("TempDirIsUniquePerTask", func(t *testing.T) {
		args := newCopyTestArgs(t, true)
		first, second := newCopyTask(args), newCopyTask(args)
		assert.NotEqual(t, first.tempDir, second.tempDir)
	})
}

// A nil copy task is the no-op the plan's no-staging decision produces: the
// dirs pass through untouched and there is nothing to clean up.
func TestCopyTaskNilIsNoOp(t *testing.T) {
	var staging *copyTask

	dirs := []partitionDir{{insertLogDir: "insert", size: 1}}
	got, err := staging.Execute(context.Background(), dirs)
	require.NoError(t, err)
	assert.Equal(t, dirs, got)

	staging.Cleanup(context.Background())
}
