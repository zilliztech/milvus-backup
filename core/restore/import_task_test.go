package restore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func TestGetFailedReason(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "failed_reason", Value: "hello"}})
		assert.Equal(t, "hello", r)
	})

	t.Run("WithoutFailedReason", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, "", r)
	})
}

func TestGetProcess(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "progress_percent", Value: "100"}})
		assert.Equal(t, 100, r)
	})

	t.Run("WithoutProgress", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, 0, r)
	})
}

func TestToPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	task := &importViaRESTFulTask{milvusStorage: newTestStorageClient(t, v2.ProviderMinio)}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, task.toPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert"}, task.toPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, task.toPaths(dir))

	// empty
	dir = partitionDir{}
	assert.Empty(t, task.toPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	task = &importViaRESTFulTask{
		milvusStorage:   newTestStorageClient(t, v2.ProviderLocal),
		milvusLocalPath: "/var/lib/milvus/data",
	}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, task.toPaths(dir))

	// paths already absolute are left alone (a same-directory local backup)
	dir = partitionDir{insertLogDir: "/data/insert"}
	assert.Equal(t, []string{"/data/insert"}, task.toPaths(dir))

	// a trailing slash is kept: the LocalChunkManager globs the prefix
	dir = partitionDir{insertLogDir: "insert/"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert/"}, task.toPaths(dir))
}

func TestToGrpcPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	task := &importViaGRPCTask{milvusStorage: newTestStorageClient(t, v2.ProviderMinio)}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, task.toGrpcPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert", ""}, task.toGrpcPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, task.toGrpcPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	task = &importViaGRPCTask{
		milvusStorage:   newTestStorageClient(t, v2.ProviderLocal),
		milvusLocalPath: "/var/lib/milvus/data",
	}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, task.toGrpcPaths(dir))
}

func TestNewImportTaskFactory(t *testing.T) {
	newFactoryCollTask := func(useV2 bool) *collTask {
		return &collTask{
			taskID:        "task1",
			target:        collref.New("db", "coll"),
			option:        &Option{UseV2Restore: useV2},
			dbBackup:      &backuppb.DatabaseBackupInfo{},
			backupStorage: newTestStorageClient(t, v2.ProviderMinio),
			milvusStorage: newTestStorageClient(t, v2.ProviderMinio),
			logger:        zap.NewNop(),
		}
	}
	b := batch{partitionDirs: []partitionDir{{insertLogDir: "a"}, {insertLogDir: "b"}, {insertLogDir: "c"}}}

	t.Run("V1OneTaskPerDir", func(t *testing.T) {
		tasks := newFactoryCollTask(false).newImportTaskFactory()("p1", b)
		assert.Len(t, tasks, 3)
	})

	t.Run("V2OneTaskPerBatch", func(t *testing.T) {
		tasks := newFactoryCollTask(true).newImportTaskFactory()("p1", b)
		assert.Len(t, tasks, 1)
	})
}

// newLocalGRPCTask builds a grpc import task backed by the local filesystem:
// the backup and the target each get their own temp dir, and the task stages
// its dir into the target before importing.
func newLocalGRPCTask(t *testing.T, keepTempFiles bool) (*importViaGRPCTask, string, string) {
	backupRoot := t.TempDir()
	milvusRoot := t.TempDir()

	srcDir := filepath.Join(backupRoot, "backup/binlog/insert_log/1/2/3") + "/"
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.WriteFile(srcDir+"file1", []byte("hello"), 0o644))

	backupCli := newTestStorageClient(t, v2.ProviderLocal)
	milvusCli := newTestStorageClient(t, v2.ProviderLocal)

	mgr := taskmgr.NewMgr()
	target := collref.New("db", "coll")
	mgr.AddRestoreTask("task1")
	mgr.UpdateRestoreTask("task1", taskmgr.AddRestoreCollTask(target, 5))

	staging := &copyTask{
		src:            backupCli,
		dest:           milvusCli,
		backupDir:      backupRoot + "/",
		tempDir:        "restore-temp-task1-db-coll-abc/",
		sem:            semaphore.NewWeighted(4),
		milvusRootPath: milvusRoot,
		logger:         zap.NewNop(),
	}

	task := &importViaGRPCTask{
		taskID:          "task1",
		target:          target,
		partitionName:   "p1",
		dir:             partitionDir{insertLogDir: srcDir, size: 5},
		staging:         staging,
		keepTempFiles:   keepTempFiles,
		milvusStorage:   milvusCli,
		milvusRootPath:  milvusRoot,
		milvusLocalPath: milvusRoot,
		taskMgr:         mgr,
		logger:          zap.NewNop(),
	}
	return task, backupRoot, milvusRoot
}

// TestImportViaGRPCTaskExecute runs a task end to end against the local
// filesystem: it copies the data into the target storage, imports it, and
// cleans its temp dir up, all inside the task.
func TestImportViaGRPCTaskExecute(t *testing.T) {
	t.Run("CopyImportAndCleanup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, backupRoot, milvusRoot := newLocalGRPCTask(t, false)

			var gotPaths []string
			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).
				Run(func(_ context.Context, in milvus.GrpcBulkInsertInput) { gotPaths = in.Paths }).
				Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			task.grpcCli = grpcCli

			require.NoError(t, task.Execute(t.Context()))

			// the import saw the staged copy under the target's local path
			want := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-abc/backup/binlog/insert_log/1/2/3") + "/"
			assert.Equal(t, []string{want, ""}, gotPaths)

			// the task removed its temp files once the import was done. LocalClient
			// deletes objects only, so the empty dir hierarchy may remain.
			copied := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-abc/backup/binlog/insert_log/1/2/3/file1")
			_, err := os.Stat(copied)
			assert.True(t, os.IsNotExist(err), "temp files should be removed, stat err: %v", err)

			// the backup itself is untouched
			_, err = os.Stat(filepath.Join(backupRoot, "backup/binlog/insert_log/1/2/3/file1"))
			assert.NoError(t, err)
		})
	})

	t.Run("KeepTempFiles", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, _, milvusRoot := newLocalGRPCTask(t, true)

			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			task.grpcCli = grpcCli

			require.NoError(t, task.Execute(t.Context()))

			copied := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-abc/backup/binlog/insert_log/1/2/3/file1")
			_, err := os.Stat(copied)
			assert.NoError(t, err, "temp files should be kept")
		})
	})

	// a failed import still removes the staged copy: the import will never
	// consume it, and the old code leaked it.
	t.Run("ImportFailedStillCleansUp", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, _, milvusRoot := newLocalGRPCTask(t, false)

			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{
					State: commonpb.ImportState_ImportFailed,
					Infos: []*commonpb.KeyValuePair{{Key: "failed_reason", Value: "boom"}},
				}, nil).Once()
			task.grpcCli = grpcCli

			err := task.Execute(t.Context())
			assert.ErrorContains(t, err, "boom")

			copied := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-abc/backup/binlog/insert_log/1/2/3/file1")
			_, statErr := os.Stat(copied)
			assert.True(t, os.IsNotExist(statErr), "temp files should be removed, stat err: %v", statErr)
		})
	})
}
