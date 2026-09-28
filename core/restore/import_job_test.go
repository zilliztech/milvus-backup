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

func newTestImportJobBase(t *testing.T, provider string) importJobBase {
	return importJobBase{milvusStorage: newTestStorageClient(t, provider)}
}

func TestRestfulImportJob_toPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	job := &restfulImportJob{importJobBase: newTestImportJobBase(t, v2.ProviderMinio)}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, job.toPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert"}, job.toPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, job.toPaths(dir))

	// empty
	dir = partitionDir{}
	assert.Empty(t, job.toPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	base := newTestImportJobBase(t, v2.ProviderLocal)
	base.milvusLocalPath = "/var/lib/milvus/data"
	job = &restfulImportJob{importJobBase: base}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, job.toPaths(dir))

	// paths already absolute are left alone (a same-directory local backup)
	dir = partitionDir{insertLogDir: "/data/insert"}
	assert.Equal(t, []string{"/data/insert"}, job.toPaths(dir))

	// a trailing slash is kept: the LocalChunkManager globs the prefix
	dir = partitionDir{insertLogDir: "insert/"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert/"}, job.toPaths(dir))
}

func TestGrpcImportJob_toGrpcPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	job := &grpcImportJob{importJobBase: newTestImportJobBase(t, v2.ProviderMinio)}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, job.toGrpcPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert", ""}, job.toGrpcPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, job.toGrpcPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	base := newTestImportJobBase(t, v2.ProviderLocal)
	base.milvusLocalPath = "/var/lib/milvus/data"
	job = &grpcImportJob{importJobBase: base}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, job.toGrpcPaths(dir))
}

func TestNewImportJobFactory(t *testing.T) {
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

	t.Run("V1OneJobPerDir", func(t *testing.T) {
		jobs := newFactoryCollTask(false).newImportJobFactory()("p1", b)
		assert.Len(t, jobs, 3)
	})

	t.Run("V2OneJobPerBatch", func(t *testing.T) {
		jobs := newFactoryCollTask(true).newImportJobFactory()("p1", b)
		assert.Len(t, jobs, 1)
	})
}

// TestGrpcImportJobExecute runs a job end to end against the local filesystem:
// it copies the data into the target storage, imports it, and cleans its temp
// dir up, all inside the job.
func TestGrpcImportJobExecute(t *testing.T) {
	newLocalJob := func(t *testing.T, keepTempFiles bool) (*grpcImportJob, string, string) {
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

		copyT := &copyTask{
			src:            backupCli,
			dest:           milvusCli,
			backupDir:      backupRoot + "/",
			tempDir:        "restore-temp-task1-db-coll-abc/",
			sem:            semaphore.NewWeighted(4),
			milvusRootPath: milvusRoot,
			logger:         zap.NewNop(),
		}

		job := &grpcImportJob{
			importJobBase: importJobBase{
				taskID:          "task1",
				target:          target,
				partitionName:   "p1",
				dirs:            []partitionDir{{insertLogDir: srcDir, size: 5}},
				copy:            copyT,
				keepTempFiles:   keepTempFiles,
				milvusStorage:   milvusCli,
				milvusLocalPath: milvusRoot,
				taskMgr:         mgr,
				logger:          zap.NewNop(),
			},
		}
		return job, backupRoot, milvusRoot
	}

	t.Run("CopyImportAndCleanup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			job, backupRoot, milvusRoot := newLocalJob(t, false)

			var gotPaths []string
			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).
				Run(func(_ context.Context, in milvus.GrpcBulkInsertInput) { gotPaths = in.Paths }).
				Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			job.grpcCli = grpcCli

			require.NoError(t, job.Execute(t.Context()))

			// the import saw the staged copy under the target's local path
			want := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-abc/backup/binlog/insert_log/1/2/3") + "/"
			assert.Equal(t, []string{want, ""}, gotPaths)

			// the job removed its temp files once the import was done. LocalClient
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
			job, _, milvusRoot := newLocalJob(t, true)

			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			job.grpcCli = grpcCli

			require.NoError(t, job.Execute(t.Context()))

			copied := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-abc/backup/binlog/insert_log/1/2/3/file1")
			_, err := os.Stat(copied)
			assert.NoError(t, err, "temp files should be kept")
		})
	})
}
