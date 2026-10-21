package restore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestGRPCImportPlannerPlanTasks(t *testing.T) {
	// planning only assembles tasks, so no client is touched; the job halves it
	// mints land on a throwaway tracker.
	jobs := jobstate.NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db", "coll"), 0)
	p := &grpcImportPlanner{args: importTaskArgs{jobs: jobs, logger: zap.NewNop()}}

	dirs := func(n int) []partitionDir {
		ds := make([]partitionDir, 0, n)
		for range n {
			ds = append(ds, partitionDir{insertLogDir: "insert", size: 1})
		}
		return ds
	}

	t.Run("OneTaskPerDir", func(t *testing.T) {
		groups := []dirGroup{
			{timestamp: 7, storageVersion: 2, dirs: dirs(10)},
			{isL0: true, timestamp: 9, storageVersion: 3, dirs: dirs(4)},
		}

		tasks := p.planTasks("p1", groups)
		require.Len(t, tasks, 14)

		for _, tk := range tasks {
			gt, ok := tk.(*importViaGRPCTask)
			require.True(t, ok)
			assert.Equal(t, "p1", gt.partitionName)
			assert.NotEmpty(t, gt.dir.insertLogDir)
		}

		// the group's request parameters ride along on every task
		assert.Equal(t, uint64(7), tasks[0].(*importViaGRPCTask).timestamp)
		assert.Equal(t, int64(2), tasks[0].(*importViaGRPCTask).storageVersion)
		assert.False(t, tasks[0].(*importViaGRPCTask).isL0)
		assert.True(t, tasks[10].(*importViaGRPCTask).isL0)
		assert.Equal(t, uint64(9), tasks[10].(*importViaGRPCTask).timestamp)
	})
}

func TestToGrpcPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	task := &importViaGRPCTask{args: importTaskArgs{milvusStorage: newTestStorageClient(t, cfg.ProviderMinio)}}

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
	task = &importViaGRPCTask{args: importTaskArgs{
		milvusStorage:   newTestStorageClient(t, cfg.ProviderLocal),
		milvusLocalPath: "/var/lib/milvus/data",
	}}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, task.toGrpcPaths(dir))
}

// newLocalGRPCTask builds a grpc import task backed by the local filesystem:
// the backup and the target each get their own temp dir, and the task stages
// its dir into the target before importing. It returns the glob the staged
// copy lands under, since the task mints its temp dir at run time.
func newLocalGRPCTask(t *testing.T, keepTempFiles bool) (*importViaGRPCTask, string, string) {
	backupRoot := t.TempDir()
	milvusRoot := t.TempDir()

	srcDir := filepath.Join(backupRoot, "backup/binlog/insert_log/1/2/3") + "/"
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.WriteFile(srcDir+"file1", []byte("hello"), 0o644))

	backupCli := newTestStorageClient(t, cfg.ProviderLocal)
	milvusCli := newTestStorageClient(t, cfg.ProviderLocal)

	mgr := jobstate.NewStore()
	target := collref.New("db", "coll")
	collTracker := mgr.AddRestoreTask("task1").NewCollTracker(target, 5)

	task := &importViaGRPCTask{
		args: importTaskArgs{
			taskID:          "task1",
			target:          target,
			staging:         true,
			backupDir:       backupRoot + "/",
			backupStorage:   backupCli,
			copySem:         semaphore.NewWeighted(4),
			keepTempFiles:   keepTempFiles,
			milvusStorage:   milvusCli,
			milvusRootPath:  milvusRoot,
			milvusLocalPath: milvusRoot,
			jobs:            collTracker,
			logger:          zap.NewNop(),
		},
		partitionName: "p1",
		dir:           partitionDir{insertLogDir: srcDir, size: 5},
		job:           collTracker.NewJob(5),
	}
	stagedGlob := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-*", "backup/binlog/insert_log/1/2/3/file1")
	return task, backupRoot, stagedGlob
}

// TestImportViaGRPCTaskExecute runs a task end to end against the local
// filesystem: it copies the data into the target storage, imports it, and
// cleans its temp dir up, all inside the task.
func TestImportViaGRPCTaskExecute(t *testing.T) {
	t.Run("CopyImportAndCleanup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, backupRoot, stagedGlob := newLocalGRPCTask(t, false)

			var gotPaths []string
			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).
				Run(func(_ context.Context, in milvus.GrpcBulkInsertInput) { gotPaths = in.Paths }).
				Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			task.args.grpcCli = grpcCli

			require.NoError(t, task.Execute(t.Context()))

			// the import saw the staged copy under the target's local path
			require.Len(t, gotPaths, 2)
			assert.True(t, strings.HasPrefix(gotPaths[0], task.args.milvusLocalPath+"/restore-temp-task1-db-coll-"))
			assert.True(t, strings.HasSuffix(gotPaths[0], "/backup/binlog/insert_log/1/2/3/"))
			assert.Equal(t, "", gotPaths[1])

			// the task removed its temp files once the import was done. LocalClient
			// deletes objects only, so the empty dir hierarchy may remain.
			copied, err := filepath.Glob(stagedGlob)
			require.NoError(t, err)
			assert.Empty(t, copied, "temp files should be removed")

			// the backup itself is untouched
			_, err = os.Stat(filepath.Join(backupRoot, "backup/binlog/insert_log/1/2/3/file1"))
			assert.NoError(t, err)
		})
	})

	t.Run("KeepTempFiles", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, _, stagedGlob := newLocalGRPCTask(t, true)

			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			task.args.grpcCli = grpcCli

			require.NoError(t, task.Execute(t.Context()))

			copied, err := filepath.Glob(stagedGlob)
			require.NoError(t, err)
			assert.Len(t, copied, 1, "temp files should be kept")
		})
	})

	// a failed import still removes the staged copy: the import will never
	// consume it, and the old code leaked it.
	t.Run("ImportFailedStillCleansUp", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, _, stagedGlob := newLocalGRPCTask(t, false)

			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{
					State: commonpb.ImportState_ImportFailed,
					Infos: []*commonpb.KeyValuePair{{Key: "failed_reason", Value: "boom"}},
				}, nil).Once()
			task.args.grpcCli = grpcCli

			err := task.Execute(t.Context())
			assert.ErrorContains(t, err, "boom")

			copied, err := filepath.Glob(stagedGlob)
			require.NoError(t, err)
			assert.Empty(t, copied, "temp files should be removed")
		})
	})
}
