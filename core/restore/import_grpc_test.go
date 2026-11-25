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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestGRPCImportPlannerPlanTasks(t *testing.T) {
	// planning only assembles tasks, so no client is touched; the job halves it
	// mints land on a throwaway tracker.
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("task-1", false)
	require.NoError(t, addErr)
	jobs := restoreTracker.NewCollTracker(collref.New("db", "coll"), 0)
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
func newLocalGRPCTask(t *testing.T, enableResume, keepTempFiles bool) (*importViaGRPCTask, string, string) {
	backupRoot := t.TempDir()
	milvusRoot := t.TempDir()

	srcDir := filepath.Join(backupRoot, "backup/binlog/insert_log/1/2/3") + "/"
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.WriteFile(srcDir+"file1", []byte("hello"), 0o644))

	deltaDir := filepath.Join(backupRoot, "delta") + "/"
	require.NoError(t, os.MkdirAll(deltaDir, 0o755))
	require.NoError(t, os.WriteFile(deltaDir+"delete1", []byte("delta"), 0o644))

	backupCli := newTestStorageClient(t, cfg.ProviderLocal)
	milvusCli := newTestStorageClient(t, cfg.ProviderLocal)

	mgr := jobstate.NewStore()
	target := collref.New("db", "coll")
	restoreTracker, addErr := mgr.AddRestoreTask("task1", enableResume)
	require.NoError(t, addErr)
	collTracker := restoreTracker.NewCollTracker(target, 10)

	args := importTaskArgs{
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
	}
	dir := partitionDir{insertLogDir: srcDir, deltaLogDir: deltaDir, size: 10}
	job := args.registerImport("p1", dirGroup{}, []partitionDir{dir})
	task := newGRPCImportTask(args, job, "p1", dirGroup{}, dir)
	stagedGlob := filepath.Join(milvusRoot, "restore-temp-task1-db-coll-*", "backup/binlog/insert_log/1/2/3/file1")
	return task, backupRoot, stagedGlob
}

// TestImportViaGRPCTaskExecute runs a task end to end against the local
// filesystem: it copies the data into the target storage, imports it, and
// cleans its temp dir up, all inside the task.
func TestImportViaGRPCTaskExecute(t *testing.T) {
	t.Run("CopyImportAndCleanup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, backupRoot, stagedGlob := newLocalGRPCTask(t, false, false)

			var gotPaths []string
			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().BulkInsert(mock.Anything, mock.Anything).
				Run(func(_ context.Context, in milvus.GrpcBulkInsertInput) {
					gotPaths = in.Paths
					assert.False(t, in.DisableRetry)
				}).
				Return(int64(42), nil).Once()
			grpcCli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
				Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			task.args.grpcCli = grpcCli

			require.NoError(t, task.Execute(t.Context()))

			// the import saw the staged copy under the target's local path
			require.Len(t, gotPaths, 2)
			assert.True(t, strings.HasPrefix(gotPaths[0], task.args.milvusLocalPath+"/restore-temp-task1-db-coll-"))
			assert.True(t, strings.HasSuffix(gotPaths[0], "/backup/binlog/insert_log/1/2/3/"))
			assert.Equal(t, strings.TrimSuffix(gotPaths[0], "backup/binlog/insert_log/1/2/3/")+"delta/", gotPaths[1])

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
			task, _, stagedGlob := newLocalGRPCTask(t, false, true)

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

	for _, tc := range []struct {
		name         string
		enableResume bool
		wantFiles    int
	}{
		{"FailedImportCleansStagingByDefault", false, 0},
		{"FailedResumableImportRetainsStaging", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, _, stagedGlob := newLocalGRPCTask(t, tc.enableResume, false)

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
			assert.Len(t, copied, tc.wantFiles)
			assert.Len(t, task.args.jobs.TrackersForBinlogImports(), tc.wantFiles)
		})
	}
}

func TestGRPCImportResume(t *testing.T) {
	t.Run("KeepsIDWhenCanceledDuringSubmission", func(t *testing.T) {
		task, _, _ := newLocalGRPCTask(t, true, false)
		grpc := milvus.NewMockGrpc(t)
		task.args.grpcCli = grpc
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		grpc.EXPECT().BulkInsert(mock.Anything, mock.Anything).RunAndReturn(func(submitCtx context.Context, _ milvus.GrpcBulkInsertInput) (int64, error) {
			cancel() // A sibling fails while Milvus is creating this job.
			return 42, submitCtx.Err()
		}).Once()
		require.ErrorIs(t, task.Execute(ctx), context.Canceled)
		id, done := task.job.Current()
		assert.Equal(t, "42", id)
		assert.False(t, done)

		grpc.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
		require.NoError(t, task.Execute(t.Context()))
	})
	t.Run("SubmissionTimesOutWithoutID", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			task, _, _ := newLocalGRPCTask(t, true, false)
			grpc := milvus.NewMockGrpc(t)
			task.args.grpcCli = grpc
			grpc.EXPECT().BulkInsert(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ milvus.GrpcBulkInsertInput) (int64, error) {
				<-ctx.Done()
				return 0, ctx.Err()
			}).Once()
			require.ErrorIs(t, task.Execute(t.Context()), context.DeadlineExceeded)
			id, done := task.job.Current()
			assert.Empty(t, id)
			assert.False(t, done)

			grpc.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(43), nil).Once()
			grpc.EXPECT().GetBulkInsertState(mock.Anything, int64(43)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
			require.NoError(t, task.Execute(t.Context()))
		})
	})
	t.Run("AdoptsIDAfterQueryError", func(t *testing.T) {
		task, backupRoot, glob := newLocalGRPCTask(t, true, false)
		grpc := milvus.NewMockGrpc(t)
		task.args.grpcCli = grpc
		grpc.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
		grpc.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(nil, assert.AnError).Once()
		require.ErrorIs(t, task.Execute(t.Context()), assert.AnError)
		files, err := filepath.Glob(glob)
		require.NoError(t, err)
		require.Len(t, files, 1)
		// Resuming a bound job only polls Milvus and cleans its saved directory.
		require.NoError(t, os.RemoveAll(backupRoot))
		saved := task.args.jobs.TrackersForBinlogImports()[0]
		rebuilt := task.args.importTask(StrategyPerSeg, saved, nil)
		grpc.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
		require.NoError(t, rebuilt.Execute(t.Context()))
		require.NoError(t, rebuilt.Execute(t.Context()))
		assert.Empty(t, saved.Tracker.StagingDir())
		_, err = os.Stat(files[0])
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("RebuiltTaskRecopiesBatchIntoSavedDirectory", func(t *testing.T) {
		task, _, glob := newLocalGRPCTask(t, true, false)
		grpc := milvus.NewMockGrpc(t)
		task.args.grpcCli = grpc
		grpc.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(0), assert.AnError).Once()
		require.ErrorIs(t, task.Execute(t.Context()), assert.AnError)
		files, err := filepath.Glob(glob)
		require.NoError(t, err)
		require.Len(t, files, 1)
		saved := task.args.jobs.TrackersForBinlogImports()[0]
		stagedDelta := filepath.Join(task.args.milvusRootPath, saved.Tracker.StagingDir(), "delta", "delete1")
		require.NoError(t, os.WriteFile(stagedDelta, []byte("x"), 0o644))
		// A staged insert log with the correct size must also be overwritten.
		require.NoError(t, os.WriteFile(files[0], []byte("stale"), 0o644))

		rebuilt := task.args.importTask(StrategyPerSeg, saved, nil)
		grpc.EXPECT().BulkInsert(mock.Anything, mock.Anything).Run(func(_ context.Context, in milvus.GrpcBulkInsertInput) {
			assert.Equal(t, []string{filepath.Dir(files[0]) + "/", filepath.Dir(stagedDelta) + "/"}, in.Paths)
			insert, err := os.ReadFile(files[0])
			require.NoError(t, err)
			assert.Equal(t, "hello", string(insert))
			data, err := os.ReadFile(stagedDelta)
			require.NoError(t, err)
			assert.Equal(t, "delta", string(data))
		}).Return(int64(43), nil).Once()
		grpc.EXPECT().GetBulkInsertState(mock.Anything, int64(43)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
		require.NoError(t, rebuilt.Execute(t.Context()))
		assert.Empty(t, saved.Tracker.StagingDir())
		assert.NoFileExists(t, files[0])
		assert.NoFileExists(t, stagedDelta)
	})
	t.Run("ReplacesKnownFailedID", func(t *testing.T) {
		task, _, _ := newLocalGRPCTask(t, true, false)
		grpc := milvus.NewMockGrpc(t)
		task.args.grpcCli = grpc
		task.job.Bind("42")
		grpc.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportFailedAndCleaned}, nil).Once()
		grpc.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(43), nil).Once()
		grpc.EXPECT().GetBulkInsertState(mock.Anything, int64(43)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
		require.NoError(t, task.Execute(t.Context()))
		id, done := task.job.Current()
		assert.Equal(t, "43", id)
		assert.True(t, done)
	})
}

func TestGRPCResumeReplaysAtMostOnce(t *testing.T) {
	failed := func(reason string) *milvuspb.GetImportStateResponse {
		return &milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportFailed,
			Infos: []*commonpb.KeyValuePair{{Key: "failed_reason", Value: reason}}}
	}
	for _, tc := range []struct {
		name      string
		state     *milvuspb.GetImportStateResponse
		lookupErr error
		replay    bool
	}{
		{"Missing", nil, milvus.ErrImportJobNotFound, false},
		{"MissingDuringGC", failed("import job does not exist, jobID=42"), nil, false},
		{"RemoteTimeout", failed("job timed out"), nil, true},
		{"LookupTimeout", nil, context.DeadlineExceeded, false},
		{"OtherIDDuringGC", failed("import job does not exist, jobID=420"), nil, false},
		{"UnknownStateFromV1Proxy", failed("unknown import job state"), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, _, _ := newLocalGRPCTask(t, true, false)
			cli := milvus.NewMockGrpc(t)
			task.args.grpcCli = cli
			task.job.Bind("42")
			cli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(tc.state, tc.lookupErr).Once()
			wantID := "42"
			if tc.replay {
				wantID = "43"
				cli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(43), nil).Once()
				cli.EXPECT().GetBulkInsertState(mock.Anything, int64(43)).Return(failed("replacement failed"), nil).Once()
			}
			err := task.Execute(t.Context())
			require.Error(t, err)
			if tc.replay {
				assert.ErrorContains(t, err, "replacement failed")
			}
			id, done := task.job.Current()
			assert.Equal(t, wantID, id)
			assert.False(t, done)
		})
	}
}

func TestGRPCResumeLostReplacementReply(t *testing.T) {
	task, _, _ := newLocalGRPCTask(t, true, false)
	server := milvus.NewMockGrpc(t)
	config := resumeTestConfig(t, server)
	cli, err := milvus.NewGrpc(&config.Milvus)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cli.Close()) })
	task.args.grpcCli = cli
	task.job.Bind("42")
	server.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(&milvuspb.GetImportStateResponse{
		State: commonpb.ImportState_ImportFailedAndCleaned, Status: &commonpb.Status{},
	}, nil).Once()
	server.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(0), status.Error(codes.Unavailable, "reply lost")).Once()
	require.ErrorContains(t, task.Execute(t.Context()), "reply lost")
	id, done := task.job.Current()
	assert.Empty(t, id)
	assert.False(t, done)
}

func TestGRPCResumeWaitFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		task, _, _ := newLocalGRPCTask(t, true, false)
		cli := milvus.NewMockGrpc(t)
		task.args.grpcCli = cli
		task.job.Bind("42")
		cli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
			Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportStarted}, nil).Once()
		cli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
			Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportFailed}, nil).Once()

		assert.ErrorIs(t, task.Execute(t.Context()), milvus.ErrRemoteJobFailed)
		id, done := task.job.Current()
		assert.Equal(t, "42", id)
		assert.False(t, done)

		// Only the next execution may replace the old job. A failed replacement
		// must also stop rather than retrying again in this execution.
		cli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).
			Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportFailedAndCleaned}, nil).Once()
		cli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(43), nil).Once()
		cli.EXPECT().GetBulkInsertState(mock.Anything, int64(43)).
			Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportFailed}, nil).Once()
		assert.ErrorIs(t, task.Execute(t.Context()), milvus.ErrRemoteJobFailed)
		id, done = task.job.Current()
		assert.Equal(t, "43", id)
		assert.False(t, done)
	})
}

func TestGRPCImportRetriesUnavailableWithoutResume(t *testing.T) {
	task, _, _ := newLocalGRPCTask(t, false, false)
	server := milvus.NewMockGrpc(t)
	config := resumeTestConfig(t, server)
	cli, err := milvus.NewGrpc(&config.Milvus)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cli.Close()) })
	task.args.grpcCli = cli
	server.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(0), status.Error(codes.Unavailable, "temporarily unavailable")).Once()
	server.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
	server.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted, Status: &commonpb.Status{}}, nil).Once()
	assert.NoError(t, task.Execute(t.Context()))
}

func TestGRPCImportPollsUnknownState(t *testing.T) {
	for _, tc := range []struct {
		name         string
		enableResume bool
	}{{"WithoutResume", false}, {"ResumeSavedJob", true}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				task, _, _ := newLocalGRPCTask(t, tc.enableResume, false)
				cli := milvus.NewMockGrpc(t)
				task.args.grpcCli = cli
				cli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(&milvuspb.GetImportStateResponse{State: 99}, nil).Once()
				cli.EXPECT().GetBulkInsertState(mock.Anything, int64(42)).Return(&milvuspb.GetImportStateResponse{State: commonpb.ImportState_ImportCompleted}, nil).Once()
				if tc.enableResume {
					task.job.Bind("42")
				} else {
					cli.EXPECT().BulkInsert(mock.Anything, mock.Anything).Return(int64(42), nil).Once()
				}
				require.NoError(t, task.Execute(t.Context()))
				_, done := task.job.Current()
				assert.True(t, done)
			})
		})
	}
}
