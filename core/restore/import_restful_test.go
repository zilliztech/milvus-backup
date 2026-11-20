package restore

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestRESTImportPartialStaging(t *testing.T) {
	for _, tc := range []struct {
		name          string
		resumable     bool
		keepTempFiles bool
		wantFiles     int
	}{
		{"DefaultCleansPartialCopy", false, false, 0},
		{"ResumableRetainsPartialCopy", true, false, 1},
		{"KeepTempFilesRetainsPartialCopy", false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local, _, stagedGlob := newLocalGRPCTask(t, tc.resumable, tc.keepTempFiles)
			dir := local.dir
			// Listing a child of a regular file fails after the insert logs were copied.
			dir.deltaLogDir = filepath.Join(dir.insertLogDir, "file1", "delta") + "/"
			task := newRESTImportTask(local.args, local.job, "p1", dirGroup{}, []partitionDir{dir}, nil)
			assert.ErrorContains(t, task.Execute(t.Context()), "copy delta log dir")
			files, err := filepath.Glob(stagedGlob)
			require.NoError(t, err)
			assert.Len(t, files, tc.wantFiles)
		})
	}
}

func TestRESTImportResume(t *testing.T) {
	for _, tc := range []struct {
		name           string
		oldJobResponse string
		replay         bool
		polls          int
	}{
		{"Missing", `{"code":2101,"message":"import job does not exist, jobID=42: importing data failed on server side"}`, false, 1},
		{"MissingDuringGC", `{"code":0,"data":{"state":"Failed","reason":"import job does not exist, jobID=42"}}`, false, 1},
		{"TerminalFailure", `{"code":0,"data":{"state":"Failed","reason":"job timed out"}}`, true, 2},
		{"OtherID", `{"code":2100,"message":"import job does not exist, jobID=420"}`, false, 10},
		{"OtherIDDuringGC", `{"code":0,"data":{"state":"Failed","reason":"import job does not exist, jobID=420"}}`, false, 1},
		{"QueryError", `{"code":2100,"message":"failed to read import metadata"}`, false, 10},
		{"EmptyResponse", `{}`, false, 1},
		{"MissingStatusCode", `{"data":{"state":"Failed","reason":"job timed out"}}`, false, 1},
		{"UnknownState", `{"code":0,"data":{"state":"Unknown"}}`, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var creates, polls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v2/vectordb/jobs/import/describe":
					polls.Add(1)
					var lookup milvus.GetProgressReq
					if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&lookup)) {
						return
					}
					assert.Equal(t, "db", lookup.DBName)
					if lookup.JobId == "42" {
						io.WriteString(w, tc.oldJobResponse)
						return
					}
					assert.Equal(t, "43", lookup.JobId)
					io.WriteString(w, `{"code":0,"data":{"jobId":"43","state":"Completed"}}`)
				case "/v2/vectordb/jobs/import/create":
					creates.Add(1)
					body, err := io.ReadAll(r.Body)
					assert.NoError(t, err)
					assert.JSONEq(t, `{"dbName":"db","collectionName":"coll","partitionName":"p1","files":[["a","b"],["c"]],"options":{"skip_disk_quota_check":"true","backup":"true","end_ts":"7","storage_version":"2"}}`, string(body))
					io.WriteString(w, `{"code":0,"data":{"jobId":"43"}}`)
				default:
					assert.Fail(t, "unexpected path", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			config := cfg.New()
			config.Milvus.Rest.Endpoint.Val = srv.URL
			cli, err := milvus.NewRestful(&config.Milvus)
			require.NoError(t, err)
			target := collref.New("db", "coll")
			restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
			require.NoError(t, addErr)
			coll := restoreTracker.NewCollTracker(target, 10)
			args := importTaskArgs{target: target, jobs: coll, restfulCli: cli, logger: zap.NewNop(), milvusStorage: newTestStorageClient(t, cfg.ProviderMinio)}
			dirs := []partitionDir{{insertLogDir: "a", deltaLogDir: "b"}, {insertLogDir: "c"}}
			args.registerImport(0, "p1", dirGroup{timestamp: 7, storageVersion: 2}, dirs)
			stored := coll.TrackersForBinlogImports()[0]
			stored.Tracker.Bind("42")
			task := args.importTask(StrategyMergedSeg, stored, nil)
			err = task.Execute(t.Context())
			id, done := stored.Tracker.Current()
			if tc.replay {
				require.NoError(t, err)
				assert.Equal(t, "43", id)
				assert.True(t, done)
				require.NoError(t, task.Execute(t.Context()))
				assert.EqualValues(t, 1, creates.Load())
			} else {
				require.Error(t, err)
				assert.Equal(t, "42", id)
				assert.False(t, done)
				assert.Zero(t, creates.Load())
			}
			assert.EqualValues(t, tc.polls, polls.Load())
		})
	}
}

func TestRESTfulImportPlannerPlanTasks(t *testing.T) {
	// planning only assembles tasks, so no client is touched; the job halves it
	// mints land on a throwaway tracker.
	newPlanner := func(maxSegs int, multiL0 bool) *restfulImportPlanner {
		restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("task-1", false)
		require.NoError(t, addErr)
		jobs := restoreTracker.NewCollTracker(collref.New("db", "coll"), 0)
		return &restfulImportPlanner{
			args:            importTaskArgs{maxSegsPerImportJob: maxSegs, jobs: jobs, logger: zap.NewNop()},
			multiL0InOneJob: multiL0,
		}
	}

	dirs := func(n int) []partitionDir {
		ds := make([]partitionDir, 0, n)
		for range n {
			ds = append(ds, partitionDir{insertLogDir: "insert", size: 1})
		}
		return ds
	}

	// 10 dirs chunk into 5 tasks of 2 dirs each
	t.Run("ChunkByMaxSegsPerImportJob", func(t *testing.T) {
		tasks := newPlanner(2, true).planTasks(0, "p1", []dirGroup{{dirs: dirs(10)}})
		require.Len(t, tasks, 5)
		for _, tk := range tasks {
			assert.Len(t, tk.(*importViaRESTFulTask).importer.dirs, 2)
		}
	})

	// the group's request parameters ride along on every task
	t.Run("GroupParamsRideAlong", func(t *testing.T) {
		tasks := newPlanner(256, true).planTasks(0, "p1",
			[]dirGroup{{timestamp: 7, storageVersion: 2, isL0: true, dirs: dirs(3)}})
		require.Len(t, tasks, 1)
		im := tasks[0].(*importViaRESTFulTask).importer
		assert.Equal(t, "p1", im.partitionName)
		assert.Equal(t, uint64(7), im.timestamp)
		assert.Equal(t, int64(2), im.storageVersion)
		assert.True(t, im.isL0)
	})

	// a target below 2.6.5 takes one L0 segment per job, so each L0 dir
	// becomes a task of its own
	t.Run("L0SingleDirPerTask", func(t *testing.T) {
		tasks := newPlanner(2, false).planTasks(0, "p1", []dirGroup{{isL0: true, dirs: dirs(10)}})
		require.Len(t, tasks, 10)
		for _, tk := range tasks {
			assert.Len(t, tk.(*importViaRESTFulTask).importer.dirs, 1)
		}
	})
}

func TestToPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	importer := &restfulImporter{args: importTaskArgs{milvusStorage: newTestStorageClient(t, cfg.ProviderMinio)}}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, importer.toPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert"}, importer.toPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, importer.toPaths(dir))

	// empty
	dir = partitionDir{}
	assert.Empty(t, importer.toPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	importer = &restfulImporter{args: importTaskArgs{
		milvusStorage:   newTestStorageClient(t, cfg.ProviderLocal),
		milvusLocalPath: "/var/lib/milvus/data",
	}}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, importer.toPaths(dir))

	// paths already absolute are left alone (a same-directory local backup)
	dir = partitionDir{insertLogDir: "/data/insert"}
	assert.Equal(t, []string{"/data/insert"}, importer.toPaths(dir))

	// a trailing slash is kept: the LocalChunkManager globs the prefix
	dir = partitionDir{insertLogDir: "insert/"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert/"}, importer.toPaths(dir))
}

func TestRESTResumeWaitFailure(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		name := "Binlog"
		if snapshot {
			name = "Snapshot"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var log []string
				var mu sync.Mutex
				cli := newTwoPCCluster("target", &log, &mu, "Importing", "Failed")
				local, _, _ := newLocalGRPCTask(t, true, false)
				local.args.restfulCli = cli
				task := newRESTImportTask(local.args, local.job, "p1", dirGroup{}, []partitionDir{local.dir}, nil)
				tracker := local.job
				if snapshot {
					importTask := newSnapshotImportTask(t, nil)
					importTask.importer.restfulCli = cli
					task, tracker = importTask, importTask.importer.job
				}
				tracker.Bind("old")

				assert.ErrorIs(t, task.Execute(t.Context()), milvus.ErrRemoteJobFailed)
				id, done := tracker.Current()
				assert.Equal(t, "old", id)
				assert.False(t, done)
				assert.Zero(t, cli.count("create"))

				// The initial lookup now confirms failure; replace once, even if
				// that replacement also fails immediately.
				assert.ErrorIs(t, task.Execute(t.Context()), milvus.ErrRemoteJobFailed)
				id, done = tracker.Current()
				assert.Equal(t, "job1", id)
				assert.False(t, done)
				assert.Equal(t, 1, cli.count("create"))

				cli.states, cli.calls = []string{"Failed", "Completed"}, 0
				assert.NoError(t, task.Execute(t.Context()))
				id, done = tracker.Current()
				assert.Equal(t, "job2", id)
				assert.True(t, done)
				assert.Equal(t, 2, cli.count("create"))
			})
		})
	}
}

func TestCompletedBinlogResumeKeepsResult(t *testing.T) {
	for _, protocol := range []string{"GRPC", "REST"} {
		t.Run(protocol, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				target := collref.New("db", "coll")
				tracker, addErr := jobstate.NewStore().AddRestoreTask("task", true)
				require.NoError(t, addErr)
				coll := tracker.NewCollTracker(target, 1)
				job := coll.NewJob(1)
				job.Bind("42")
				job.SetCompleted()
				before := tracker.Snapshot().Colls[target].Jobs[0]
				time.Sleep(time.Second)

				args := importTaskArgs{target: target, jobs: coll, logger: zap.NewNop()}
				if protocol == "GRPC" {
					task := newGRPCImportTask(args, job, "", dirGroup{}, partitionDir{})
					require.NoError(t, task.Execute(t.Context()))
				} else {
					task := newRESTImportTask(args, job, "", dirGroup{}, nil, nil)
					require.NoError(t, task.Execute(t.Context()))
				}
				assert.Equal(t, before, tracker.Snapshot().Colls[target].Jobs[0])
			})
		})
	}
}
