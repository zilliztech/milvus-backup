package restore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// twoPCCluster is a scripted Restful endpoint for the two-phase import tests.
// It answers GetBulkInsertState from a sequence (repeating the last entry),
// captures how the job was created, and appends every lifecycle call to a log
// shared with the other cluster, so a test can assert the order of events
// across the two ends — and that the secondary ends never get a lifecycle call
// at all, since a replication target refuses client-side commit/abort.
type twoPCCluster struct {
	name string

	log   *[]string
	logMu *sync.Mutex

	states []string
	calls  int

	manualCommit  bool
	gotBulkInsert milvus.BulkInsertV2Input
	recordStates  bool
	commitErr     error
	abortErr      error
	describeErr   error
	reason        string
}

func newTwoPCCluster(name string, log *[]string, logMu *sync.Mutex, states ...string) *twoPCCluster {
	return &twoPCCluster{name: name, log: log, logMu: logMu, states: states, reason: "disk quota exceeded"}
}

func (c *twoPCCluster) record(op string) {
	c.logMu.Lock()
	defer c.logMu.Unlock()
	*c.log = append(*c.log, c.name+":"+op)
}

func (c *twoPCCluster) BulkInsert(_ context.Context, in milvus.BulkInsertV2Input) (string, error) {
	c.manualCommit = in.ManualCommit
	c.gotBulkInsert = in
	c.record("create")
	return fmt.Sprintf("job%d", c.count("create")), nil
}

func (c *twoPCCluster) GetBulkInsertState(_ context.Context, _ string, jobID string) (*milvus.GetProcessResp, error) {
	if c.describeErr != nil {
		return nil, c.describeErr
	}
	i := min(c.calls, len(c.states)-1)
	c.calls++

	resp := &milvus.GetProcessResp{}
	if c.states[i] == "missing" {
		resp.Code = 2101
		resp.Message = "import job does not exist, jobID=" + jobID
		return resp, nil
	}
	resp.Data.State = c.states[i]
	if c.recordStates {
		c.record("state:" + c.states[i])
	}
	if c.states[i] == string(milvus.ImportStateFailed) {
		resp.Data.Reason = c.reason
	}
	return resp, nil
}

func (c *twoPCCluster) GetSegmentInfo(context.Context, string, int64, int64) (*milvus.SegmentInfo, error) {
	return nil, nil
}

func (c *twoPCCluster) CommitImport(context.Context, string, string) error {
	c.record("commit")
	return c.commitErr
}

func (c *twoPCCluster) AbortImport(context.Context, string, string) error {
	c.record("abort")
	return c.abortErr
}

func (c *twoPCCluster) count(op string) int {
	c.logMu.Lock()
	defer c.logMu.Unlock()
	n := 0
	for _, e := range *c.log {
		if e == c.name+":"+op {
			n++
		}
	}
	return n
}

// newTwoPCTask builds the two-phase task against the scripted clusters, with no
// staging: the dirs are treated as already readable by the target.
func newTwoPCTask(t *testing.T, enableResume bool, local *twoPCCluster, secondaries ...*twoPCCluster) *importViaRESTFul2PCTask {
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("task1", enableResume)
	require.NoError(t, addErr)
	collTracker := restoreTracker.NewCollTracker(target, 1)

	importer := &restfulImporter{
		args: importTaskArgs{
			taskID:        "task1",
			target:        target,
			milvusStorage: newTestStorageClient(t, cfg.ProviderMinio),
			restfulCli:    local,
			jobs:          collTracker,
			logger:        zap.NewNop(),
		},

		partitionName: "p1",
		dirs:          []partitionDir{{insertLogDir: "insert", size: 1}},
	}
	importer.job = importer.args.registerImport("p1", dirGroup{}, importer.dirs)
	importSecondaries := make([]importSecondary, 0, len(secondaries))
	for _, s := range secondaries {
		importSecondaries = append(importSecondaries, importSecondary{addr: s.name, cli: s})
	}
	return &importViaRESTFul2PCTask{importer: importer, secondaries: importSecondaries}
}

func TestImportViaRESTFul2PCTaskExecute(t *testing.T) {
	// The happy path commits on the local cluster only; the secondary mirrors
	// are committed by the replicated fence message, so the task just waits
	// them out and never issues a lifecycle call against a secondary.
	t.Run("CommitsLocalThenWaitsSecondaries", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateImporting),
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary := newTwoPCCluster("secondary", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCommitting),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, true, local, secondary)
			require.NoError(t, task.Execute(t.Context()))

			assert.True(t, local.manualCommit)
			assert.Equal(t, []string{"local:create", "local:commit"}, log)
			assert.Zero(t, local.count("abort"))
			assert.Zero(t, secondary.count("commit"))
			assert.Zero(t, secondary.count("abort"))
		})
	})

	// Past the local commit there is no way back: a mirror that fails on its
	// own does not take the replicated commit, so the error says the two ends
	// have diverged — and nothing aborts, the local data stays.
	t.Run("SecondaryMirrorFailureDiverges", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary := newTwoPCCluster("secondary", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateFailed))

			task := newTwoPCTask(t, true, local, secondary)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "disk quota exceeded")
			assert.Contains(t, err.Error(), "already committed")
			assert.Contains(t, err.Error(), "diverge")

			err = task.Execute(t.Context())
			require.ErrorContains(t, err, "already committed")

			assert.Equal(t, []string{"local:create", "local:commit"}, log)
			assert.Zero(t, local.count("abort"))
			assert.Zero(t, secondary.count("abort"))
		})
	})

	// A failure before the commit aborts the local job; the abort's rollback
	// message replicates and fails the mirrors, so the secondaries are never
	// called.
	t.Run("LocalFailureAbortsLocally", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu, string(milvus.ImportStateFailed))
			secondary := newTwoPCCluster("secondary", &log, &mu, string(milvus.ImportStateUncommitted))

			task := newTwoPCTask(t, true, local, secondary)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "disk quota exceeded")

			assert.Equal(t, []string{"local:create", "local:abort"}, log)
			assert.Zero(t, secondary.count("commit"))
			assert.Zero(t, secondary.count("abort"))
		})
	})

	// The mirror is created around the submit, so a secondary that still has no
	// trace of the job a minute later means the secondary is wrong, not slow.
	t.Run("SecondaryMirrorTimeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu, string(milvus.ImportStateUncommitted))
			secondary := newTwoPCCluster("secondary", &log, &mu, "missing")

			task := newTwoPCTask(t, false, local, secondary)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "secondaries option")

			assert.Equal(t, []string{"local:create", "local:abort"}, log)
		})
	})

	// A mirror that takes a few polls to show up is waited for.
	t.Run("SecondaryMirrorAppearsLate", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary := newTwoPCCluster("secondary", &log, &mu,
				"", "",
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, false, local, secondary)
			require.NoError(t, task.Execute(t.Context()))
			assert.Equal(t, []string{"local:create", "local:commit"}, log)
		})
	})

	// A retried task can meet a secondary mirror that is already committing:
	// the crashed first run's commit replicated before the retry got there.
	t.Run("SecondaryAlreadyCommitting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary := newTwoPCCluster("secondary", &log, &mu,
				string(milvus.ImportStateCommitting),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, true, local, secondary)
			require.NoError(t, task.Execute(t.Context()))
			assert.Zero(t, secondary.count("commit"))
		})
	})

	// Every secondary must show the mirror before the local commit, and every
	// secondary's mirror is waited out after it.
	t.Run("TwoSecondaries", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary1 := newTwoPCCluster("secondary1", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary2 := newTwoPCCluster("secondary2", &log, &mu, "missing",
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, true, local, secondary1, secondary2)
			require.NoError(t, task.Execute(t.Context()))
			assert.Equal(t, []string{"local:create", "local:commit"}, log)
			assert.Zero(t, secondary1.count("commit"))
			assert.Zero(t, secondary2.count("commit"))
		})
	})

	// A mirror that never finishes after the local commit means the fence did
	// not land; the timeout names the secondary and points at the stream.
	t.Run("SecondaryCommitTimeoutNamesIt", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary1 := newTwoPCCluster("secondary1", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary2 := newTwoPCCluster("secondary2", &log, &mu, string(milvus.ImportStateUncommitted))

			task := newTwoPCTask(t, true, local, secondary1, secondary2)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "secondary2")
			assert.Contains(t, err.Error(), "replication stream")
		})
	})
}

func TestRESTfulImportPlannerSecondaryFork(t *testing.T) {
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("task-1", false)
	require.NoError(t, addErr)
	jobs := restoreTracker.NewCollTracker(collref.New("db", "coll"), 0)
	planner := &restfulImportPlanner{
		args:            importTaskArgs{maxSegsPerImportJob: 2, jobs: jobs, logger: zap.NewNop()},
		multiL0InOneJob: true,
	}
	g := dirGroup{dirs: []partitionDir{{insertLogDir: "insert", size: 1}}}

	// Without a secondary every task is the plain single-phase shape.
	task := planner.newTask("p1", g, g.dirs)
	_, ok := task.(*importViaRESTFulTask)
	assert.True(t, ok)

	// With secondaries the same import rides the two-phase task, driving the
	// same importer device.
	planner.secondaries = []importSecondary{{
		addr: "secondary",
		cli:  newTwoPCCluster("secondary", &[]string{}, &sync.Mutex{}, string(milvus.ImportStateUncommitted)),
	}}
	task = planner.newTask("p1", g, g.dirs)
	twoPC, ok := task.(*importViaRESTFul2PCTask)
	require.True(t, ok)
	assert.Equal(t, planner.secondaries, twoPC.secondaries)
	assert.NotNil(t, twoPC.importer)
}

func TestTwoPCResumeAfterLostCommitResponse(t *testing.T) {
	for _, tc := range []struct {
		phase   milvus.ImportState
		commits int
	}{
		{milvus.ImportStateUncommitted, 2},
		{milvus.ImportStateCommitting, 1},
		{milvus.ImportStateCompleted, 1},
		{milvus.ImportStateFailed, 1},
		{"missing", 1},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("primary", &log, &mu, string(milvus.ImportStateUncommitted))
			secondary := newTwoPCCluster("secondary", &log, &mu, string(milvus.ImportStateUncommitted))
			task := newTwoPCTask(t, true, local, secondary)
			local.commitErr = assert.AnError
			require.ErrorIs(t, task.Execute(t.Context()), assert.AnError)
			id, done := task.importer.job.Current()
			assert.Equal(t, "job1", id)
			assert.False(t, done)

			local.commitErr = nil
			local.reason = "failed to read"
			local.states = []string{string(tc.phase), string(milvus.ImportStateCompleted)}
			local.calls = 0
			secondary.states = []string{string(milvus.ImportStateCompleted)}
			err := task.Execute(t.Context())
			if tc.phase == milvus.ImportStateFailed || tc.phase == "missing" {
				require.ErrorContains(t, err, "after commit was requested")
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, 1, local.count("create"))
			assert.Zero(t, local.count("abort"))
			assert.Equal(t, tc.commits, local.count("commit"))
		})
	}
}

func TestTwoPCResumeReplacesConfirmedPreCommitFailure(t *testing.T) {
	var log []string
	var mu sync.Mutex
	local := newTwoPCCluster("primary", &log, &mu, string(milvus.ImportStateFailed))
	secondary := newTwoPCCluster("secondary", &log, &mu, string(milvus.ImportStateCompleted))
	task := newTwoPCTask(t, true, local, secondary)
	require.ErrorContains(t, task.Execute(t.Context()), "disk quota exceeded")
	local.abortErr = assert.AnError
	require.ErrorIs(t, task.Execute(t.Context()), assert.AnError)
	assert.Equal(t, 1, local.count("create"))
	local.abortErr = nil
	local.calls = 0
	local.states = []string{string(milvus.ImportStateFailed), string(milvus.ImportStateUncommitted), string(milvus.ImportStateCompleted)}
	require.NoError(t, task.Execute(t.Context()))
	id, done := task.importer.job.Current()
	assert.Equal(t, "job2", id)
	assert.True(t, done)
	assert.Equal(t, 2, local.count("create"))
	assert.Equal(t, 1, local.count("commit"))
}

func TestTwoPCResumeRetainsStagedBinlogs(t *testing.T) {
	var log []string
	var mu sync.Mutex
	local := newTwoPCCluster("primary", &log, &mu, string(milvus.ImportStateUncommitted), string(milvus.ImportStateUncommitted), string(milvus.ImportStateCompleted))
	secondary := newTwoPCCluster("secondary", &log, &mu, string(milvus.ImportStateCompleted))
	task := newTwoPCTask(t, true, local, secondary)
	im := task.importer
	im.args.staging = true
	im.args.backupDir = t.TempDir() + "/"
	im.args.milvusRootPath = t.TempDir()
	im.args.backupStorage = newTestStorageClient(t, cfg.ProviderLocal)
	im.args.milvusStorage = newTestStorageClient(t, cfg.ProviderLocal)
	im.args.copySem = semaphore.NewWeighted(1)
	src := filepath.Join(im.args.backupDir, "insert")
	require.NoError(t, os.Mkdir(src, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "log"), []byte("binlog"), 0o644))
	im.dirs = []partitionDir{{insertLogDir: src + "/"}}

	secondary.describeErr = assert.AnError
	require.ErrorIs(t, task.Execute(t.Context()), assert.AnError)
	dir := im.job.StagingDir()
	require.NotEmpty(t, dir)
	file := filepath.Join(im.args.milvusRootPath, dir, "insert", "log")
	assert.FileExists(t, file)
	assert.Zero(t, local.count("abort"))
	secondary.describeErr = milvus.ErrImportJobNotFound
	local.calls = 0
	require.ErrorIs(t, task.Execute(t.Context()), milvus.ErrImportJobNotFound)
	assert.Equal(t, 1, local.count("create"))
	assert.Zero(t, local.count("abort"))
	assert.FileExists(t, file)
	secondary.describeErr = nil
	require.NoError(t, os.RemoveAll(src))
	require.NoError(t, task.Execute(t.Context()))
	assert.Equal(t, 1, local.count("create"))
	assert.NoFileExists(t, file)
	assert.Empty(t, im.job.StagingDir())
}

func TestTwoPCResubmitRecopiesIntoSavedDirectory(t *testing.T) {
	local, _, _ := newLocalGRPCTask(t, true, false)
	var events []string
	var mu sync.Mutex
	primary := newTwoPCCluster("primary", &events, &mu, string(milvus.ImportStateUncommitted))
	mirror := newTwoPCCluster("mirror", &events, &mu, string(milvus.ImportStateCompleted))
	mirror.describeErr = assert.AnError
	args := local.args
	args.restfulCli = primary
	secondaries := []importSecondary{{addr: "mirror", cli: mirror}}
	saved := args.jobs.TrackersForBinlogImports()[0]
	require.ErrorIs(t, args.importTask(StrategyMergedSeg, saved, secondaries).Execute(t.Context()), assert.AnError)
	dir := saved.Tracker.StagingDir()
	require.NotEmpty(t, dir)
	// A confirmed failed primary before commit permits one replacement. With
	// the same byte count, verification by size alone cannot repair this file.
	file := filepath.Join(args.milvusRootPath, dir, "backup/binlog/insert_log/1/2/3/file1")
	require.NoError(t, os.WriteFile(file, []byte("stale"), 0o644))
	primary.states = []string{"Failed", string(milvus.ImportStateUncommitted), string(milvus.ImportStateCompleted)}
	primary.calls = 0
	require.ErrorIs(t, args.importTask(StrategyMergedSeg, saved, secondaries).Execute(t.Context()), assert.AnError)
	assert.Equal(t, dir, saved.Tracker.StagingDir())
	content, err := os.ReadFile(file)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(content))
	id, done := saved.Tracker.Current()
	assert.Equal(t, "job2", id)
	assert.False(t, done)
	mirror.describeErr = nil
	require.NoError(t, args.importTask(StrategyMergedSeg, saved, secondaries).Execute(t.Context()))
	assert.NoFileExists(t, file)
	assert.Equal(t, 2, primary.count("create"))
}

func TestTwoPCDefaultCleansPartialStaging(t *testing.T) {
	local, _, stagedGlob := newLocalGRPCTask(t, false, false)
	dir := local.dir
	dir.deltaLogDir = filepath.Join(dir.insertLogDir, "file1", "delta") + "/"
	task := newRESTImportTask(local.args, local.job, "p1", dirGroup{}, []partitionDir{dir}, []importSecondary{{addr: "mirror"}})
	assert.ErrorContains(t, task.Execute(t.Context()), "copy delta log dir")
	matches, err := filepath.Glob(stagedGlob)
	require.NoError(t, err)
	assert.Empty(t, matches)
}

func TestTwoPCMissingMirrorRecovery(t *testing.T) {
	for _, tc := range []struct{ name, state string }{
		{"MissingStatus", "missing"},
		{"MissingDuringGC", "Failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var events []string
				var mu sync.Mutex
				primary := newTwoPCCluster("primary", &events, &mu, "Uncommitted")
				secondary := newTwoPCCluster("secondary", &events, &mu, tc.state)
				secondary.reason = "import job does not exist, jobID=job1"
				task := newTwoPCTask(t, true, primary, secondary)
				start := time.Now()
				require.ErrorIs(t, task.Execute(t.Context()), milvus.ErrImportJobNotFound)
				assert.GreaterOrEqual(t, time.Since(start), _secondaryMirrorTimeout)
				assert.Equal(t, []string{"primary:create"}, events)
				id, done := task.importer.job.Current()
				assert.Equal(t, "job1", id)
				assert.False(t, done)
				primary.states = []string{"Uncommitted", "Completed"}
				primary.calls = 0
				secondary.states = []string{"Completed"}
				require.NoError(t, task.Execute(t.Context()))
				assert.Equal(t, []string{"primary:create", "primary:commit"}, events)
			})
		})
	}
}

func TestTwoPCMirrorLookupRetainsPrimary(t *testing.T) {
	for _, tc := range []struct {
		name, state, reason string
		commitRequested     bool
	}{
		{"EmptyState", "", "", false},
		{"OtherMissingID", "Failed", "import job does not exist, jobID=job2", false},
		{"MissingAfterCommitRequested", "missing", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var events []string
				var mu sync.Mutex
				primary := newTwoPCCluster("primary", &events, &mu, "Uncommitted")
				secondary := newTwoPCCluster("secondary", &events, &mu, tc.state)
				secondary.reason = tc.reason
				task := newTwoPCTask(t, true, primary, secondary)
				task.importer.job.Bind("job1")
				if tc.commitRequested {
					task.importer.job.MarkCommitRequested()
				}
				require.Error(t, task.Execute(t.Context()))
				assert.Empty(t, events)
				id, _ := task.importer.job.Current()
				assert.Equal(t, "job1", id)
			})
		})
	}
}

func TestTwoPCMirrorProgressResetsMissingWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var events []string
		var mu sync.Mutex
		primary := newTwoPCCluster("primary", &events, &mu, "Uncommitted", "Completed")
		// Each stretch is shorter than the window, but together they exceed it.
		var states []string
		for _, progress := range []string{"Pending", "Importing", "Uncommitted"} {
			for range int(_secondaryMirrorTimeout / _bulkInsertCheckInterval) {
				states = append(states, "missing")
			}
			states = append(states, progress)
		}
		secondary := newTwoPCCluster("secondary", &events, &mu, append(states, "Completed")...)
		require.NoError(t, newTwoPCTask(t, true, primary, secondary).Execute(t.Context()))
		assert.Equal(t, []string{"primary:create", "primary:commit"}, events)
	})
}

func TestTwoPCMirrorMissingAfterCommit(t *testing.T) {
	var events []string
	var mu sync.Mutex
	primary := newTwoPCCluster("primary", &events, &mu, "Completed")
	secondary := newTwoPCCluster("secondary", &events, &mu, "Failed")
	secondary.reason = "import job does not exist, jobID=job1"
	task := newTwoPCTask(t, true, primary, secondary)
	task.importer.job.Bind("job1")

	err := task.Execute(t.Context())
	require.ErrorIs(t, err, milvus.ErrImportJobNotFound)
	assert.NotContains(t, err.Error(), "diverge")
	id, done := task.importer.job.Current()
	assert.Equal(t, "job1", id)
	assert.False(t, done)
	assert.Empty(t, events)
}

func TestTwoPCResumeKeepsCompletedPrimary(t *testing.T) {
	var events []string
	var mu sync.Mutex
	primary := newTwoPCCluster("primary", &events, &mu, "Completed", "missing")
	mirror := newTwoPCCluster("mirror", &events, &mu, "Completed")
	task := newTwoPCTask(t, true, primary, mirror)
	task.importer.job.Bind("job1")

	require.NoError(t, task.Execute(t.Context()))
	assert.Equal(t, 1, primary.calls)
	assert.Equal(t, 1, mirror.calls)
	assert.Empty(t, events)
	_, done := task.importer.job.Current()
	assert.True(t, done)
}

func TestTwoPCResumeOnlyPollsUnconfirmedMirrors(t *testing.T) {
	var events []string
	var mu sync.Mutex
	primary := newTwoPCCluster("primary", &events, &mu, "Completed", "missing")
	first := newTwoPCCluster("first", &events, &mu, "Completed", "missing")
	last := newTwoPCCluster("last", &events, &mu, "missing")
	task := newTwoPCTask(t, true, primary, first, last)
	task.importer.job.Bind("job1")
	require.ErrorIs(t, task.Execute(t.Context()), milvus.ErrImportJobNotFound)
	_, done := task.importer.job.Current()
	assert.False(t, done)

	last.states = append(last.states, "Completed")
	require.NoError(t, task.Execute(t.Context()))
	assert.Equal(t, 1, primary.calls)
	assert.Equal(t, 1, first.calls)
	assert.Equal(t, 2, last.calls)
	assert.Empty(t, events)
}

func TestTwoPCMirrorCommitProtectsPrimaryID(t *testing.T) {
	for _, mirrorState := range []string{"Committing", "Completed"} {
		t.Run(mirrorState, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var events []string
				var mu sync.Mutex
				primary := newTwoPCCluster("primary", &events, &mu, "Uncommitted", "missing")
				first := newTwoPCCluster("first", &events, &mu, mirrorState)
				last := newTwoPCCluster("last", &events, &mu, "missing")
				task := newTwoPCTask(t, true, primary, first, last)
				task.importer.job.Bind("job1")

				start := time.Now()
				require.ErrorIs(t, task.Execute(t.Context()), milvus.ErrImportJobNotFound)
				assert.GreaterOrEqual(t, time.Since(start), _secondaryMirrorTimeout)
				require.Empty(t, events, "a commit observed on the first mirror must prevent abort")
				assert.Equal(t, mirrorState == "Completed", task.importer.job.MirrorCompleted(first.name))

				require.ErrorIs(t, task.Execute(t.Context()), milvus.ErrImportJobNotFound)
				id, done := task.importer.job.Current()
				assert.Equal(t, "job1", id)
				assert.False(t, done)
				assert.Equal(t, 2, primary.calls)
				assert.Empty(t, events, "a missing primary must not trigger a replacement")
			})
		})
	}
}

func TestTwoPCResumeWaitFailure(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		wantErr     error
		wantAborts  int
	}{
		{"Failed", "Failed", milvus.ErrRemoteJobFailed, 1},
		{"Missing", "missing", milvus.ErrImportJobNotFound, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var log []string
				var mu sync.Mutex
				local := newTwoPCCluster("primary", &log, &mu, "Importing", tc.state)
				secondary := newTwoPCCluster("secondary", &log, &mu, "Completed")
				task := newTwoPCTask(t, true, local, secondary)
				task.importer.job.Bind("old")

				assert.ErrorIs(t, task.Execute(t.Context()), tc.wantErr)
				id, done := task.importer.job.Current()
				assert.Equal(t, "old", id)
				assert.False(t, done)
				assert.Zero(t, local.count("create"))
				assert.Zero(t, local.count("commit"))
				assert.Equal(t, tc.wantAborts, local.count("abort"))

				local.states, local.calls = []string{tc.state, "Uncommitted", "Completed"}, 0
				if tc.state == "missing" {
					require.ErrorIs(t, task.Execute(t.Context()), tc.wantErr)
					id, done = task.importer.job.Current()
					assert.Equal(t, "old", id)
					assert.False(t, done)
					assert.Empty(t, log)
					return
				}
				require.NoError(t, task.Execute(t.Context()))
				id, done = task.importer.job.Current()
				assert.Equal(t, "job1", id)
				assert.True(t, done)
				assert.Equal(t, 1, local.count("create"))
				assert.Equal(t, 1, local.count("commit"))
				assert.Equal(t, 2*tc.wantAborts, local.count("abort"))
			})
		})
	}
}

// Cancel the caller after Milvus accepts the import but before submit returns.
type cancelOnSubmitClient struct {
	milvus.Restful
	cancel context.CancelFunc
}

func (c cancelOnSubmitClient) BulkInsert(ctx context.Context, in milvus.BulkInsertV2Input) (string, error) {
	id, err := c.Restful.BulkInsert(ctx, in)
	c.cancel()
	return id, err
}

func TestTwoPCSubmissionCancellationRetainsID(t *testing.T) {
	for _, tc := range []struct {
		name         string
		enableResume bool
		wantAborts   int
	}{
		{name: "ResumeEnabled", enableResume: true},
		{name: "ResumeDisabled", wantAborts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("primary", &log, &mu, "Completed")
			task := newTwoPCTask(t, tc.enableResume, local)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			task.importer.args.restfulCli = cancelOnSubmitClient{Restful: local, cancel: cancel}

			assert.ErrorIs(t, task.Execute(ctx), context.Canceled)
			id, done := task.importer.job.Current()
			assert.Equal(t, "job1", id)
			assert.False(t, done)
			assert.Equal(t, 1, local.count("create"))
			assert.Zero(t, local.count("commit"))
			assert.Equal(t, tc.wantAborts, local.count("abort"))

			if tc.enableResume {
				require.NoError(t, task.Execute(t.Context()))
				id, done = task.importer.job.Current()
				assert.Equal(t, "job1", id)
				assert.True(t, done)
				assert.Equal(t, 1, local.count("create"))
			}
		})
	}
}

func TestTwoPCResumeAbortFailureRetainsID(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"RemoteFailure", milvus.ErrRemoteJobFailed},
		{"MissingID", milvus.ErrImportJobNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("primary", &log, &mu, "Failed")
			local.abortErr = tc.err
			task := newTwoPCTask(t, true, local)
			task.importer.job.Bind("old")

			assert.ErrorIs(t, task.Execute(t.Context()), tc.err)
			id, done := task.importer.job.Current()
			assert.Equal(t, "old", id)
			assert.False(t, done)
			assert.Equal(t, []string{"primary:abort"}, log)
		})
	}
}

func TestTwoPCResumeReplacementFailsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var log []string
		var mu sync.Mutex
		local := newTwoPCCluster("primary", &log, &mu, "Failed", "Importing", "Failed")
		task := newTwoPCTask(t, true, local)
		task.importer.job.Bind("old")

		assert.ErrorIs(t, task.Execute(t.Context()), milvus.ErrRemoteJobFailed)
		id, done := task.importer.job.Current()
		assert.Equal(t, "job1", id)
		assert.False(t, done)
		assert.Equal(t, []string{"primary:abort", "primary:create", "primary:abort"}, log)
	})
}

func TestTwoPCResumeWaitingAfterCommitRetainsID(t *testing.T) {
	for _, tc := range []struct {
		phase           string
		commitRequested bool
	}{
		{"Committing", false},
		{"Importing", true},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("primary", &log, &mu, tc.phase, "Failed")
			mirror := newTwoPCCluster("mirror", &log, &mu, "Completed")
			task := newTwoPCTask(t, true, local, mirror)
			task.importer.job.Bind("old")
			if tc.commitRequested {
				task.importer.job.MarkCommitRequested()
			}

			assert.ErrorIs(t, task.Execute(t.Context()), milvus.ErrRemoteJobFailed)
			assert.True(t, task.importer.job.CommitRequested())
			assert.ErrorIs(t, task.Execute(t.Context()), milvus.ErrRemoteJobFailed)
			id, done := task.importer.job.Current()
			assert.Equal(t, "old", id)
			assert.False(t, done)
			assert.Empty(t, log)
		})
	}
}

func TestRESTTwoPhaseMirrorPolling(t *testing.T) {
	for _, tc := range []struct {
		name         string
		enableResume bool
		states       []string
		wantDuration time.Duration
		wantError    string
		wantCommits  int
	}{
		{"UnknownThenReady", false, []string{"FutureState", "Uncommitted", "Completed"}, _bulkInsertCheckInterval, "", 1},
		{"ResumeRejectsUnknown", true, []string{"FutureState"}, 0, "unknown state", 0},
		{"MissingThenCompleted", false, []string{"Uncommitted", "missing", "Completed"}, _bulkInsertCheckInterval, "", 1},
		{"MissingUntilCommitTimeout", false, []string{"Uncommitted", "missing"}, _secondaryCommitTimeout, "after the local commit", 1},
		{"ResumeRejectsMissingAfterCommit", true, []string{"Uncommitted", "missing"}, 0, "job1", 1},
		{"FailedWithGCReasonAfterCommit", false, []string{"Uncommitted", "Failed"}, 0, "job1", 1},
	} {
		for _, format := range []string{"Binlog", "Snapshot"} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var events []string
					var mu sync.Mutex
					primary := newTwoPCCluster("primary", &events, &mu, "Uncommitted", "Completed")
					mirror := newTwoPCCluster("mirror", &events, &mu, tc.states...)
					mirror.reason = "import job does not exist, jobID=job1"
					var execute func(context.Context) error
					if format == "Snapshot" {
						task := newSnapshot2PCTask(t, primary, mirror)
						owner, err := jobstate.NewStore().AddRestoreTask("task", tc.enableResume)
						require.NoError(t, err)
						task.importer.jobs = owner.NewCollTracker(task.importer.target, 1)
						task.importer.job = task.importer.jobs.NewJob(1)
						execute = task.Execute
					} else {
						execute = newTwoPCTask(t, tc.enableResume, primary, mirror).Execute
					}
					start := time.Now()
					err := execute(t.Context())
					if tc.wantError == "" {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, tc.wantError)
					}
					assert.Equal(t, tc.wantDuration, time.Since(start))
					assert.Equal(t, 1, primary.count("create"))
					assert.Equal(t, tc.wantCommits, primary.count("commit"))
					assert.Zero(t, primary.count("abort"))
				})
			})
		}
	}
}
