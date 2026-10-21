package restore

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

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

	manualCommit bool
}

func newTwoPCCluster(name string, log *[]string, logMu *sync.Mutex, states ...string) *twoPCCluster {
	return &twoPCCluster{name: name, log: log, logMu: logMu, states: states}
}

func (c *twoPCCluster) record(op string) {
	c.logMu.Lock()
	defer c.logMu.Unlock()
	*c.log = append(*c.log, c.name+":"+op)
}

func (c *twoPCCluster) BulkInsert(_ context.Context, in milvus.BulkInsertV2Input) (string, error) {
	c.manualCommit = in.ManualCommit
	c.record("create")
	return "job1", nil
}

func (c *twoPCCluster) GetBulkInsertState(context.Context, string, string) (*milvus.GetProcessResp, error) {
	i := min(c.calls, len(c.states)-1)
	c.calls++

	resp := &milvus.GetProcessResp{}
	resp.Data.State = c.states[i]
	if c.states[i] == string(milvus.ImportStateFailed) {
		resp.Data.Reason = "disk quota exceeded"
	}
	return resp, nil
}

func (c *twoPCCluster) GetSegmentInfo(context.Context, string, int64, int64) (*milvus.SegmentInfo, error) {
	return nil, nil
}

func (c *twoPCCluster) CommitImport(context.Context, string, string) error {
	c.record("commit")
	return nil
}

func (c *twoPCCluster) AbortImport(context.Context, string, string) error {
	c.record("abort")
	return nil
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
func newTwoPCTask(t *testing.T, local *twoPCCluster, secondaries ...*twoPCCluster) *importViaRESTFul2PCTask {
	target := collref.New("db", "coll")
	collTracker := jobstate.NewStore().AddRestoreTask("task1").NewCollTracker(target, 1)

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

		job: collTracker.NewJob(1),
	}
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

			task := newTwoPCTask(t, local, secondary)
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

			task := newTwoPCTask(t, local, secondary)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "disk quota exceeded")
			assert.Contains(t, err.Error(), "already committed")
			assert.Contains(t, err.Error(), "diverge")

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

			task := newTwoPCTask(t, local, secondary)
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
			secondary := newTwoPCCluster("secondary", &log, &mu, "")

			task := newTwoPCTask(t, local, secondary)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "--secondaries")

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

			task := newTwoPCTask(t, local, secondary)
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

			task := newTwoPCTask(t, local, secondary)
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
			secondary2 := newTwoPCCluster("secondary2", &log, &mu, "",
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, local, secondary1, secondary2)
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

			task := newTwoPCTask(t, local, secondary1, secondary2)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "secondary2")
			assert.Contains(t, err.Error(), "replication stream")
		})
	})
}

func TestRESTfulImportPlannerSecondaryFork(t *testing.T) {
	jobs := jobstate.NewStore().AddRestoreTask("task-1").NewCollTracker(collref.New("db", "coll"), 0)
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
