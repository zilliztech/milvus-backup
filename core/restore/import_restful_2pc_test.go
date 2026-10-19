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
// across the two ends — and that the peer ends never get a lifecycle call at
// all, since a replication target refuses client-side commit/abort.
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
func newTwoPCTask(t *testing.T, local *twoPCCluster, peers ...*twoPCCluster) *importViaRESTFul2PCTask {
	target := collref.New("db", "coll")
	store := jobstate.NewStore()
	store.AddRestoreTask("task1")
	store.UpdateRestoreTask("task1", jobstate.AddRestoreCollTask(target, 1))

	importer := &restfulImporter{
		args: importTaskArgs{
			taskID:        "task1",
			target:        target,
			milvusStorage: newTestStorageClient(t, cfg.ProviderMinio),
			restfulCli:    local,
			store:         store,
			logger:        zap.NewNop(),
		},

		partitionName: "p1",
		dirs:          []partitionDir{{insertLogDir: "insert", size: 1}},
	}
	importPeers := make([]importPeer, 0, len(peers))
	for _, p := range peers {
		importPeers = append(importPeers, importPeer{addr: p.name, cli: p})
	}
	return &importViaRESTFul2PCTask{importer: importer, peers: importPeers}
}

func TestImportViaRESTFul2PCTaskExecute(t *testing.T) {
	// The happy path commits on the local cluster only; the peer mirrors are
	// committed by the replicated fence message, so the task just waits them
	// out and never issues a lifecycle call against a peer.
	t.Run("CommitsLocalThenWaitsPeer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateImporting),
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer := newTwoPCCluster("peer", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCommitting),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, local, peer)
			require.NoError(t, task.Execute(t.Context()))

			assert.True(t, local.manualCommit)
			assert.Equal(t, []string{"local:create", "local:commit"}, log)
			assert.Zero(t, local.count("abort"))
			assert.Zero(t, peer.count("commit"))
			assert.Zero(t, peer.count("abort"))
		})
	})

	// Past the local commit there is no way back: a mirror that fails on its
	// own does not take the replicated commit, so the error says the two ends
	// have diverged — and nothing aborts, the local data stays.
	t.Run("PeerMirrorFailureDiverges", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer := newTwoPCCluster("peer", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateFailed))

			task := newTwoPCTask(t, local, peer)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "disk quota exceeded")
			assert.Contains(t, err.Error(), "already committed")
			assert.Contains(t, err.Error(), "diverge")

			assert.Equal(t, []string{"local:create", "local:commit"}, log)
			assert.Zero(t, local.count("abort"))
			assert.Zero(t, peer.count("abort"))
		})
	})

	// A failure before the commit aborts the local job; the abort's rollback
	// message replicates and fails the mirrors, so the peers are never called.
	t.Run("LocalFailureAbortsLocally", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu, string(milvus.ImportStateFailed))
			peer := newTwoPCCluster("peer", &log, &mu, string(milvus.ImportStateUncommitted))

			task := newTwoPCTask(t, local, peer)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "disk quota exceeded")

			assert.Equal(t, []string{"local:create", "local:abort"}, log)
			assert.Zero(t, peer.count("commit"))
			assert.Zero(t, peer.count("abort"))
		})
	})

	// The mirror is created around the submit, so a peer that still has no trace
	// of the job a minute later means the peer is wrong, not slow.
	t.Run("PeerMirrorTimeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu, string(milvus.ImportStateUncommitted))
			peer := newTwoPCCluster("peer", &log, &mu, "")

			task := newTwoPCTask(t, local, peer)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "--peers")

			assert.Equal(t, []string{"local:create", "local:abort"}, log)
		})
	})

	// A mirror that takes a few polls to show up is waited for.
	t.Run("PeerMirrorAppearsLate", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer := newTwoPCCluster("peer", &log, &mu,
				"", "",
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, local, peer)
			require.NoError(t, task.Execute(t.Context()))
			assert.Equal(t, []string{"local:create", "local:commit"}, log)
		})
	})

	// A retried task can meet a peer mirror that is already committing: the
	// crashed first run's commit replicated before the retry got there.
	t.Run("PeerAlreadyCommitting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer := newTwoPCCluster("peer", &log, &mu,
				string(milvus.ImportStateCommitting),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, local, peer)
			require.NoError(t, task.Execute(t.Context()))
			assert.Zero(t, peer.count("commit"))
		})
	})

	// Every peer must show the mirror before the local commit, and every
	// peer's mirror is waited out after it.
	t.Run("TwoPeers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer1 := newTwoPCCluster("peer1", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer2 := newTwoPCCluster("peer2", &log, &mu, "",
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))

			task := newTwoPCTask(t, local, peer1, peer2)
			require.NoError(t, task.Execute(t.Context()))
			assert.Equal(t, []string{"local:create", "local:commit"}, log)
			assert.Zero(t, peer1.count("commit"))
			assert.Zero(t, peer2.count("commit"))
		})
	})

	// A mirror that never finishes after the local commit means the fence did
	// not land; the timeout names the peer and points at the stream.
	t.Run("PeerCommitTimeoutNamesIt", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer1 := newTwoPCCluster("peer1", &log, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			peer2 := newTwoPCCluster("peer2", &log, &mu, string(milvus.ImportStateUncommitted))

			task := newTwoPCTask(t, local, peer1, peer2)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "peer2")
			assert.Contains(t, err.Error(), "replication stream")
		})
	})
}

func TestRESTfulImportPlannerPeerFork(t *testing.T) {
	planner := &restfulImportPlanner{
		args:            importTaskArgs{maxSegsPerImportJob: 2, logger: zap.NewNop()},
		multiL0InOneJob: true,
	}
	g := dirGroup{dirs: []partitionDir{{insertLogDir: "insert", size: 1}}}

	// Without a peer every task is the plain single-phase shape.
	task := planner.newTask("p1", g, g.dirs)
	_, ok := task.(*importViaRESTFulTask)
	assert.True(t, ok)

	// With peers the same import rides the two-phase task, driving the same
	// importer device.
	planner.peers = []importPeer{{
		addr: "peer",
		cli:  newTwoPCCluster("peer", &[]string{}, &sync.Mutex{}, string(milvus.ImportStateUncommitted)),
	}}
	task = planner.newTask("p1", g, g.dirs)
	twoPC, ok := task.(*importViaRESTFul2PCTask)
	require.True(t, ok)
	assert.Equal(t, planner.peers, twoPC.peers)
	assert.NotNil(t, twoPC.importer)
}
