package breakpoint

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenFreshAndPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bp.json")

	tr, err := Open(path, "rid-1", "backup-a")
	require.NoError(t, err)
	assert.Equal(t, "rid-1", tr.RestoreID())

	// issue + complete one job
	require.NoError(t, tr.MarkInflight("job-1", "db.coll", []int64{10, 11, 12}))
	require.NoError(t, tr.Complete("job-1"))

	// reopen: completed must survive, restoreID from file wins over the seed
	tr2, err := Open(path, "rid-IGNORED", "backup-a")
	require.NoError(t, err)
	assert.Equal(t, "rid-1", tr2.RestoreID(), "restoreID persisted in ledger must win over the seed")
	for _, seg := range []int64{10, 11, 12} {
		assert.True(t, tr2.IsCompleted("db.coll", seg), "seg %d should be completed after reopen", seg)
	}
	assert.False(t, tr2.IsCompleted("db.coll", 99))
	assert.False(t, tr2.IsCompleted("db.other", 10), "completion is per-namespace")
	assert.Empty(t, tr2.Inflight(), "completed job must not remain inflight")
}

func TestFailDropsInflightWithoutCompleting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bp.json")
	tr, err := Open(path, "rid", "b")
	require.NoError(t, err)

	require.NoError(t, tr.MarkInflight("job-x", "db.coll", []int64{1, 2}))
	assert.Len(t, tr.Inflight(), 1)

	require.NoError(t, tr.Fail("job-x"))
	assert.Empty(t, tr.Inflight())
	// a failed job's segments must NOT be marked completed (they re-enter to-do)
	assert.False(t, tr.IsCompleted("db.coll", 1))
	assert.False(t, tr.IsCompleted("db.coll", 2))
}

func TestInflightSurvivesCrashForReconciliation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bp.json")
	tr, err := Open(path, "rid", "b")
	require.NoError(t, err)

	// simulate: job issued, then the tool "crashes" (no Complete/Fail)
	require.NoError(t, tr.MarkInflight("job-crash", "db.coll", []int64{7, 8}))

	// reopen sees the inflight record so the caller can reconcile via Milvus
	tr2, err := Open(path, "rid", "b")
	require.NoError(t, err)
	infl := tr2.Inflight()
	require.Len(t, infl, 1)
	assert.ElementsMatch(t, []int64{7, 8}, infl["job-crash"].Segs)
	assert.Equal(t, "db.coll", infl["job-crash"].NS)

	// reconciling it as completed promotes the segments and clears inflight
	require.NoError(t, tr2.Complete("job-crash"))
	assert.True(t, tr2.IsCompleted("db.coll", 7))
	assert.Empty(t, tr2.Inflight())
}

func TestCompleteIsIdempotentAndDedups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bp.json")
	tr, err := Open(path, "rid", "b")
	require.NoError(t, err)

	require.NoError(t, tr.MarkInflight("j1", "db.coll", []int64{1, 2}))
	require.NoError(t, tr.Complete("j1"))
	// overlapping segment set in a later job must not duplicate entries
	require.NoError(t, tr.MarkInflight("j2", "db.coll", []int64{2, 3}))
	require.NoError(t, tr.Complete("j2"))

	tr2, err := Open(path, "rid", "b")
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 3}, tr2.completedIdxKeys("db.coll"))

	// Complete on an unknown job is a no-op, not an error
	require.NoError(t, tr2.Complete("does-not-exist"))
}

// completedIdxKeys is a test helper exposing the completed set for a namespace.
func (t *Tracker) completedIdxKeys(ns string) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]int64, 0)
	for s := range t.completedIdx[ns] {
		out = append(out, s)
	}
	return out
}
