package mpath

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FuzzParseLogPath throws arbitrary object storage keys at both parsers. Backup
// and restore parse every key they list under a prefix, and those keys are
// outside this tool's control — an old layout, another writer, or a corrupted
// backup all surface here. The invariant is that no key panics a parser and the
// two layouts stay distinguishable: at most one of them accepts a key.
func FuzzParseLogPath(f *testing.F) {
	f.Add("base/insert_log/1/2/3/4/5")
	f.Add("insert_log/1/2/3/4/5")
	f.Add("base/delta_log/1/-1/2/3")
	f.Add("delta_log/1/2/3/4")
	// int64 overflow, the regression #1197 fixed
	f.Add("base/insert_log/99999999999999999999/2/3/4/5")
	f.Add("base/insert_log/1/2/3/4/5/")
	f.Add("base/insert_log/1/2/3/4")
	f.Add("")

	f.Fuzz(func(t *testing.T, key string) {
		_, insertErr := ParseInsertLogPath(key)
		_, deltaErr := ParseDeltaLogPath(key)
		if insertErr == nil {
			assert.Error(t, deltaErr, "key %q is accepted by both parsers", key)
		}
		if deltaErr == nil {
			assert.Error(t, insertErr, "key %q is accepted by both parsers", key)
		}
	})
}

// FuzzInsertLogPathRoundTrip pins the parser against the dir builder: every path
// MilvusInsertLogDir writes for non-negative ids must parse back to exactly
// those ids. Negative ids cannot appear in a milvus path (the regex admits only
// digits), but the builder still formats them, so they must be rejected.
func FuzzInsertLogPathRoundTrip(f *testing.F) {
	f.Add(int64(1), int64(2), int64(3), int64(4), int64(5))
	f.Add(int64(0), int64(0), int64(0), int64(0), int64(0))
	f.Add(int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64))
	f.Add(int64(-1), int64(2), int64(3), int64(4), int64(5))

	f.Fuzz(func(t *testing.T, col, part, seg, field, log int64) {
		dir := MilvusInsertLogDir("base", CollectionID(col), PartitionID(part), SegmentID(seg), FieldID(field), LogID(log))
		key := strings.TrimSuffix(dir, "/")
		got, err := ParseInsertLogPath(key)
		if col < 0 || part < 0 || seg < 0 || field < 0 || log < 0 {
			require.Error(t, err, "key %q", key)
			return
		}
		require.NoError(t, err, "key %q", key)
		assert.Equal(t, InsertLogPath{Root: "base", CollectionID: col, PartitionID: part, SegmentID: seg, FieldID: field, LogID: log}, got, "key %q", key)
	})
}

// FuzzDeltaLogPathRoundTrip is the delta twin. The partition id is the one field
// allowed to be negative — an L0 delta log applies to every partition and is
// stored under partition id -1 — so only collection, segment and log ids must
// reject negatives.
func FuzzDeltaLogPathRoundTrip(f *testing.F) {
	f.Add(int64(1), int64(2), int64(3), int64(4))
	f.Add(int64(1), int64(-1), int64(2), int64(3))
	f.Add(int64(0), int64(math.MinInt64), int64(0), int64(0))
	f.Add(int64(1), int64(2), int64(-3), int64(4))

	f.Fuzz(func(t *testing.T, col, part, seg, log int64) {
		dir := MilvusDeltaLogDir("base", CollectionID(col), PartitionID(part), SegmentID(seg), LogID(log))
		key := strings.TrimSuffix(dir, "/")
		got, err := ParseDeltaLogPath(key)
		if col < 0 || seg < 0 || log < 0 {
			require.Error(t, err, "key %q", key)
			return
		}
		require.NoError(t, err, "key %q", key)
		assert.Equal(t, DeltaLogPath{Root: "base", CollectionID: col, PartitionID: part, SegmentID: seg, LogID: log}, got, "key %q", key)
	})
}
