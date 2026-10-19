package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// newTestStorageClient builds a storage client of the given provider without
// touching any backend: constructing a client never connects.
func newTestStorageClient(t *testing.T, provider string) storage.Client {
	cli, err := storage.NewClient(context.Background(), storage.Config{
		Provider:   provider,
		Endpoint:   "localhost:9000",
		Credential: storage.Credential{Type: storage.Static, AK: "a", SK: "b"},
	})
	require.NoError(t, err)
	return cli
}

// segs10 spreads 10 segments over 2 (vchannel, storage version) pairs: the
// even ids on (vch0, 0) and the odd ids on (vch1, 1).
func segs10() []*backuppb.SegmentBackupInfo {
	segs := make([]*backuppb.SegmentBackupInfo, 0, 10)
	for i := range 10 {
		seg := &backuppb.SegmentBackupInfo{
			SegmentId:      int64(i),
			PartitionId:    1,
			VChannel:       fmt.Sprintf("vch%d", i%2),
			Size:           1,
			StorageVersion: int64(i % 2),
		}
		segs = append(segs, seg)
	}
	return segs
}

func TestL0DirGroups(t *testing.T) {
	src := newBinlogSource(&backuppb.CollectionBackupInfo{CollectionId: 1}, &Option{}, "backup/", nil, zap.NewNop())

	groups, err := src.l0DirGroups(segs10())
	require.NoError(t, err)

	// one group per (vchannel, storage version) pair, 5 dirs each
	require.Len(t, groups, 2)
	for _, g := range groups {
		assert.True(t, g.isL0)
		assert.Len(t, g.dirs, 5)
		for _, dir := range g.dirs {
			assert.Empty(t, dir.insertLogDir)
			assert.NotEmpty(t, dir.deltaLogDir)
		}
	}
}

func TestNotL0DirGroups(t *testing.T) {
	newSource := func(t *testing.T, option *Option) *binlogSource {
		return newBinlogSource(
			&backuppb.CollectionBackupInfo{CollectionId: 1},
			option,
			"backup/",
			newTestStorageClient(t, cfg.ProviderLocal),
			zap.NewNop())
	}

	t.Run("WithGroupID", func(t *testing.T) {
		src := newSource(t, &Option{})

		part := &backuppb.PartitionBackupInfo{PartitionId: 1, PartitionName: "p1", Size: 10}
		for _, seg := range segs10() {
			seg.GroupId = 7
			part.SegmentBackups = append(part.SegmentBackups, seg)
		}

		groups, err := src.notL0DirGroups(context.Background(), part)
		require.NoError(t, err)

		// one group per (vchannel, storage version) pair, 5 dirs each; no
		// delta logs exist in the empty backup dir, so each dir carries the
		// insert log dir only
		require.Len(t, groups, 2)
		for _, g := range groups {
			assert.False(t, g.isL0)
			assert.Len(t, g.dirs, 5)
			for _, dir := range g.dirs {
				assert.NotEmpty(t, dir.insertLogDir)
				assert.Empty(t, dir.deltaLogDir)
			}
		}
	})

	t.Run("WithoutGroupID", func(t *testing.T) {
		src := newSource(t, &Option{})

		// an old backup without group ids: the whole partition is one dir
		// group with default request parameters
		part := &backuppb.PartitionBackupInfo{PartitionId: 1, PartitionName: "p1", Size: 10}
		part.SegmentBackups = segs10()

		groups, err := src.notL0DirGroups(context.Background(), part)
		require.NoError(t, err)

		require.Len(t, groups, 1)
		assert.False(t, groups[0].isL0)
		assert.Equal(t, uint64(0), groups[0].timestamp)
		assert.Equal(t, int64(0), groups[0].storageVersion)
		require.Len(t, groups[0].dirs, 1)
		assert.NotEmpty(t, groups[0].dirs[0].insertLogDir)
	})

	t.Run("WithoutGroupIDTruncateByTsRejected", func(t *testing.T) {
		src := newSource(t, &Option{TruncateBinlogByTs: true})

		part := &backuppb.PartitionBackupInfo{PartitionId: 1}
		part.SegmentBackups = segs10()

		_, err := src.notL0DirGroups(context.Background(), part)
		assert.ErrorContains(t, err, "truncate binlog by ts is not supported")
	})
}
