package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func newTestCollDMLTask() *collDMLTask {
	return &collDMLTask{logger: zap.NewNop(), option: &Option{}, maxSegsPerImportJob: 256}
}

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

func TestCollTask_ezk(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollDMLTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "hello"}
		assert.Equal(t, "hello", ct.ezk())
	})

	t.Run("WithoutEZK", func(t *testing.T) {
		ct := newTestCollDMLTask()
		assert.Equal(t, "", ct.ezk())
	})

	t.Run("WithMapping", func(t *testing.T) {
		ct := newTestCollDMLTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "old_key"}
		ct.option.EZKMapping = map[string]string{"old_key": "new_key"}
		assert.Equal(t, "new_key", ct.ezk())
	})

	t.Run("WithMappingNoMatch", func(t *testing.T) {
		ct := newTestCollDMLTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "other_key"}
		ct.option.EZKMapping = map[string]string{"old_key": "new_key"}
		assert.Equal(t, "other_key", ct.ezk())
	})

	t.Run("WithMappingEmptyEZK", func(t *testing.T) {
		ct := newTestCollDMLTask()
		ct.option.EZKMapping = map[string]string{"old_key": "new_key"}
		assert.Equal(t, "", ct.ezk())
	})
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
	ct := newTestCollDMLTask()
	ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
	ct.backupDir = "backup/"

	groups, err := ct.l0DirGroups(segs10())
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
	t.Run("WithGroupID", func(t *testing.T) {
		ct := newTestCollDMLTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
		ct.backupDir = "backup/"
		ct.backupStorage = newTestStorageClient(t, v2.ProviderLocal)

		part := &backuppb.PartitionBackupInfo{PartitionId: 1, PartitionName: "p1", Size: 10}
		for _, seg := range segs10() {
			seg.GroupId = 7
			part.SegmentBackups = append(part.SegmentBackups, seg)
		}

		groups, err := ct.notL0DirGroups(context.Background(), part)
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
		ct := newTestCollDMLTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
		ct.backupDir = "backup/"
		ct.backupStorage = newTestStorageClient(t, v2.ProviderLocal)

		// an old backup without group ids: the whole partition is one dir
		// group with default request parameters
		part := &backuppb.PartitionBackupInfo{PartitionId: 1, PartitionName: "p1", Size: 10}
		part.SegmentBackups = segs10()

		groups, err := ct.notL0DirGroups(context.Background(), part)
		require.NoError(t, err)

		require.Len(t, groups, 1)
		assert.False(t, groups[0].isL0)
		assert.Equal(t, uint64(0), groups[0].timestamp)
		assert.Equal(t, int64(0), groups[0].storageVersion)
		require.Len(t, groups[0].dirs, 1)
		assert.NotEmpty(t, groups[0].dirs[0].insertLogDir)
	})

	t.Run("WithoutGroupIDTruncateByTsRejected", func(t *testing.T) {
		ct := newTestCollDMLTask()
		ct.option.TruncateBinlogByTs = true

		part := &backuppb.PartitionBackupInfo{PartitionId: 1}
		part.SegmentBackups = segs10()

		_, err := ct.notL0DirGroups(context.Background(), part)
		assert.ErrorContains(t, err, "truncate binlog by ts is not supported")
	})
}
