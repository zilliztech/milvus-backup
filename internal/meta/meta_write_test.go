package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// TestWriteReadRoundTrip verifies Write persists the whole tree and Read
// reconstructs it (via the full-meta path and, after removing full_meta, via the
// leveled files).
func TestWriteReadRoundTrip(t *testing.T) {
	ctx := context.Background()
	cli, err := storage.NewClient(ctx, storage.Config{Provider: cfg.Local})
	require.NoError(t, err)

	backupDir := t.TempDir() + "/mybackup"

	info := &backuppb.BackupInfo{
		Id:   "bk-1",
		Name: "mybackup",
		CollectionBackups: []*backuppb.CollectionBackupInfo{{
			CollectionId: 1,
			PartitionBackups: []*backuppb.PartitionBackupInfo{{
				CollectionId: 1,
				PartitionId:  10,
				SegmentBackups: []*backuppb.SegmentBackupInfo{
					{SegmentId: 200, CollectionId: 1, PartitionId: 10, Size: 5},
					{SegmentId: 201, CollectionId: 1, PartitionId: 10, Size: 7},
				},
			}},
		}},
	}

	require.NoError(t, Write(ctx, cli, backupDir, info))

	// Read (full-meta path).
	got, err := Read(ctx, cli, backupDir)
	require.NoError(t, err)
	require.Len(t, got.GetCollectionBackups(), 1)
	parts := got.GetCollectionBackups()[0].GetPartitionBackups()
	require.Len(t, parts, 1)
	segs := parts[0].GetSegmentBackups()
	require.Len(t, segs, 2)
	assert.Equal(t, int64(200), segs[0].GetSegmentId())
	assert.Equal(t, int64(201), segs[1].GetSegmentId())

	// Remove full_meta so Read must reassemble from the leveled files.
	require.NoError(t, cli.DeleteObject(ctx, mpath.MetaKey(backupDir, mpath.FullMeta)))
	got2, err := Read(ctx, cli, backupDir)
	require.NoError(t, err)
	require.Len(t, got2.GetCollectionBackups(), 1)
	segs2 := got2.GetCollectionBackups()[0].GetPartitionBackups()[0].GetSegmentBackups()
	require.Len(t, segs2, 2, "leveled reassembly must re-nest segments into partitions")
	assert.Equal(t, int64(201), segs2[1].GetSegmentId())
}
