package meta

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

func TestWriteReadRoundTrip(t *testing.T) {
	ctx := context.Background()
	cli := &storage.LocalClient{}
	backupDir := filepath.Join(t.TempDir(), "mybackup")

	info := &backuppb.BackupInfo{
		Id: "bk-1", Name: "mybackup",
		CollectionBackups: []*backuppb.CollectionBackupInfo{{
			CollectionId: 1,
			PartitionBackups: []*backuppb.PartitionBackupInfo{{
				CollectionId: 1, PartitionId: 10,
				SegmentBackups: []*backuppb.SegmentBackupInfo{
					{SegmentId: 200, CollectionId: 1, PartitionId: 10, Size: 5},
				},
			}},
		}},
	}
	require.NoError(t, Write(ctx, cli, backupDir, info))

	got, err := Read(ctx, cli, backupDir)
	require.NoError(t, err)
	require.Len(t, got.GetCollectionBackups(), 1)
	require.Len(t, got.GetCollectionBackups()[0].GetPartitionBackups()[0].GetSegmentBackups(), 1)

	// remove full_meta -> Read must reassemble from leveled files
	require.NoError(t, cli.DeleteObject(ctx, mpath.MetaKey(backupDir, mpath.FullMeta)))
	got2, err := Read(ctx, cli, backupDir)
	require.NoError(t, err)
	require.Len(t, got2.GetCollectionBackups()[0].GetPartitionBackups()[0].GetSegmentBackups(), 1)
}
