package meta

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestWriteSnapshotFormat(t *testing.T) {
	ctx := context.Background()
	cli := &storage.LocalClient{}
	backupDir := filepath.Join(t.TempDir(), "mybackup")

	info := &backuppb.BackupInfo{
		Id: "bk-1", Name: "mybackup", Format: FormatSnapshot,
		CollectionBackups: []*backuppb.CollectionBackupInfo{{
			CollectionId: 1, CollectionName: "coll",
			SnapshotBackup: &backuppb.SnapshotBackupInfo{
				MetadataPath: "snapshots/1/metadata/2.json",
				TotalFiles:   3,
				TotalBytes:   4096,
			},
		}},
	}
	require.NoError(t, Write(ctx, cli, backupDir, info))

	t.Run("StopsAfterCollectionMeta", func(t *testing.T) {
		for _, metaType := range []mpath.MetaType{mpath.FullMeta, mpath.BackupMeta, mpath.CollectionMeta} {
			exist, err := storage.Exist(ctx, cli, mpath.MetaKey(backupDir, metaType))
			require.NoError(t, err)
			assert.True(t, exist, metaType)
		}

		// The partition and segment files are asserted absent rather than left
		// unchecked. They would hold empty lists, and writing them would give this
		// backup a second read path through levelToTree, which derives a collection's
		// size by summing segments and would report zero for this format.
		for _, metaType := range []mpath.MetaType{mpath.PartitionMeta, mpath.SegmentMeta} {
			exist, err := storage.Exist(ctx, cli, mpath.MetaKey(backupDir, metaType))
			require.NoError(t, err)
			assert.False(t, exist, metaType)
		}
	})

	// backup_meta.json is what marks a directory as holding a backup, in either format.
	t.Run("Exist", func(t *testing.T) {
		exist, err := Exist(ctx, cli, backupDir)
		require.NoError(t, err)
		assert.True(t, exist)
	})

	// The top-level view carries the format too, so a reader that only fetches
	// backup_meta.json still knows what it is looking at.
	t.Run("BackupMetaCarriesTheFormat", func(t *testing.T) {
		byts, err := storage.Read(ctx, cli, mpath.MetaKey(backupDir, mpath.BackupMeta))
		require.NoError(t, err)

		var got backuppb.BackupInfo
		require.NoError(t, json.Unmarshal(byts, &got))
		assert.Equal(t, FormatSnapshot, got.GetFormat())
		assert.Empty(t, got.GetCollectionBackups())
	})

	t.Run("RoundTrip", func(t *testing.T) {
		got, err := Read(ctx, cli, backupDir)
		require.NoError(t, err)
		assert.Equal(t, FormatSnapshot, got.GetFormat())
		require.Len(t, got.GetCollectionBackups(), 1)

		snapshot := got.GetCollectionBackups()[0].GetSnapshotBackup()
		require.NotNil(t, snapshot)
		assert.Equal(t, "snapshots/1/metadata/2.json", snapshot.GetMetadataPath())
		assert.EqualValues(t, 3, snapshot.GetTotalFiles())
		assert.EqualValues(t, 4096, snapshot.GetTotalBytes())
	})

	// format is a string so that the stored file says what it is without the .proto
	// to decode it — this meta is json, and an enum would land here as a bare integer.
	t.Run("FormatIsReadableInTheStoredJSON", func(t *testing.T) {
		byts, err := storage.Read(ctx, cli, mpath.MetaKey(backupDir, mpath.FullMeta))
		require.NoError(t, err)
		assert.Contains(t, string(byts), `"format":"snapshot"`)
	})
}
