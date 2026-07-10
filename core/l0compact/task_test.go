package l0compact

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/l0compact"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

func TestExecuteFoldsAndDropsL0(t *testing.T) {
	ctx := context.Background()
	cli := &storage.LocalClient{}
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	dstDir := filepath.Join(tmp, "dst")

	info := buildSyntheticV1Backup(ctx, t, cli, srcDir)
	require.NoError(t, meta.Write(ctx, cli, srcDir, info))

	task := NewTask(cli, srcDir, dstDir)
	require.NoError(t, task.Execute(ctx))

	out, err := meta.Read(ctx, cli, dstDir)
	require.NoError(t, err)

	// L0 gone: no collection-level L0Segments and no IsL0 partition segments.
	require.Nil(t, out.GetCollectionBackups()[0].GetL0Segments())
	for _, p := range out.GetCollectionBackups()[0].GetPartitionBackups() {
		for _, s := range p.GetSegmentBackups() {
			require.False(t, s.GetIsL0())
		}
	}

	// The washed backup must be complete: the source data segment's INSERT
	// binlog must physically exist under dstDir at the same relative path, so
	// restore can reconstruct import paths. This guards against forgetting to
	// copy the source data objects into the destination.
	dstInsertKey := mpath.Join(mpath.BackupInsertLogDir(dstDir,
		mpath.CollectionID(1), mpath.PartitionID(10), mpath.SegmentID(200), mpath.FieldID(0)),
		mpath.LogID(1))
	dstInsertBlob, err := storage.Read(ctx, cli, dstInsertKey)
	require.NoError(t, err)
	require.NotEmpty(t, dstInsertBlob)

	// The original L0 deltalog is also copied under dstDir (harmless after drop).
	dstL0DeltaKey := mpath.Join(mpath.BackupDeltaLogDir(dstDir,
		mpath.CollectionID(1), mpath.PartitionID(10), mpath.SegmentID(100)),
		mpath.LogID(1))
	_, err = storage.Read(ctx, cli, dstL0DeltaKey)
	require.NoError(t, err)

	// data segment 200 now has a deltalog; read it back and verify it deletes pk=2.
	seg := findSeg(out, 200)
	require.NotNil(t, seg)
	require.NotEmpty(t, seg.GetDeltalogs())
	blob, err := storage.Read(ctx, cli, seg.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
	require.NoError(t, err)
	entries, err := l0compact.ReadDeltalog(blob, l0compact.KindV2, l0compact.PKInt64)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, int64(2), entries[0].PK.Int)
	require.Equal(t, uint64(50), entries[0].Ts)
}

// buildSyntheticV1Backup writes insert + delta blobs to storage using only the
// exported codec funcs (v2-storage format, PK field id 0) and returns the meta
// tree pointing at them. Data segment 200 holds pks {1,2,3}; a partition-level
// L0 segment 100 deletes pk=2 at ts=50.
func buildSyntheticV1Backup(ctx context.Context, t *testing.T, cli storage.Client, srcDir string) *backuppb.BackupInfo {
	t.Helper()

	// Data segment insert blob: field_id 0 column carries the PK values {1,2,3}.
	insertBlob, err := l0compact.WriteParquetPKTs(
		[]l0compact.PrimaryKey{{Int: 1}, {Int: 2}, {Int: 3}},
		[]uint64{0, 0, 0}, l0compact.PKInt64)
	require.NoError(t, err)
	insertKey := mpath.Join(mpath.BackupInsertLogDir(srcDir,
		mpath.CollectionID(1), mpath.PartitionID(10), mpath.SegmentID(200), mpath.FieldID(0)),
		mpath.LogID(1))
	require.NoError(t, storage.Write(ctx, cli, insertKey, insertBlob))

	// L0 deltalog blob: deletes pk=2 at ts=50.
	deltaBlob, err := l0compact.WriteDeltalog(
		[]l0compact.DeleteEntry{{PK: l0compact.PrimaryKey{Type: l0compact.PKInt64, Int: 2}, Ts: 50}},
		l0compact.KindV2, l0compact.PKInt64)
	require.NoError(t, err)
	deltaKey := mpath.Join(mpath.BackupDeltaLogDir(srcDir,
		mpath.CollectionID(1), mpath.PartitionID(10), mpath.SegmentID(100)),
		mpath.LogID(1))
	require.NoError(t, storage.Write(ctx, cli, deltaKey, deltaBlob))

	dataSeg := &backuppb.SegmentBackupInfo{
		SegmentId:      200,
		CollectionId:   1,
		PartitionId:    10,
		VChannel:       "vch0",
		StorageVersion: 2,
		IsL0:           false,
		Binlogs: []*backuppb.FieldBinlog{{
			FieldID: 0,
			Binlogs: []*backuppb.Binlog{{LogId: 1, LogPath: insertKey, LogSize: int64(len(insertBlob))}},
		}},
	}
	l0Seg := &backuppb.SegmentBackupInfo{
		SegmentId:      100,
		CollectionId:   1,
		PartitionId:    10,
		VChannel:       "vch0",
		StorageVersion: 2,
		IsL0:           true,
		Deltalogs: []*backuppb.FieldBinlog{{
			FieldID: 0,
			Binlogs: []*backuppb.Binlog{{LogId: 1, LogPath: deltaKey, LogSize: int64(len(deltaBlob))}},
		}},
	}

	return &backuppb.BackupInfo{
		Id:   "bk-1",
		Name: "src",
		CollectionBackups: []*backuppb.CollectionBackupInfo{{
			CollectionId: 1,
			Schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{
					{FieldID: 0, Name: "pk", IsPrimaryKey: true, DataType: backuppb.DataType_Int64},
				},
			},
			PartitionBackups: []*backuppb.PartitionBackupInfo{{
				CollectionId:   1,
				PartitionId:    10,
				SegmentBackups: []*backuppb.SegmentBackupInfo{dataSeg, l0Seg},
			}},
		}},
	}
}

func findSeg(info *backuppb.BackupInfo, id int64) *backuppb.SegmentBackupInfo {
	for _, coll := range info.GetCollectionBackups() {
		for _, part := range coll.GetPartitionBackups() {
			for _, seg := range part.GetSegmentBackups() {
				if seg.GetSegmentId() == id {
					return seg
				}
			}
		}
	}
	return nil
}
