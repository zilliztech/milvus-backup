package l0compact

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/l0compact"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// _copyConcurrency bounds the parallelism when copying the source backup's
// binlog objects into the washed backup.
const _copyConcurrency = 10

// Task folds a backup's L0 (delete-only) segments into per-data-segment
// deltalogs by exact PK membership, then drops the L0 segments from the meta.
// The source backup is untouched; output goes to dstDir.
type Task struct {
	cli    storage.Client
	srcDir string
	dstDir string
	nextID int64 // deltalog log id allocator
}

// NewTask builds an l0compact task reading from srcDir and writing to dstDir.
func NewTask(cli storage.Client, srcDir, dstDir string) *Task {
	return &Task{cli: cli, srcDir: srcDir, dstDir: dstDir, nextID: 1}
}

// Execute reads the source meta, folds every collection's L0 segments into its
// data-segment deltalogs, drops the L0 segments, and writes the new meta.
func (t *Task) Execute(ctx context.Context) error {
	info, err := meta.Read(ctx, t.cli, t.srcDir)
	if err != nil {
		return fmt.Errorf("l0compact: read src meta: %w", err)
	}
	// The washed backup must be a complete, restorable backup: restore
	// reconstructs file paths from dstDir, so every data object (insert_log and
	// the original delta_log) must physically exist there. Copy the whole
	// binlogs/ subtree first; folding then appends new deltalogs to dstDir and
	// dropL0 removes the (now redundant) L0 segments from the meta.
	if err := t.copyBinlogs(ctx); err != nil {
		return err
	}
	for _, coll := range info.GetCollectionBackups() {
		if err := t.foldCollection(ctx, coll); err != nil {
			return err
		}
	}
	dropL0(info)
	if err := meta.Write(ctx, t.cli, t.dstDir, info); err != nil {
		return fmt.Errorf("l0compact: write dst meta: %w", err)
	}
	return nil
}

// copyBinlogs copies every object under the source backup's binlogs/ subtree to
// the same relative path under dstDir, preserving keys. It relies on the storage
// layer's server-side copy (dest.CopyObject) so large insert binlogs are not
// buffered in memory. The source backup is left untouched.
func (t *Task) copyBinlogs(ctx context.Context) error {
	copyTask := storage.NewCopyPrefixTask(storage.CopyPrefixOpt{
		Src:        t.cli,
		Dest:       t.cli,
		SrcPrefix:  mpath.BackupBinlogDir(t.srcDir),
		DestPrefix: mpath.BackupBinlogDir(t.dstDir),
		Sem:        semaphore.NewWeighted(_copyConcurrency),
	})
	if err := copyTask.Execute(ctx); err != nil {
		return fmt.Errorf("l0compact: copy src binlogs to dst: %w", err)
	}
	return nil
}

func (t *Task) foldCollection(ctx context.Context, coll *backuppb.CollectionBackupInfo) error {
	pkFieldID, pkType, err := pkInfo(coll)
	if err != nil {
		return err
	}
	// Gather L0 groups: channel-level (coll.L0Segments) fold into all partitions of
	// the same vchannel; partition-level (IsL0 in a partition) fold into that partition.
	for _, part := range coll.GetPartitionBackups() {
		targets := dataSegments(part) // non-L0 segments in this partition
		// channel-level L0 for this partition's vchannel(s) plus partition-level L0
		l0segs := append(channelL0For(coll, targets), partitionL0(part)...)
		if len(l0segs) == 0 || len(targets) == 0 {
			continue
		}
		delMap, err := t.buildDeleteMap(ctx, l0segs, pkType)
		if err != nil {
			return err
		}
		for _, seg := range targets {
			if err := t.foldIntoSegment(ctx, seg, delMap, pkFieldID, pkType); err != nil {
				return err
			}
		}
	}
	return nil
}

// buildDeleteMap reads all L0 deltalogs into pk -> ts, keeping the max delete ts
// per pk (delete-ts preservation, not last-write-wins on data).
func (t *Task) buildDeleteMap(ctx context.Context, l0segs []*backuppb.SegmentBackupInfo, pkType l0compact.PKType) (map[l0compact.PrimaryKey]uint64, error) {
	m := map[l0compact.PrimaryKey]uint64{}
	for _, seg := range l0segs {
		kind, err := l0compact.ClassifyVersion(seg.GetStorageVersion())
		if err != nil {
			return nil, err
		}
		for _, field := range seg.GetDeltalogs() {
			for _, bl := range field.GetBinlogs() {
				blob, err := storage.Read(ctx, t.cli, bl.GetLogPath())
				if err != nil {
					return nil, err
				}
				entries, err := l0compact.ReadDeltalog(blob, kind, pkType)
				if err != nil {
					return nil, err
				}
				for _, e := range entries {
					if cur, ok := m[e.PK]; !ok || e.Ts > cur {
						m[e.PK] = e.Ts
					}
				}
			}
		}
	}
	return m, nil
}

// foldIntoSegment reads a data segment's PK column, keeps deletes for the PKs it
// actually contains, writes a per-segment deltalog to dstDir, and records it.
func (t *Task) foldIntoSegment(ctx context.Context, seg *backuppb.SegmentBackupInfo, delMap map[l0compact.PrimaryKey]uint64, pkFieldID int64, pkType l0compact.PKType) error {
	kind, err := l0compact.ClassifyVersion(seg.GetStorageVersion())
	if err != nil {
		return err
	}
	blobs, err := t.pkBlobs(ctx, seg, pkFieldID, kind)
	if err != nil {
		return err
	}
	pks, err := l0compact.ReadInsertPK(blobs, kind, pkFieldID, pkType)
	if err != nil {
		return err
	}
	var hits []l0compact.DeleteEntry
	seen := map[l0compact.PrimaryKey]struct{}{}
	for _, pk := range pks {
		ts, ok := delMap[pk]
		if !ok {
			continue
		}
		if _, dup := seen[pk]; dup {
			continue
		}
		seen[pk] = struct{}{}
		hits = append(hits, l0compact.DeleteEntry{PK: pk, Ts: ts})
	}
	if len(hits) == 0 {
		return nil
	}
	blob, err := l0compact.WriteDeltalog(hits, kind, pkType)
	if err != nil {
		return err
	}
	logID := t.nextID
	t.nextID++
	opts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionId()),
		mpath.PartitionID(seg.GetPartitionId()),
		mpath.SegmentID(seg.GetSegmentId()),
	}
	if seg.GetGroupId() != 0 {
		opts = append(opts, mpath.GroupID(seg.GetGroupId()))
	}
	deltaDir := mpath.BackupDeltaLogDir(t.dstDir, opts...)
	key := mpath.Join(deltaDir, mpath.LogID(logID))
	if err := storage.Write(ctx, t.cli, key, blob); err != nil {
		return err
	}
	seg.Deltalogs = append(seg.GetDeltalogs(), &backuppb.FieldBinlog{
		FieldID: 0,
		Binlogs: []*backuppb.Binlog{{LogId: logID, LogPath: key, LogSize: int64(len(blob))}},
	})
	return nil
}

// pkInfo returns the primary-key field id and PKType from the collection schema.
func pkInfo(coll *backuppb.CollectionBackupInfo) (int64, l0compact.PKType, error) {
	for _, f := range coll.GetSchema().GetFields() {
		if f.GetIsPrimaryKey() {
			t, err := l0compact.PKTypeFromDataType(int32(f.GetDataType()))
			return f.GetFieldID(), t, err
		}
	}
	return 0, 0, fmt.Errorf("l0compact: collection %d has no primary key field", coll.GetCollectionId())
}

func dataSegments(part *backuppb.PartitionBackupInfo) []*backuppb.SegmentBackupInfo {
	var out []*backuppb.SegmentBackupInfo
	for _, s := range part.GetSegmentBackups() {
		if !s.GetIsL0() {
			out = append(out, s)
		}
	}
	return out
}

func partitionL0(part *backuppb.PartitionBackupInfo) []*backuppb.SegmentBackupInfo {
	var out []*backuppb.SegmentBackupInfo
	for _, s := range part.GetSegmentBackups() {
		if s.GetIsL0() {
			out = append(out, s)
		}
	}
	return out
}

// channelL0For returns collection-level (all-partition) L0 segments whose vchannel
// matches any target data segment's vchannel.
func channelL0For(coll *backuppb.CollectionBackupInfo, targets []*backuppb.SegmentBackupInfo) []*backuppb.SegmentBackupInfo {
	vch := map[string]struct{}{}
	for _, s := range targets {
		vch[s.GetVChannel()] = struct{}{}
	}
	var out []*backuppb.SegmentBackupInfo
	for _, s := range coll.GetL0Segments() {
		if _, ok := vch[s.GetVChannel()]; ok {
			out = append(out, s)
		}
	}
	return out
}

// pkBlobs returns the insert binlog blobs to read the PK column from.
//   - v1: the FieldBinlog whose FieldID == pkFieldID.
//   - v2: all group binlog blobs (ReadInsertPK scans for the field_id).
func (t *Task) pkBlobs(ctx context.Context, seg *backuppb.SegmentBackupInfo, pkFieldID int64, kind l0compact.StorageKind) ([][]byte, error) {
	var paths []string
	if kind == l0compact.KindV1 {
		for _, f := range seg.GetBinlogs() {
			if f.GetFieldID() == pkFieldID {
				for _, bl := range f.GetBinlogs() {
					paths = append(paths, bl.GetLogPath())
				}
			}
		}
	} else {
		for _, f := range seg.GetBinlogs() {
			for _, bl := range f.GetBinlogs() {
				paths = append(paths, bl.GetLogPath())
			}
		}
	}
	var blobs [][]byte
	for _, p := range paths {
		b, err := storage.Read(ctx, t.cli, p)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, b)
	}
	return blobs, nil
}
