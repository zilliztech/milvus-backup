package l0compact

import (
	"context"
	"fmt"
	"path"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/l0compact"
	"github.com/zilliztech/milvus-backup/internal/log"
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
	force  bool  // clear a non-empty destination instead of erroring
	nextID int64 // deltalog log id allocator
}

// Option configures a Task.
type Option func(*Task)

// WithForce clears a non-empty destination before writing instead of erroring.
func WithForce(force bool) Option { return func(t *Task) { t.force = force } }

// NewTask builds an l0compact task reading from srcDir and writing to dstDir.
func NewTask(cli storage.Client, srcDir, dstDir string, opts ...Option) *Task {
	t := &Task{cli: cli, srcDir: srcDir, dstDir: dstDir, nextID: 1}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Execute reads the source meta, folds every collection's L0 segments into its
// data-segment deltalogs, drops the L0 segments, and writes the new meta.
func (t *Task) Execute(ctx context.Context) error {
	info, err := meta.Read(ctx, t.cli, t.srcDir)
	if err != nil {
		return fmt.Errorf("l0compact: read src meta: %w", err)
	}
	// Don't stack onto a non-empty destination: a leftover half-backup (e.g. from
	// a previously failed run) would silently mix with this run's objects. With
	// --force, clear it first so the run (or a re-run) starts clean.
	keys, _, err := storage.ListPrefixFlat(ctx, t.cli, t.dstDir, true)
	if err != nil {
		return fmt.Errorf("l0compact: list dst dir: %w", err)
	}
	if len(keys) > 0 {
		if !t.force {
			return fmt.Errorf("l0compact: destination %s is not empty (%d objects); choose a fresh output or use --force", t.dstDir, len(keys))
		}
		if err := storage.DeletePrefix(ctx, t.cli, t.dstDir); err != nil {
			return fmt.Errorf("l0compact: clear destination %s: %w", t.dstDir, err)
		}
	}
	// New fold deltalogs share the deltaKey namespace with the copied source
	// deltalogs (which keep their original LogIds). Allocate above the highest
	// existing LogId so a new fold deltalog can never overwrite a copied one.
	t.nextID = maxLogID(info) + 1

	// The washed backup must be a complete, restorable backup: restore
	// reconstructs file paths from dstDir, so every data object (insert_log and
	// the original delta_log) must physically exist there. Copy the whole
	// binlogs/ subtree first; folding then appends new deltalogs to dstDir and
	// dropL0 removes the (now redundant) L0 segments from the meta.
	if err := t.run(ctx, info); err != nil {
		// A mid-pipeline failure leaves a meta-less, half-copied backup in dstDir.
		// Best-effort remove it so the destination is clean for a re-run.
		if cerr := storage.DeletePrefix(ctx, t.cli, t.dstDir); cerr != nil {
			log.Warn("l0compact: cleanup dst dir after failure", zap.String("dst", t.dstDir), zap.Error(cerr))
		}
		return err
	}
	return nil
}

// run copies binlogs, folds L0 into per-segment deltalogs, drops L0 from the
// meta, and writes the destination meta. On any error the caller cleans dstDir.
func (t *Task) run(ctx context.Context, info *backuppb.BackupInfo) error {
	if err := t.copyBinlogs(ctx); err != nil {
		return err
	}
	for _, coll := range info.GetCollectionBackups() {
		if err := t.foldCollection(ctx, coll); err != nil {
			return err
		}
	}
	dropL0(info)
	// The destination is a distinct backup: give it the output name and a fresh
	// id so `list` doesn't show two backups with the source's identity.
	info.Name = path.Base(t.dstDir)
	info.Id = uuid.NewString()
	if err := meta.Write(ctx, t.cli, t.dstDir, info); err != nil {
		return fmt.Errorf("l0compact: write dst meta: %w", err)
	}
	return nil
}

// maxLogID returns the highest LogId across every segment's insert binlogs and
// deltalogs (including collection-level L0 segments) in the backup meta.
func maxLogID(info *backuppb.BackupInfo) int64 {
	var max int64
	scan := func(seg *backuppb.SegmentBackupInfo) {
		for _, f := range seg.GetBinlogs() {
			for _, bl := range f.GetBinlogs() {
				if bl.GetLogId() > max {
					max = bl.GetLogId()
				}
			}
		}
		for _, f := range seg.GetDeltalogs() {
			for _, bl := range f.GetBinlogs() {
				if bl.GetLogId() > max {
					max = bl.GetLogId()
				}
			}
		}
	}
	for _, coll := range info.GetCollectionBackups() {
		for _, seg := range coll.GetL0Segments() {
			scan(seg)
		}
		for _, part := range coll.GetPartitionBackups() {
			for _, seg := range part.GetSegmentBackups() {
				scan(seg)
			}
		}
	}
	return max
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
				blob, err := storage.Read(ctx, t.cli, deltaKey(t.srcDir, seg, bl.GetLogId()))
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
	pks, err := t.readSegmentPKs(ctx, seg, pkFieldID, kind, pkType)
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
	// The deltalog is always written in the v1 legacy envelope format regardless
	// of the segment's storage version: Milvus's bulk-insert/import reads a
	// segment's deltalogs with the v1 binlog reader (magic 0xfffabc) even on the
	// v2 restore path, so a bare v2 parquet deltalog fails "parse magic number".
	// (Insert data is still read per the segment version; only deltalogs are v1.)
	blob, err := l0compact.WriteDeltalog(hits, l0compact.KindV1, pkType)
	if err != nil {
		return err
	}
	logID := t.nextID
	t.nextID++
	key := deltaKey(t.dstDir, seg, logID)
	if err := storage.Write(ctx, t.cli, key, blob); err != nil {
		return err
	}
	// Unlike copied entries (whose LogPath is the original Milvus source path),
	// this newly folded deltalog only exists inside the backup, so LogPath holds
	// its backup object key. Restore reconstructs delta paths from the backup dir
	// and never reads LogPath, so this is inert for restore.
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

// insertKey / deltaKey reconstruct a backup object key from backupDir + segment
// identity. The stored Binlog.LogPath is the ORIGINAL milvus source path, not
// the backup's own key, so reads must reconstruct (mirroring backup's
// insertLogsAttrs/deltaLogAttrs and restore).
func insertKey(backupDir string, seg *backuppb.SegmentBackupInfo, fieldID, logID int64) string {
	dir := mpath.BackupInsertLogDir(backupDir,
		mpath.CollectionID(seg.GetCollectionId()), mpath.PartitionID(seg.GetPartitionId()),
		mpath.SegmentID(seg.GetSegmentId()), mpath.GroupID(seg.GetGroupId()))
	return mpath.Join(dir, mpath.FieldID(fieldID), mpath.LogID(logID))
}

func deltaKey(backupDir string, seg *backuppb.SegmentBackupInfo, logID int64) string {
	opts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionId()), mpath.PartitionID(seg.GetPartitionId()),
		mpath.SegmentID(seg.GetSegmentId()),
	}
	if seg.GetPartitionId() != allPartitionID {
		opts = append(opts, mpath.GroupID(seg.GetGroupId()))
	}
	return mpath.Join(mpath.BackupDeltaLogDir(backupDir, opts...), mpath.LogID(logID))
}

// readSegmentPKs reads a data segment's primary-key column.
//   - v1: read the FieldBinlog whose FieldID == pkFieldID (PK is column 0).
//   - v2: read column-group parquets ONE file at a time and stop at the first
//     group that carries the PK column. This keeps peak memory to a single group
//     file instead of buffering every group (incl. large vector columns) at once.
func (t *Task) readSegmentPKs(ctx context.Context, seg *backuppb.SegmentBackupInfo, pkFieldID int64, kind l0compact.StorageKind, pkType l0compact.PKType) ([]l0compact.PrimaryKey, error) {
	if kind == l0compact.KindV1 {
		var blobs [][]byte
		for _, f := range seg.GetBinlogs() {
			if f.GetFieldID() != pkFieldID {
				continue
			}
			for _, bl := range f.GetBinlogs() {
				b, err := storage.Read(ctx, t.cli, insertKey(t.srcDir, seg, f.GetFieldID(), bl.GetLogId()))
				if err != nil {
					return nil, err
				}
				blobs = append(blobs, b)
			}
		}
		return l0compact.ReadInsertPK(blobs, kind, pkFieldID, pkType)
	}
	// v2: probe one group file at a time; the PK column lives in exactly one
	// column group, so once a group is found to carry it, read the rest of that
	// group's files and stop (never touch the remaining groups).
	var out []l0compact.PrimaryKey
	for _, fb := range seg.GetBinlogs() {
		groupHasPK := false
		for _, bl := range fb.GetBinlogs() {
			blob, err := storage.Read(ctx, t.cli, insertKey(t.srcDir, seg, fb.GetFieldID(), bl.GetLogId()))
			if err != nil {
				return nil, err
			}
			pks, ok, err := l0compact.ReadParquetColumnByFieldID(blob, pkFieldID, pkType)
			if err != nil {
				return nil, err
			}
			if !ok {
				break // this group lacks the PK column; move to the next group
			}
			groupHasPK = true
			out = append(out, pks...)
		}
		if groupHasPK {
			return out, nil
		}
	}
	return nil, fmt.Errorf("l0compact: PK field %d not found in any insert group parquet", pkFieldID)
}
