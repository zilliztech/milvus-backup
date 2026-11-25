// Package restoreplan describes the inputs for replaying a prepared restore.
package restoreplan

import (
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/snapshotsource"
)

type SkipParams struct {
	CollectionProperties []string

	FieldIndexParams []string
	FieldTypeParams  []string

	IndexParams []string
}

// CollOverride contains per-collection overrides for restore.
type CollOverride struct {
	ShardNum    int32
	Description string
}

// SecondaryEndpoint identifies a mirror. An empty token uses the primary credentials.
type SecondaryEndpoint struct {
	Address string
	Token   string
}

// Option is shared by the prepared tasks. Callers must treat it as read-only
// after submitting a restore, including while the job can still be resumed.
type Option struct {
	// Index related options
	DropExistIndex bool
	RebuildIndex   bool
	UseAutoIndex   bool

	// Collection schema related options
	DropExistCollection  bool
	SkipCreateCollection bool
	MaxShardNum          int32
	SkipParams           SkipParams

	// data related options
	MetaOnly bool
	// Strategy pins the data path the restore takes. The zero value auto
	// resolves it against the backup's format and the target's capabilities.
	Strategy           Strategy
	TruncateBinlogByTs bool

	// Secondaries are the REST endpoints of the secondary clusters that
	// mirror the target's import jobs over the replication stream. When set,
	// every import job is committed two-phase: held at Uncommitted until every
	// secondary's mirror is there too, then committed on the target and all
	// secondaries. Empty for a plain restore.
	Secondaries []SecondaryEndpoint

	// SnapshotSourceKey picks where a snapshot import reads the bundle from.
	// The zero value (user-facing "shared") sends the complete uri of one
	// bucket; SourceKeyRelative (user-facing "own_bucket") sends a
	// bucket-relative object key that the target and every secondary resolve
	// against their own bucket, which needs the bundle staged in each one
	// first. It only applies to snapshot format backups, and only on the
	// import path.
	SnapshotSourceKey snapshotsource.SourceKey

	RestoreRBAC bool

	// EZKMapping maps old encryption keys to new ones.
	// During restore, the old EZK from backup is looked up in this map
	// and replaced with the new EZK if a match is found.
	EZKMapping map[string]string

	EnableResume bool
}

// Strategy pins the data path a restore takes. Every value names both the
// backup format it applies to and the path it selects, so no value is
// ambiguous across formats: per_seg and merged_seg are binlog strategies,
// restore_snap and import_snap are snapshot ones. What carries the data in
// the first place is the backup's format, the other axis the task resolves
// at start.
//
//go:generate stringer -type=Strategy
type Strategy int

const (
	StrategyAuto Strategy = iota
	StrategyPerSeg
	StrategyMergedSeg
	StrategyRestoreSnap
	StrategyImportSnap
)

// Plan is published once, after preflight. Collection and import order is immutable.
type Plan struct {
	Secondary      *Secondary
	Milvus         cfg.MilvusConfig
	Backup         cfg.BackupConfig
	Restore        cfg.RestoreConfig
	Transfer       cfg.TransferConfig
	BackupDir      string
	Option         Option
	SnapshotSource snapshotsource.Source
	Streaming      bool
	Strategy       Strategy
	Collections    []Collection
}

type Collection struct {
	HasData  bool // Retain the field ID verification requirement after segment metadata is omitted.
	Target   collref.Name
	Metadata *backuppb.CollectionBackupInfo
	EZK      string
	Override CollOverride
}

type BinlogImportBatch struct {
	PartitionName  string // Empty only for collection-global L0 in a primary restore.
	L0             bool
	Timestamp      uint64
	StorageVersion int64
	Dirs           []Directory
}

type Directory struct {
	Insert, Delta string
	Size          int64
}
