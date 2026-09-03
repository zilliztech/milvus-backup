package mpath

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// milvus bin log path (${root} is the milvus storage root)
// insert log
// ${root}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_id}
// delta log
// ${root}/delta_log/${collection_id}/${partition_id}/${segment_id}/${log_id}
// An L0 delta log applies to every partition, so its partition_id is -1.

// backup bin log path (${root} is the backup directory)
// insert log
// ${root}/binlogs/insert_log/${collection_id}/${partition_id}/${group_id}(optional)/${segment_id}/${field_id}/${log_id}
// level 1 delta log
// ${root}/binlogs/delta_log/${collection_id}/${partition_id}/${group_id}(optional)/${segment_id}/${log_id}
// level 0 delta log
// ${root}/binlogs/delta_log/${collection_id}/${partition_id}/${segment_id}/${log_id}
// The group ID is a virtual partition ID. The Milvus BulkInsert interface requires a partition prefix,
// but passing multiple segments is a more suitable option.
// Therefore, a virtual partition ID is used here to enable the functionality of importing multiple segments.
// It is the segment's own ID, is absent on L0 delta logs, and is missing entirely
// in backups written before it was introduced — restore falls back to the layout
// without it.

// backup meta path
// ${root}/meta/${file_type}.json

// snapshot bundle path (snapshot-format backup; written and read by Milvus
// ExportSnapshot/RestoreExternalSnapshot, this package only anchors its root at
// ${root}/bundle, see BundleDirName)
// Milvus namespaces every export under an exports/${export_uuid} layer of its
// own, so exports sharing a target never collide, and a retried or recovered
// export reuses the namespace it first persisted.
// snapshot metadata: protojson SnapshotMetadata, the bundle's publication marker.
// The backup meta records this path relative to the backup directory.
// ${root}/bundle/exports/${export_uuid}/snapshots/${collection_id}/metadata/${snapshot_id}.json
// segment manifests: one avro file per segment
// ${root}/bundle/exports/${export_uuid}/snapshots/${collection_id}/manifests/${snapshot_id}/${segment_id}.avro
// data files: each copied under its path relative to the milvus storage root
// (files/insert_log/..., files/delta_log/..., files/stats_log/..., index files, ...)
// ${root}/bundle/exports/${export_uuid}/files/${source_relative_path}
// An export interrupted mid-copy can also leave
// ${root}/bundle/exports/${export_uuid}/_staging/metadata.json,
// the pre-publication metadata copy the exporter removes once the bundle is committed.

const _separator = "/"

const _metaPrefix = "meta"

type MetaType string

const (
	BackupMeta     MetaType = "backup_meta.json"
	CollectionMeta MetaType = "collection_meta.json"
	PartitionMeta  MetaType = "partition_meta.json"
	SegmentMeta    MetaType = "segment_meta.json"
	FullMeta       MetaType = "full_meta.json"
)

func MetaKey(backupDir string, mateType MetaType) string {
	return path.Join(backupDir, _metaPrefix, string(mateType))
}

func MetaDir(backupDir string) string {
	return path.Join(backupDir, _metaPrefix) + _separator
}

const (
	_binlogPrefix    = "binlogs"
	_insertLogPrefix = "insert_log"
	_deltaLogPrefix  = "delta_log"
)

// BundleDirName is where a snapshot-format backup keeps the bundle Milvus exports:
// its metadata, its manifests and the data files they reference, under one prefix
// beside the meta this tool writes.
const BundleDirName = "bundle"

// BackupBundleDir returns the directory an exported snapshot bundle is written to.
func BackupBundleDir(backupDir string) string { return path.Join(backupDir, BundleDirName) }

func Join(base string, options ...Option) string {
	elem := make([]string, 0, 8)
	elem = append(elem, base)

	var o opt
	for _, option := range options {
		option(&o)
	}
	elem = append(elem, o.elem()...)

	return path.Join(elem...)
}

func MilvusRootDir(root string) string {
	if root == "" {
		return ""
	}

	return path.Clean(root) + _separator
}

// MilvusInsertLogDir returns the directory for insert logs.
func MilvusInsertLogDir(base string, opts ...Option) string {
	return Join(path.Join(base, _insertLogPrefix), opts...) + _separator
}

// MilvusDeltaLogDir returns the directory for insert logs.
func MilvusDeltaLogDir(base string, opts ...Option) string {
	return Join(path.Join(base, _deltaLogPrefix), opts...) + _separator
}

// BackupInsertLogDir returns the directory for insert logs.
func BackupInsertLogDir(base string, opts ...Option) string {
	return Join(path.Join(base, _binlogPrefix, _insertLogPrefix), opts...) + _separator
}

// BackupDeltaLogDir returns the directory for insert logs.
func BackupDeltaLogDir(base string, opts ...Option) string {
	return Join(path.Join(base, _binlogPrefix, _deltaLogPrefix), opts...) + _separator
}

// BackupBinlogDir returns the root directory holding all of a backup's binlogs
// (both insert_log and delta_log subtrees).
func BackupBinlogDir(base string, opts ...Option) string {
	return Join(path.Join(base, _binlogPrefix), opts...) + _separator
}

func BackupDir(backupRoot string, backupName string) string {
	return path.Join(backupRoot, backupName) + _separator
}

func BackupRootDir(backupRoot string) string {
	if backupRoot == "" {
		return ""
	}

	return path.Clean(backupRoot) + _separator
}

type opt struct {
	collectionID    int64
	setCollectionID bool

	partitionID    int64
	setPartitionID bool

	groupID    int64
	setGroupID bool

	segmentID    int64
	setSegmentID bool

	fieldID    int64
	setFieldID bool

	logID    int64
	setLogID bool
}

func (opt *opt) elem() []string {
	var elem []string
	if opt.setCollectionID {
		elem = append(elem, strconv.FormatInt(opt.collectionID, 10))
	}
	if opt.setPartitionID {
		elem = append(elem, strconv.FormatInt(opt.partitionID, 10))
	}
	if opt.setGroupID {
		elem = append(elem, strconv.FormatInt(opt.groupID, 10))
	}
	if opt.setSegmentID {
		elem = append(elem, strconv.FormatInt(opt.segmentID, 10))
	}
	if opt.setFieldID {
		elem = append(elem, strconv.FormatInt(opt.fieldID, 10))
	}
	if opt.setLogID {
		elem = append(elem, strconv.FormatInt(opt.logID, 10))
	}

	return elem
}

type Option func(*opt)

func CollectionID(collectionID int64) Option {
	return func(opt *opt) {
		opt.collectionID = collectionID
		opt.setCollectionID = true
	}
}

func PartitionID(partitionID int64) Option {
	return func(opt *opt) {
		opt.partitionID = partitionID
		opt.setPartitionID = true
	}
}

func GroupID(groupID int64) Option {
	return func(opt *opt) {
		opt.groupID = groupID
		opt.setGroupID = true
	}
}

func SegmentID(segmentID int64) Option {
	return func(opt *opt) {
		opt.segmentID = segmentID
		opt.setSegmentID = true
	}
}

func FieldID(fieldID int64) Option {
	return func(opt *opt) {
		opt.fieldID = fieldID
		opt.setFieldID = true
	}
}

func LogID(logID int64) Option {
	return func(opt *opt) {
		opt.logID = logID
		opt.setLogID = true
	}
}

type binlogPath struct {
	Root string

	CollectionID int64
	PartitionID  int64
	SegmentID    int64
}

type InsertLogPath struct {
	Root string

	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	FieldID      int64
	LogID        int64
}

type DeltaLogPath struct {
	Root string

	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	LogID        int64
}

var (
	_insertLogPathRegex = regexp.MustCompile(`^(?:(.+)/)?insert_log/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)$`)
	_deltaLogPathRegex  = regexp.MustCompile(`^(?:(.+)/)?delta_log/(\d+)/(-?\d+)/(\d+)/(\d+)$`)
)

func parseBinlogPath(reg *regexp.Regexp, p string) (binlogPath, []string, error) {
	if strings.HasSuffix(p, "/") {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s should not end with /", p)
	}

	matches := reg.FindStringSubmatch(p)
	if len(matches) == 0 {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s does not match the pattern", p)
	}

	root := matches[1]

	collectionID, err := strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s collectionID %s is not a number", p, matches[2])
	}
	partitionID, err := strconv.ParseInt(matches[3], 10, 64)
	if err != nil {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s partitionID %s is not a number", p, matches[3])
	}
	segmentID, err := strconv.ParseInt(matches[4], 10, 64)
	if err != nil {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s segmentID %s is not a number", p, matches[4])
	}

	return binlogPath{
		Root:         root,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		SegmentID:    segmentID,
	}, matches[5:], nil
}

func ParseInsertLogPath(p string) (InsertLogPath, error) {
	bp, matches, err := parseBinlogPath(_insertLogPathRegex, p)
	if err != nil {
		return InsertLogPath{}, err
	}

	if len(matches) != 2 {
		return InsertLogPath{}, fmt.Errorf("mpath: log path %s does not match the pattern", p)
	}

	fieldID, err := strconv.ParseInt(matches[0], 10, 64)
	if err != nil {
		return InsertLogPath{}, fmt.Errorf("mpath: log path %s fieldID %s is not a number", p, matches[0])
	}
	logID, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return InsertLogPath{}, fmt.Errorf("mpath: log path %s logID %s is not a number", p, matches[1])
	}

	return InsertLogPath{
		Root: bp.Root,

		CollectionID: bp.CollectionID,
		PartitionID:  bp.PartitionID,
		SegmentID:    bp.SegmentID,
		FieldID:      fieldID,
		LogID:        logID,
	}, nil
}

func ParseDeltaLogPath(p string) (DeltaLogPath, error) {
	bp, matches, err := parseBinlogPath(_deltaLogPathRegex, p)
	if err != nil {
		return DeltaLogPath{}, err
	}

	if len(matches) != 1 {
		return DeltaLogPath{}, fmt.Errorf("mpath: log path %s does not match the pattern", p)
	}
	logID, err := strconv.ParseInt(matches[0], 10, 64)
	if err != nil {
		return DeltaLogPath{}, fmt.Errorf("mpath: log path %s logID %s is not a number", p, matches[0])
	}

	return DeltaLogPath{
		Root: bp.Root,

		CollectionID: bp.CollectionID,
		PartitionID:  bp.PartitionID,
		SegmentID:    bp.SegmentID,
		LogID:        logID,
	}, nil
}
