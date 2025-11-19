package mpath

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// milvus bin log path
// insert log
// ${root}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_idx}
// delta log
// ${root}/delta_log/${collection_id}/${partition_id}/${segment_id}/${log_idx}

// backup bin log path
// insert log
// ${root}/binglogs/insert_log/${collection_id}/${partition_id}/${groupId}(optional)/${segment_id}/${field_id}/${log_idx}
// level 1 delta log
// ${root}/binglogs/delta_log/${collection_id}/${partition_id}/${groupId}(optional)/${segment_id}/${log_idx}
// level 0 delta log
// ${root}/binglogs/delta_log/${collection_id}/${partition_id}/${segment_id}/${log_idx}
// The group ID is a virtual partition ID. The Milvus BulkInsert interface requires a partition prefix,
// but passing multiple segments is a more suitable option.
// Therefore, a virtual partition ID is used here to enable the functionality of importing multiple segments.

// backup meta path
// ${root}/meta/${file_type}.json

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

func Join(base string, options ...Option) string {
	elem := []string{base}

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
	if strings.HasSuffix("/", p) {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s should not end with /", p)
	}

	matches := reg.FindStringSubmatch(p)
	if len(matches) == 0 {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s is not match the pattern", p)
	}

	root := matches[1]

	collectionID, err := strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s collectionID %s is not a number", p, matches[3])
	}
	partitionID, err := strconv.ParseInt(matches[3], 10, 64)
	if err != nil {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s partitionID %s is not a number", p, matches[4])
	}
	segmentID, err := strconv.ParseInt(matches[4], 10, 64)
	if err != nil {
		return binlogPath{}, nil, fmt.Errorf("mpath: log path %s segmentID %s is not a number", p, matches[5])
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
		return InsertLogPath{}, fmt.Errorf("mpath: log path %s is not match the pattern", p)
	}

	fieldID, err := strconv.ParseInt(matches[0], 10, 64)
	if err != nil {
		return InsertLogPath{}, fmt.Errorf("mpath: log path %s fieldID %s is not a number", p, matches[6])
	}
	logID, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return InsertLogPath{}, fmt.Errorf("mpath: log path %s logIdx %s is not a number", p, matches[7])
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
		return DeltaLogPath{}, fmt.Errorf("mpath: log path %s is not match the pattern", p)
	}
	logID, err := strconv.ParseInt(matches[0], 10, 64)
	if err != nil {
		return DeltaLogPath{}, fmt.Errorf("mpath: log path %s logIdx %s is not a number", p, matches[6])
	}

	return DeltaLogPath{
		Root: bp.Root,

		CollectionID: bp.CollectionID,
		PartitionID:  bp.PartitionID,
		SegmentID:    bp.SegmentID,
		LogID:        logID,
	}, nil
}
