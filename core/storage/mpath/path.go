package mpath

import (
	"path"
	"strconv"
)

const (
	_binlogPrefix    = "binlogs"
	_insertLogPrefix = "insert_log"
	_deltaLogPrefix  = "delta_log"
)

const Separator = "/"

// milvus bin log path
// insert log
// ${root}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_idx}
// delta log
// ${root}/delta_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_idx}

// backup bin log path
// insert log
// ${root}/binglogs/insert_log/${collection_id}/${partition_id}/${groupId}(optional)/${segment_id}/${field_id}/${log_idx}
// delta log
// ${root}/binglogs/delta_log/${collection_id}/${partition_id}/${groupId}(optional)/${segment_id}/${field_id}/${log_idx}
// The group ID is a virtual partition ID. The Milvus BulkInsert interface requires a partition prefix,
// but passing multiple segments is a more suitable option.
// Therefore, a virtual partition ID is used here to enable the functionality of importing multiple segments.

func LogDir(base, typePrefix string, opts ...PathOption) string {
	elem := []string{base, _binlogPrefix, typePrefix}

	opt := &PathOpt{}
	for _, o := range opts {
		o(opt)
	}

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

	return path.Join(elem...) + Separator
}

// InsertLogDir returns the directory for insert logs.
func InsertLogDir(base string, opts ...PathOption) string {
	return LogDir(base, _insertLogPrefix, opts...)
}

// DeltaLogDir returns the directory for insert logs.
func DeltaLogDir(base string, opts ...PathOption) string {
	return LogDir(base, _deltaLogPrefix, opts...)
}

type PathOpt struct {
	collectionID    int64
	setCollectionID bool

	partitionID    int64
	setPartitionID bool

	groupID    int64
	setGroupID bool

	segmentID    int64
	setSegmentID bool
}

type PathOption func(*PathOpt)

func CollectionID(collectionID int64) PathOption {
	return func(opt *PathOpt) {
		opt.collectionID = collectionID
		opt.setCollectionID = true
	}
}

func PartitionID(partitionID int64) PathOption {
	return func(opt *PathOpt) {
		opt.partitionID = partitionID
		opt.setPartitionID = true
	}
}

func GroupID(groupID int64) PathOption {
	return func(opt *PathOpt) {
		opt.groupID = groupID
		opt.setGroupID = true
	}
}

func SegmentID(segmentID int64) PathOption {
	return func(opt *PathOpt) {
		opt.segmentID = segmentID
		opt.setSegmentID = true
	}
}
