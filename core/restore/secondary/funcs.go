package secondary

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// checkDynamicField fails the restore if the source collection had dynamic
// schema enabled but the backup metadata does not carry the actual $meta
// field. See zilliztech/milvus-backup#1013.
func checkDynamicField(schema *schemapb.CollectionSchema) error {
	if !schema.GetEnableDynamicField() {
		return nil
	}
	for _, f := range schema.GetFields() {
		if f.GetIsDynamic() {
			return nil
		}
	}
	return fmt.Errorf("secondary: %q missing dynamic field in backup", schema.GetName())
}

// indexNames formats the indexes as "<db>.<coll>/<index>" for error and log
// messages.
func indexNames(backup *backuppb.BackupInfo, has func(*backuppb.IndexInfo) bool) []string {
	var names []string
	for _, coll := range backup.GetCollectionBackups() {
		for _, index := range coll.GetIndexInfos() {
			if !has(index) {
				continue
			}
			names = append(names, fmt.Sprintf("%s.%s/%s",
				coll.GetDbName(), coll.GetCollectionName(), index.GetIndexName()))
		}
	}
	return names
}

// hasIndexExtra reports whether an index info carries the attributes that only
// the index-extra task collects. field_id, type_params, index_params,
// create_time, is_auto_index and min/max_index_version are read from etcd when
// the backup is created with --backup_index_extra; without it the index info
// holds only what DescribeIndex returns (field_name, index_name, index_type,
// params, index_id).
//
// FieldID 0 is RowIDField, and user fields start at 100, so a zero field id
// means the etcd attributes were never merged in.
func hasIndexExtra(index *backuppb.IndexInfo) bool {
	return index.GetFieldId() != 0 && len(index.GetIndexParams()) != 0
}

// replayIndex decides whether the create index DDL of a backup can be replayed
// on the secondary cluster, and is the only place that decision is made.
//
// The backup answers it as a whole, because the extra attributes are collected
// for every index at once or for none: a backup created with
// --backup_index_extra carries the indexes to replay, one created without it
// carries no index information that a verbatim DDL replay can use, and its
// collections are restored without indexes.
//
// The missing attributes cannot be reconstructed on the client side - they are
// what makes the replay match the source cluster - so a partially populated
// backup is a contradiction rather than a case to work around, and is
// reported. The alternative, broadcasting create index with FieldID 0, leaves
// the target with an index on a field that cannot be indexed: the import job
// sits in IndexBuilding forever and DescribeIndex fails with "failed to get
// collection field: 0". See zilliztech/milvus-backup#1167.
func replayIndex(backup *backuppb.BackupInfo) (bool, error) {
	with := indexNames(backup, hasIndexExtra)
	without := indexNames(backup, func(index *backuppb.IndexInfo) bool { return !hasIndexExtra(index) })

	switch {
	case len(with) != 0 && len(without) != 0:
		return false, fmt.Errorf("secondary: backup %q carries the index extra info of %s but not of %s, "+
			"the index extra info is collected for every index or for none, so the backup meta is "+
			"inconsistent; re-create the backup with --backup_index_extra",
			backup.GetName(), strings.Join(with, ", "), strings.Join(without, ", "))
	case len(without) != 0:
		return false, nil
	default:
		return true, nil
	}
}

func appendSysFields(schema *schemapb.CollectionSchema) {
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(common.RowIDField),
		Name:         common.RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "row id",
		DataType:     schemapb.DataType_Int64,
	})

	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(common.TimeStampField),
		Name:         common.TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "time stamp",
		DataType:     schemapb.DataType_Int64,
	})
}
