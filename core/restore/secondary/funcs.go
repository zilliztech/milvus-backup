package secondary

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
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
	return fmt.Errorf("secondary: %q has dynamic schema enabled but the backup carries no $meta "+
		"field, so the collection cannot be recreated to match the source. Milvus does not return "+
		"$meta from DescribeCollection, so a backup can only record it by reading etcd, which it "+
		"does only when index extra info is enabled. Take the backup again with --backup_index_extra "+
		"(with_index_extra in the REST create request) and milvus.etcd configured", schema.GetName())
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
