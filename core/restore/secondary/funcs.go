package secondary

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
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
