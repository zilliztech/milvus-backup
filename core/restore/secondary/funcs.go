package secondary

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/zilliztech/milvus-backup/core/restore/funcs"
)

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

func appendDynamicField(schema *schemapb.CollectionSchema) {
	if schema.GetEnableDynamicField() {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:     funcs.GuessDynFieldID(schema.Fields),
			Name:        common.MetaFieldName,
			Description: "dynamic schema",
			DataType:    schemapb.DataType_JSON,
			IsDynamic:   true,
		})
	}
}
