package conv

import (
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func TestSchemaRestoresVersionNamespaceAndSourceIDs(t *testing.T) {
	bak := &backuppb.CollectionSchema{Name: "source", Version: 9, EnableDynamicField: true, EnableNamespace: true,
		Fields: []*backuppb.FieldSchema{{FieldID: 100, Name: "id"},
			{FieldID: 102, Name: "$meta", IsDynamic: true, Nullable: true, DataType: backuppb.DataType_JSON},
			{FieldID: 103, Name: "$namespace_id", DataType: backuppb.DataType_VarChar}},
		StructArrayFields: []*backuppb.StructArrayFieldSchema{{FieldID: 104, Name: "items", Fields: []*backuppb.FieldSchema{{FieldID: 109, Name: "value", ElementNullable: true}}}},
		Functions:         []*backuppb.FunctionSchema{{Id: 100, Name: "function", InputFieldNames: []string{"id"}, InputFieldIds: []int64{100}}},
	}
	schema, err := Schema(bak)
	require.NoError(t, err)
	assert.Equal(t, "source", schema.GetName())
	assert.Equal(t, int32(9), schema.GetVersion())
	assert.True(t, schema.GetEnableNamespace())
	assert.True(t, schema.GetEnableDynamicField())
	require.Len(t, schema.GetFields(), 3)
	assert.Equal(t, int64(102), schema.GetFields()[1].GetFieldID())
	assert.True(t, schema.GetFields()[1].GetIsDynamic())
	assert.Equal(t, int64(103), schema.GetFields()[2].GetFieldID())
	assert.Equal(t, "items[value]", schema.GetStructArrayFields()[0].GetFields()[0].GetName())
	assert.Equal(t, int64(109), schema.GetStructArrayFields()[0].GetFields()[0].GetFieldID())
	assert.True(t, schema.GetStructArrayFields()[0].GetFields()[0].GetElementNullable())
	assert.Equal(t, int64(100), schema.GetFunctions()[0].GetId())
	assert.Equal(t, []int64{100}, schema.GetFunctions()[0].GetInputFieldIds())
	assert.Equal(t, "value", bak.GetStructArrayFields()[0].GetFields()[0].GetName(), "conversion must not mutate metadata")
}

func TestTypeSchemaMetadataRoundTrip(t *testing.T) {
	leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}
	inner := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}}}
	nested := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}, Nullable: true}
	data, err := proto.Marshal(nested)
	require.NoError(t, err)
	field := &backuppb.FieldSchema{FieldID: 109, Name: "nested", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array,
		Nullable: true, ElementNullable: true, TypeSchema: data}
	encoded, err := json.Marshal(field)
	require.NoError(t, err)
	var stored backuppb.FieldSchema
	require.NoError(t, json.Unmarshal(encoded, &stored))
	fields, err := Fields([]*backuppb.FieldSchema{&stored})
	require.NoError(t, err)
	assert.True(t, proto.Equal(nested, fields[0].GetTypeSchema()))
	assert.True(t, fields[0].GetNullable())
	assert.True(t, fields[0].GetElementNullable())
	assert.Equal(t, data, stored.GetTypeSchema())
}

func TestSchemaMetadataErrorsAndLegacyDefaults(t *testing.T) {
	t.Run("CorruptTypeSchema", func(t *testing.T) {
		_, err := Fields([]*backuppb.FieldSchema{{Name: "labels", FieldID: 105, TypeSchema: []byte{0xff}}})
		assert.ErrorContains(t, err, "type_schema")
		assert.ErrorContains(t, err, "labels")
		assert.ErrorContains(t, err, "105")
	})
	t.Run("OldMetadataDefaults", func(t *testing.T) {
		var bak backuppb.CollectionSchema
		require.NoError(t, json.Unmarshal([]byte(`{"name":"old","fields":[{"fieldID":100,"name":"id","data_type":5}],"struct_array_fields":[{"fieldID":101,"name":"items","fields":[{"fieldID":102,"name":"value","data_type":22,"element_type":5}]}]}`), &bak))
		schema, err := Schema(&bak)
		require.NoError(t, err)
		assert.Equal(t, "old", schema.GetName())
		assert.Zero(t, schema.GetVersion())
		assert.False(t, schema.GetEnableNamespace())
		assert.Nil(t, schema.GetFields()[0].GetTypeSchema())
		child := schema.GetStructArrayFields()[0].GetFields()[0]
		assert.Equal(t, int64(102), child.GetFieldID())
		assert.False(t, child.GetElementNullable())
		assert.Nil(t, child.GetTypeSchema())
		assert.NoError(t, ValidateLegacySchema(schema))
	})
}

func TestSchemaFromCollectionMergesResponseProperties(t *testing.T) {
	for _, partition := range []bool{false, true} {
		mode, schemaMode, name := "partition_key", "partition", "RequiresNamespaceField"
		if partition {
			mode, schemaMode, name = "partition", "partition_key", "NoNamespaceField"
		}
		t.Run(name, func(t *testing.T) {
			bak := &backuppb.CollectionSchema{Name: "source", EnableNamespace: true, Version: 3,
				Fields:     []*backuppb.FieldSchema{{FieldID: 100, Name: "id"}},
				Properties: []*backuppb.KeyValuePair{{Key: "schema.only", Value: "keep"}, {Key: "max_field_id", Value: "104"}, {Key: "namespace.mode", Value: schemaMode}}}
			if !partition {
				bak.Fields = append(bak.Fields, &backuppb.FieldSchema{FieldID: 103, Name: "$namespace_id", DataType: backuppb.DataType_VarChar})
			}
			coll := &backuppb.CollectionBackupInfo{Schema: bak, Properties: []*backuppb.KeyValuePair{{Key: "max_field_id", Value: "200"}, {Key: "namespace.mode", Value: mode}, {Key: "response.only", Value: "carry"}}}
			schema, err := SchemaFromCollection(coll)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"schema.only": "keep", "max_field_id": "200", "namespace.mode": mode, "response.only": "carry"}, pbconv.MilvusKVToMap(schema.GetProperties()))
			assert.Equal(t, int32(3), schema.GetVersion())
			assert.Equal(t, !partition, NeedsNamespaceField(schema))
			if partition {
				assert.Len(t, schema.GetFields(), 1)
			} else {
				require.Len(t, schema.GetFields(), 2)
				assert.Equal(t, int64(103), schema.GetFields()[1].GetFieldID())
			}
			assert.Equal(t, "104", bak.GetProperties()[1].GetValue())
		})
	}
}
