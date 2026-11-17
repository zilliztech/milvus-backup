package backup

import (
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

// A struct-bearing schema must survive backup metadata storage with its struct
// attributes intact: element_nullable and the recursive type_schema bytes are
// what a restore needs to rebuild the fields faithfully.
func TestStructSchemaRoundTrip(t *testing.T) {
	leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}
	inner := &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf},
		Nullable:   true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}},
	}
	nested := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}, Nullable: true}
	source := &schemapb.CollectionSchema{
		Name:        "source",
		Description: "collection",
		Version:     12,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "labels", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
				Nullable: true, ElementNullable: true, TypeSchema: nested},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 103, Name: "items", Description: "structure", Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 104, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array, ElementNullable: true, Nullable: true, TypeSchema: nested},
				{FieldID: 105, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, ElementNullable: true, Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			}}},
	}

	bak, err := newTestCollDDLTask().convSchema(source)
	require.NoError(t, err)
	builder := newMetaBuilder("task", "backup")
	require.NoError(t, builder.addCollection(collref.New("default", "source"), &backuppb.CollectionBackupInfo{CollectionId: 1, Schema: bak}))

	data, err := builder.buildCollectionMeta()
	require.NoError(t, err)
	var meta backuppb.CollectionLevelBackupInfo
	require.NoError(t, json.Unmarshal(data, &meta))
	stored := meta.GetInfos()[0].GetSchema()

	restored, err := conv.Schema(stored)
	require.NoError(t, err)
	assert.Equal(t, int32(12), restored.GetVersion())
	require.Len(t, restored.GetFields(), len(source.GetFields()))
	for i, field := range source.GetFields() {
		assert.True(t, proto.Equal(field, restored.GetFields()[i]), "field %s", field.GetName())
	}
	require.Len(t, restored.GetStructArrayFields(), 1)
	structure := restored.GetStructArrayFields()[0]
	assert.Equal(t, int64(103), structure.GetFieldID())
	assert.Equal(t, "structure", structure.GetDescription())
	// conv qualifies sub-field names as struct[sub] for the create request; the
	// attributes must survive that rename.
	require.Len(t, structure.GetFields(), len(source.GetStructArrayFields()[0].GetFields()))
	assert.Equal(t, "items[values]", structure.GetFields()[0].GetName())
	assert.True(t, structure.GetFields()[0].GetElementNullable())
	assert.True(t, proto.Equal(nested, structure.GetFields()[0].GetTypeSchema()))
	assert.True(t, structure.GetFields()[1].GetElementNullable())
	assert.Equal(t, "4", pbconv.MilvusKVToMap(structure.GetFields()[1].GetTypeParams())["dim"])
}

// Metadata written before these fields existed restores as it always did: the
// absent attributes decode to their zero values and nothing else changes.
func TestOldMetadataHasNoStructAttributes(t *testing.T) {
	var bak backuppb.CollectionSchema
	require.NoError(t, json.Unmarshal([]byte(
		`{"name":"old","fields":[{"fieldID":100,"name":"id","data_type":5}],
		  "struct_array_fields":[{"fieldID":101,"name":"items","fields":[{"fieldID":102,"name":"value","data_type":22,"element_type":5}]}]}`), &bak))
	schema, err := conv.Schema(&bak)
	require.NoError(t, err)
	assert.Nil(t, schema.GetFields()[0].GetTypeSchema())
	assert.False(t, schema.GetFields()[0].GetElementNullable())
	child := schema.GetStructArrayFields()[0].GetFields()[0]
	assert.Equal(t, int64(102), child.GetFieldID())
	assert.False(t, child.GetElementNullable())
	assert.Nil(t, child.GetTypeSchema())
}
