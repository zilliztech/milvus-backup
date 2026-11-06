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

func TestRawSchemaPriorityAndDynamicSupplement(t *testing.T) {
	for _, present := range []bool{false, true} {
		name := "SupplementMissingDynamicField"
		if present {
			name = "KeepExistingDynamicField"
		}
		t.Run(name, func(t *testing.T) {
			source := &schemapb.CollectionSchema{Name: "raw", Version: 9, EnableDynamicField: true,
				Fields:            []*schemapb.FieldSchema{{FieldID: 100, Name: "id", IsClusteringKey: true}},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 103, Name: "items", Fields: []*schemapb.FieldSchema{{FieldID: 109, Name: "value", ElementNullable: true}}}}}
			dynamic := &schemapb.FieldSchema{FieldID: 102, Name: "$meta", IsDynamic: true, Nullable: true, DataType: schemapb.DataType_JSON}
			if present {
				source.Fields = append(source.Fields, dynamic)
			}
			data, err := proto.Marshal(source)
			require.NoError(t, err)
			bak := &backuppb.CollectionSchema{Name: "stale", SchemaProto: data, Fields: []*backuppb.FieldSchema{{FieldID: 102, Name: "$meta", IsDynamic: true, Nullable: true, DataType: backuppb.DataType_JSON}}}
			raw, err := RawSchema(bak)
			require.NoError(t, err)
			assert.Equal(t, "raw", raw.GetName())
			assert.Equal(t, int32(9), raw.GetVersion())
			assert.True(t, raw.GetFields()[0].GetIsClusteringKey())
			require.Len(t, raw.GetFields(), 2)
			assert.True(t, proto.Equal(dynamic, raw.GetFields()[1]))
			assert.Equal(t, "value", raw.GetStructArrayFields()[0].GetFields()[0].GetName())
			secondary, err := Schema(bak)
			require.NoError(t, err)
			assert.Equal(t, "items[value]", secondary.GetStructArrayFields()[0].GetFields()[0].GetName())
			assert.Equal(t, int64(109), secondary.GetStructArrayFields()[0].GetFields()[0].GetFieldID())
			assert.True(t, secondary.GetStructArrayFields()[0].GetFields()[0].GetElementNullable())
			assert.Equal(t, data, bak.GetSchemaProto(), "conversion must not mutate stored raw bytes")
		})
	}
}

func TestSchemaMetadataErrorsAndLegacyDefaults(t *testing.T) {
	t.Run("CorruptRawSchema", func(t *testing.T) {
		_, err := Schema(&backuppb.CollectionSchema{Name: "orders", SchemaProto: []byte{0xff}})
		assert.ErrorContains(t, err, "schema_proto")
		assert.ErrorContains(t, err, "orders")
	})
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
		assert.False(t, schema.GetFields()[0].GetIsClusteringKey())
		assert.Nil(t, schema.GetFields()[0].GetTypeSchema())
		child := schema.GetStructArrayFields()[0].GetFields()[0]
		assert.Equal(t, int64(102), child.GetFieldID())
		assert.False(t, child.GetElementNullable())
		assert.Nil(t, child.GetTypeSchema())
	})
}

func TestSchemaFromCollectionMergesResponseProperties(t *testing.T) {
	for _, raw := range []bool{false, true} {
		name := "Projection"
		if raw {
			name = "RawSchema"
		}
		t.Run(name, func(t *testing.T) {
			for _, partition := range []bool{false, true} {
				mode, schemaMode, modeName := "partition_key", "partition", "RequiresNamespaceField"
				if partition {
					mode, schemaMode, modeName = "partition", "partition_key", "NoNamespaceField"
				}
				t.Run(modeName, func(t *testing.T) {
					original := &schemapb.CollectionSchema{Name: "source", EnableNamespace: true,
						Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "id"}},
						Properties: []*commonpb.KeyValuePair{{Key: "schema.only", Value: "keep"}, {Key: "max_field_id", Value: "104"}, {Key: "namespace.mode", Value: schemaMode}}}
					bak := &backuppb.CollectionSchema{Name: "source", EnableNamespace: true,
						Fields: []*backuppb.FieldSchema{{FieldID: 100, Name: "id"}}, Properties: pbconv.MilvusKVToBakKV(original.GetProperties())}
					if !partition {
						bak.Fields = append(bak.Fields, &backuppb.FieldSchema{FieldID: 103, Name: "$namespace_id", DataType: backuppb.DataType_VarChar})
					}
					if raw {
						data, err := proto.Marshal(original)
						require.NoError(t, err)
						bak.SchemaProto = data
					}
					originalBytes := append([]byte(nil), bak.GetSchemaProto()...)
					coll := &backuppb.CollectionBackupInfo{Schema: bak, Properties: []*backuppb.KeyValuePair{
						{Key: "max_field_id", Value: "200"}, {Key: "namespace.mode", Value: mode}, {Key: "response.only", Value: "carry"}}}
					schema, err := SchemaFromCollection(coll)
					require.NoError(t, err)
					assert.Equal(t, map[string]string{"schema.only": "keep", "max_field_id": "200", "namespace.mode": mode, "response.only": "carry"}, pbconv.MilvusKVToMap(schema.GetProperties()))
					assert.Equal(t, !partition, NeedsNamespaceField(schema))
					if partition {
						assert.Len(t, schema.GetFields(), 1)
					} else {
						require.Len(t, schema.GetFields(), 2)
						assert.Equal(t, int64(103), schema.GetFields()[1].GetFieldID())
					}
					assert.Equal(t, "104", bak.GetProperties()[1].GetValue())
					assert.Equal(t, originalBytes, bak.GetSchemaProto(), "stored raw bytes must remain unchanged")
				})
			}
		})
	}
}
