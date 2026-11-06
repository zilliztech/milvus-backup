package backup

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func TestCollectionSchemaRoundTrip(t *testing.T) {
	leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}
	inner := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}, Nullable: true}
	nested := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}
	source := &schemapb.CollectionSchema{
		Name: "source", Version: 12,
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64,
			IsPrimaryKey: true, IsClusteringKey: true, ExternalField: "external_id"}},
		Properties: []*commonpb.KeyValuePair{{Key: "max_field_id", Value: "110"}},
		Functions:  []*schemapb.FunctionSchema{{Id: 7, Name: "function", InputFieldIds: []int64{100}, OutputFieldIds: []int64{108}}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 101, Name: "items", Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 103, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
					ElementNullable: true, Nullable: true, TypeSchema: nested},
				{FieldID: 108, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
					ElementNullable: true, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			},
		}},
	}
	// A field unknown to this client must survive the persisted raw protobuf too.
	source.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	bak, err := newTestCollDDLTask().convSchema(source)
	require.NoError(t, err)
	builder := newMetaBuilder("backup", "backup")
	require.NoError(t, builder.addCollection(collref.New("default", "source"), &backuppb.CollectionBackupInfo{CollectionId: 1, Schema: bak}))
	for _, build := range []struct {
		name string
		fn   func() ([]byte, error)
	}{
		{"CollectionMetadata", builder.buildCollectionMeta},
		{"FullMetadata", builder.buildFullMeta},
	} {
		t.Run(build.name, func(t *testing.T) {
			data, err := build.fn()
			require.NoError(t, err)
			var stored *backuppb.CollectionSchema
			if build.name == "CollectionMetadata" {
				var meta backuppb.CollectionLevelBackupInfo
				require.NoError(t, json.Unmarshal(data, &meta))
				stored = meta.GetInfos()[0].GetSchema()
			} else {
				var meta backuppb.BackupInfo
				require.NoError(t, json.Unmarshal(data, &meta))
				stored = meta.GetCollectionBackups()[0].GetSchema()
			}
			raw, err := conv.RawSchema(stored)
			require.NoError(t, err)
			assert.True(t, proto.Equal(source, raw), "raw collection schema must survive metadata JSON")
			fields, err := conv.Fields(stored.GetFields())
			require.NoError(t, err)
			assert.True(t, proto.Equal(source.GetFields()[0], fields[0]))
			subs, err := conv.Fields(stored.GetStructArrayFields()[0].GetFields())
			require.NoError(t, err)
			for i, field := range source.GetStructArrayFields()[0].GetFields() {
				assert.True(t, proto.Equal(field, subs[i]), "fallback field %s", field.GetName())
			}
			stored.SchemaProto = nil
			legacy, err := conv.Schema(stored)
			require.NoError(t, err)
			assert.Equal(t, int64(101), legacy.GetStructArrayFields()[0].GetFieldID())
			assert.Equal(t, int64(108), legacy.GetStructArrayFields()[0].GetFields()[1].GetFieldID())
			assert.True(t, proto.Equal(source.GetFunctions()[0], legacy.GetFunctions()[0]))
		})
	}
}

func TestDynamicFieldConversionIncludesNewAttributes(t *testing.T) {
	source := &schemapb.FieldSchema{FieldID: 102, Name: "$meta", IsDynamic: true,
		DataType: schemapb.DataType_JSON, Nullable: true, ElementNullable: true, IsClusteringKey: true,
		ExternalField: "metadata", TypeSchema: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_JSON}}}
	stored, err := convDynField(source)
	require.NoError(t, err)
	fields, err := conv.Fields([]*backuppb.FieldSchema{stored})
	require.NoError(t, err)
	assert.True(t, proto.Equal(source, fields[0]))
}

func TestBinlogRejectsManifestSegments(t *testing.T) {
	_, err := (&collDMLTask{}).getSegment(context.Background(), &milvuspb.PersistentSegmentInfo{SegmentID: 1, StorageVersion: 3})
	assert.ErrorContains(t, err, "manifest storage version 3")
	assert.ErrorContains(t, err, "snapshot")
}

func TestHiddenNamespaceFieldCapture(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		name := "BinlogMetadata"
		if snapshot {
			name = "SnapshotMetadata"
		}
		t.Run(name, func(t *testing.T) {
			public := &schemapb.CollectionSchema{Name: "namespace", EnableNamespace: true, Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id"}}}
			schema, err := newTestCollDDLTask().convSchema(public)
			require.NoError(t, err)
			builder := newMetaBuilder("backup", "backup")
			coll := &backuppb.CollectionBackupInfo{CollectionId: 1, CollectionName: "namespace", Schema: schema}
			ref := collref.New("default", "namespace")
			require.NoError(t, builder.addCollection(ref, coll))
			hidden := &schemapb.FieldSchema{FieldID: 102, Name: "$namespace_id", DataType: schemapb.DataType_VarChar, IsPartitionKey: true}
			if snapshot {
				bundle := proto.Clone(public).(*schemapb.CollectionSchema)
				bundle.Fields = append(bundle.Fields, hidden)
				require.NoError(t, builder.addSnapshot(ref, &backuppb.SnapshotBackupInfo{}, 10, bundle, nil))
			} else {
				assert.ErrorContains(t, builder.addNamespaceFields(nil), "missing $namespace_id")
				require.NoError(t, builder.addNamespaceFields(map[int64]*schemapb.FieldSchema{1: hidden}))
			}
			restored, err := conv.RawSchema(coll.GetSchema())
			require.NoError(t, err)
			require.Len(t, restored.GetFields(), 2)
			assert.True(t, proto.Equal(hidden, restored.GetFields()[1]))
			assert.True(t, restored.GetEnableNamespace())
		})
	}
	t.Run("PartitionModeNeedsNoHiddenNamespaceField", func(t *testing.T) {
		field, err := bundleNamespaceField(&schemapb.CollectionSchema{EnableNamespace: true, Properties: []*commonpb.KeyValuePair{{Key: "namespace.mode", Value: "partition"}}})
		assert.NoError(t, err)
		assert.Nil(t, field)
	})
}

func TestCollectionResponsePropertiesControlHiddenFieldCapture(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "ResponseOnlyMode"
		if conflict {
			name = "ResponseOverridesMode"
		}
		t.Run(name, func(t *testing.T) {
			public := &schemapb.CollectionSchema{Name: "namespace", EnableNamespace: true,
				Properties: []*commonpb.KeyValuePair{{Key: "schema.only", Value: "keep"}}}
			if conflict {
				public.Properties = append(public.Properties, &commonpb.KeyValuePair{Key: "namespace.mode", Value: "partition_key"})
			}
			schema, err := newTestCollDDLTask().convSchema(public)
			require.NoError(t, err)
			originalBytes := append([]byte(nil), schema.GetSchemaProto()...)
			coll := &backuppb.CollectionBackupInfo{CollectionId: 1, DbName: "default", CollectionName: "namespace", Schema: schema,
				Properties: []*backuppb.KeyValuePair{{Key: "namespace.mode", Value: "partition"}, {Key: "max_field_id", Value: "200"}}}
			builder := newMetaBuilder("backup", "backup")
			require.NoError(t, builder.addCollection(collref.New("default", "namespace"), coll))
			assert.Equal(t, map[string]string{"schema.only": "keep", "namespace.mode": "partition", "max_field_id": "200"}, pbconv.MilvusKVToMap(pbconv.BakKVToMilvusKV(coll.GetSchema().GetProperties())))
			assert.Empty(t, builder.dynamicSchemaCollections(), "partition namespaces require no hidden field")
			assert.NoError(t, builder.addNamespaceFields(nil))
			assert.Equal(t, originalBytes, coll.GetSchema().GetSchemaProto())
			var original schemapb.CollectionSchema
			require.NoError(t, proto.Unmarshal(coll.GetSchema().GetSchemaProto(), &original))
			assert.Equal(t, conflict, pbconv.MilvusKVToMap(original.GetProperties())["namespace.mode"] == "partition_key")
		})
	}
}
