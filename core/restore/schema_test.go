package restore

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func evolvedSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "source", Description: "original", EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, IsClusteringKey: true, IsPartitionKey: true},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}, {Key: "skip_type", Value: "yes"}},
				IndexParams: []*commonpb.KeyValuePair{{Key: "skip_index", Value: "yes"}}},
			{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true, Nullable: true},
			{FieldID: 110, Name: "output", DataType: schemapb.DataType_Int64, IsFunctionOutput: true},
		},
		Properties: []*commonpb.KeyValuePair{{Key: "max_field_id", Value: "112"}, {Key: "mmap.enabled", Value: "true"}, {Key: "preserve_field_ids", Value: "false"}},
		Functions:  []*schemapb.FunctionSchema{{Id: 100, Name: "function", InputFieldNames: []string{"id"}, InputFieldIds: []int64{100}, OutputFieldNames: []string{"output"}, OutputFieldIds: []int64{110}}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 103, Name: "items", Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 105, Name: "value", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true, Nullable: true},
				{FieldID: 109, Name: "nested", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array, Nullable: true,
					TypeSchema: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}}, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "skip_type", Value: "yes"}}}}}},
			}}},
	}
}

func backupRawSchema(t *testing.T, schema *schemapb.CollectionSchema) *backuppb.CollectionBackupInfo {
	t.Helper()
	data, err := proto.Marshal(schema)
	require.NoError(t, err)
	return &backuppb.CollectionBackupInfo{Schema: &backuppb.CollectionSchema{SchemaProto: data},
		ShardsNum: 2, ConsistencyLevel: backuppb.ConsistencyLevel_Bounded,
		PartitionBackups: []*backuppb.PartitionBackupInfo{{PartitionName: "p0"}, {PartitionName: "p1"}}}
}

func TestCreateCollectionPreservesSourceFieldIDs(t *testing.T) {
	source := evolvedSchema()
	source.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(true).Once()
	cli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true).Once()
	cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Run(func(_ context.Context, in milvus.CreateCollectionInput) {
		assert.Equal(t, "target_db", in.DB)
		assert.Equal(t, "target", in.Schema.GetName())
		assert.Equal(t, "overridden", in.Schema.GetDescription())
		assert.Equal(t, int32(1), in.ShardNum)
		assert.Equal(t, 2, in.PartitionNum)
		assert.Equal(t, commonpb.ConsistencyLevel_Bounded, in.ConsLevel)
		assert.Equal(t, "true", pbconv.MilvusKVToMap(in.Properties)[preserveFieldIDsProperty])
		assert.Equal(t, "112", pbconv.MilvusKVToMap(in.Properties)[maxFieldIDProperty])
		assert.NotContains(t, pbconv.MilvusKVToMap(in.Properties), "mmap.enabled")
		assert.Len(t, in.Schema.GetFields(), 4)
		assert.Equal(t, int64(102), in.Schema.GetFields()[2].GetFieldID())
		assert.True(t, in.Schema.GetFields()[2].GetIsDynamic())
		assert.True(t, in.Schema.GetFields()[0].GetIsClusteringKey())
		assert.NotContains(t, pbconv.MilvusKVToMap(in.Schema.GetFields()[1].GetTypeParams()), "skip_type")
		assert.Empty(t, in.Schema.GetFields()[1].GetIndexParams())
		structure := in.Schema.GetStructArrayFields()[0]
		assert.Equal(t, int64(103), structure.GetFieldID())
		assert.Equal(t, int64(105), structure.GetFields()[0].GetFieldID())
		assert.Equal(t, int64(109), structure.GetFields()[1].GetFieldID())
		assert.True(t, structure.GetFields()[0].GetElementNullable())
		assert.Empty(t, structure.GetFields()[1].GetTypeSchema().GetArrayElement().GetTypeParams())
		assert.True(t, proto.Equal(source.GetFunctions()[0], in.Schema.GetFunctions()[0]))
		assert.Equal(t, source.ProtoReflect().GetUnknown(), in.Schema.ProtoReflect().GetUnknown())
	}).Once()
	ddlt := newTestCollDDLTask()
	ddlt.grpcCli = cli
	ddlt.target = collref.New("target_db", "target")
	ddlt.collBackup = backupRawSchema(t, source)
	ddlt.collOverride = CollOverride{Description: "overridden", ShardNum: 1}
	ddlt.option.SkipParams = SkipParams{CollectionProperties: []string{"mmap.enabled"}, FieldTypeParams: []string{"skip_type"}, FieldIndexParams: []string{"skip_index"}}
	require.NoError(t, ddlt.createColl(context.Background()))
	assert.Equal(t, "original", source.GetDescription())
	assert.Contains(t, pbconv.MilvusKVToMap(source.GetFields()[1].GetTypeParams()), "skip_type")
}

func TestPreservedSchemaRequiresCompleteIDs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*schemapb.CollectionSchema)
		want   string
	}{
		{"MissingDynamicField", func(s *schemapb.CollectionSchema) { s.Fields = append(s.Fields[:2], s.Fields[3:]...) }, "$meta"},
		{"DuplicateSubFieldID", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].FieldID = 100 }, "duplicate"},
		{"MissingSubFieldID", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].FieldID = 0 }, "invalid source ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := evolvedSchema()
			tc.change(schema)
			assert.ErrorContains(t, validatePreservedIDs(schema), tc.want)
		})
	}
	t.Run("MaxFieldIDCannotBeSkipped", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(true)
		cli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true)
		ddlt := newTestCollDDLTask()
		ddlt.grpcCli = cli
		ddlt.collBackup = backupRawSchema(t, evolvedSchema())
		ddlt.option.SkipParams.CollectionProperties = []string{maxFieldIDProperty}
		_, _, err := ddlt.createCollectionInput()
		assert.ErrorContains(t, err, "cannot skip max_field_id")
	})
}

func originalStructSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Name: "original", Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id"}, {FieldID: 101, Name: "vec"}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 102, Name: "items", Fields: []*schemapb.FieldSchema{{FieldID: 103, Name: "first", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, {FieldID: 104, Name: "second", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}}}},
		Properties:        []*commonpb.KeyValuePair{{Key: maxFieldIDProperty, Value: "104"}}}
}

func TestOldTargetSchemaValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*schemapb.CollectionSchema)
		want   string
	}{
		{"OriginalStruct", func(*schemapb.CollectionSchema) {}, ""},
		{"ContiguousAppendWithChangedVersion", func(s *schemapb.CollectionSchema) { s.Version = 1 }, "struct schema version 1"},
		{"ElementNullable", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].ElementNullable = true }, "element_nullable"},
		{"NestedArray", func(s *schemapb.CollectionSchema) {
			s.StructArrayFields[0].Fields[0].ElementType = schemapb.DataType_Array
		}, "nested arrays"},
		{"DeletedMiddleSubField", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields = s.StructArrayFields[0].Fields[1:] }, "sub-field IDs"},
		{"AddedSubFieldOutOfOrder", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[1].FieldID = 110 }, "sub-field IDs"},
		{"DeletedTrailingSubField", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields = s.StructArrayFields[0].Fields[:1] }, "deleted field IDs"},
		{"DeletedTopLevelField", func(s *schemapb.CollectionSchema) { s.Fields = s.Fields[:1] }, "evolved field IDs"},
		{"InvalidHighWaterMark", func(s *schemapb.CollectionSchema) { s.Properties[0].Value = "broken" }, "invalid max_field_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := originalStructSchema()
			tc.change(source)
			cli := milvus.NewMockGrpc(t)
			cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(false)
			ddlt := newTestCollDDLTask()
			ddlt.grpcCli = cli
			ddlt.target = collref.New("default", "target")
			ddlt.collBackup = backupRawSchema(t, source)
			ddlt.option.DropExistCollection = true
			if tc.want == "" {
				in, add, err := ddlt.createCollectionInput()
				require.NoError(t, err)
				assert.Empty(t, add)
				assert.NotContains(t, pbconv.MilvusKVToMap(in.Properties), preserveFieldIDsProperty)
				assert.Equal(t, int64(103), in.Schema.GetStructArrayFields()[0].GetFields()[0].GetFieldID())
			} else {
				// A requested drop must not run before validation fails.
				assert.ErrorContains(t, ddlt.execute(context.Background()), tc.want)
			}
		})
	}
	t.Run("OriginalDynamicStructFromOldMetadata", func(t *testing.T) {
		schema := originalStructSchema()
		schema.EnableDynamicField = true
		schema.StructArrayFields[0].FieldID++
		for _, f := range schema.StructArrayFields[0].Fields {
			f.FieldID++
		}
		schema.Properties[0].Value = "105"
		assert.NoError(t, validateLegacySchema(schema))
	})
}

func TestHiddenFieldIDFallback(t *testing.T) {
	for _, tc := range []struct {
		name          string
		segments      bool
		namespace     bool
		partitionMode bool
	}{
		{"EmptyDynamicCollection", false, false, false},
		{"NonemptyDynamicCollection", true, false, false},
		{"EmptyDynamicAndNamespaceCollection", false, true, false},
		{"NonemptyDynamicAndNamespaceCollection", true, true, false},
		{"NamespacePartitionMode", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := evolvedSchema()
			source.Fields = append(source.Fields[:2], source.Fields[3:]...)
			source.EnableNamespace = tc.namespace
			if tc.partitionMode {
				source.Properties = append(source.Properties, &commonpb.KeyValuePair{Key: "namespace.mode", Value: "partition"})
			}
			cli := milvus.NewMockGrpc(t)
			cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(true)
			ddlt := newTestCollDDLTask()
			ddlt.grpcCli = cli
			ddlt.collBackup = backupRawSchema(t, source)
			if tc.segments {
				ddlt.collBackup.PartitionBackups[0].SegmentBackups = []*backuppb.SegmentBackupInfo{{SegmentId: 1, Binlogs: []*backuppb.FieldBinlog{{FieldID: 107}, {FieldID: 108}}}}
				_, _, err := ddlt.createCollectionInput()
				assert.ErrorContains(t, err, "source IDs for $meta")
				assert.ErrorContains(t, err, "dropped fields")
			} else {
				cli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true)
				in, add, err := ddlt.createCollectionInput()
				require.NoError(t, err)
				assert.Empty(t, add)
				if tc.namespace && !tc.partitionMode {
					assert.Equal(t, 2, in.PartitionNum)
				}
				assert.Len(t, in.Schema.GetFields(), 3, "all visible fields must be created before functions resolve them")
				assert.NotContains(t, pbconv.MilvusKVToMap(in.Properties), preserveFieldIDsProperty)
				assert.NotContains(t, pbconv.MilvusKVToMap(in.Schema.GetProperties()), preserveFieldIDsProperty)
			}
		})
	}
}

func TestPreservedNamespaceAndProxySchema(t *testing.T) {
	source := evolvedSchema()
	source.EnableNamespace = true
	source.Fields = append(source.Fields, &schemapb.FieldSchema{FieldID: 111, Name: "$namespace_id", DataType: schemapb.DataType_VarChar, IsPartitionKey: true},
		&schemapb.FieldSchema{FieldID: 0, Name: "RowID"}, &schemapb.FieldSchema{FieldID: 1, Name: "Timestamp"})
	child := source.StructArrayFields[0].Fields[0]
	child.Name = "items[value]"
	child.TypeParams = []*commonpb.KeyValuePair{{Key: "__struct_sub_field_added__", Value: "true"}, {Key: "max_capacity", Value: "8"}}
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(true)
	cli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true)
	ddlt := newTestCollDDLTask()
	ddlt.grpcCli = cli
	ddlt.collBackup = backupRawSchema(t, source)
	in, _, err := ddlt.createCollectionInput()
	require.NoError(t, err)
	assert.Len(t, in.Schema.GetFields(), 5)
	assert.Equal(t, int64(111), in.Schema.GetFields()[4].GetFieldID())
	assert.Equal(t, "value", in.Schema.GetStructArrayFields()[0].GetFields()[0].GetName())
	assert.NotContains(t, pbconv.MilvusKVToMap(in.Schema.GetStructArrayFields()[0].GetFields()[0].GetTypeParams()), "__struct_sub_field_added__")
	assert.Equal(t, "8", pbconv.MilvusKVToMap(in.Schema.GetStructArrayFields()[0].GetFields()[0].GetTypeParams())["max_capacity"])
}

func TestPreservedFunctionAndHiddenValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*schemapb.CollectionSchema)
		want   string
	}{
		{"ChildBeforeStruct", func(s *schemapb.CollectionSchema) {
			s.StructArrayFields[0].Fields[0].FieldID = 104
			s.StructArrayFields[0].FieldID = 106
		}, "must exceed"},
		{"MissingNamespace", func(s *schemapb.CollectionSchema) { s.EnableNamespace = true }, "$namespace_id"},
		{"UnexpectedDynamicField", func(s *schemapb.CollectionSchema) { s.EnableDynamicField = false }, "$meta presence"},
		{"InvalidFunctionID", func(s *schemapb.CollectionSchema) { s.Functions[0].Id = 0 }, "function"},
		{"DuplicateFunctionID", func(s *schemapb.CollectionSchema) {
			s.Functions = append(s.Functions, proto.Clone(s.Functions[0]).(*schemapb.FunctionSchema))
		}, "duplicate function ID"},
		{"FunctionIDMismatch", func(s *schemapb.CollectionSchema) { s.Functions[0].InputFieldIds = []int64{105} }, "does not match"},
		{"FunctionFieldMissing", func(s *schemapb.CollectionSchema) { s.Functions[0].InputFieldNames = []string{"absent"} }, "unknown field"},
		{"FunctionIDsOmitted", func(s *schemapb.CollectionSchema) {
			s.Functions[0].InputFieldIds = nil
			s.Functions[0].OutputFieldIds = nil
		}, ""},
		{"InvalidMaxFieldID", func(s *schemapb.CollectionSchema) { s.Properties[0].Value = "invalid" }, "invalid max_field_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := evolvedSchema()
			tc.change(source)
			err := validatePreservedIDs(source)
			if tc.want == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestProjectionSchemaUsesNewFieldAttributes(t *testing.T) {
	typeSchema := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}}}
	data, err := proto.Marshal(typeSchema)
	require.NoError(t, err)
	ddlt := newTestCollDDLTask()
	ddlt.collBackup = &backuppb.CollectionBackupInfo{Schema: &backuppb.CollectionSchema{
		Fields:            []*backuppb.FieldSchema{{FieldID: 100, Name: "id", DataType: backuppb.DataType_Int64, IsClusteringKey: true}},
		StructArrayFields: []*backuppb.StructArrayFieldSchema{{FieldID: 101, Name: "items", Fields: []*backuppb.FieldSchema{{FieldID: 105, Name: "nested", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array, ElementNullable: true, TypeSchema: data}}}},
		Properties:        []*backuppb.KeyValuePair{{Key: maxFieldIDProperty, Value: "108"}},
	}}
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(true)
	ddlt.grpcCli = cli
	in, add, err := ddlt.createCollectionInput()
	require.NoError(t, err)
	assert.Empty(t, add)
	assert.True(t, in.Schema.GetFields()[0].GetIsClusteringKey())
	field := in.Schema.GetStructArrayFields()[0].GetFields()[0]
	assert.Equal(t, int64(105), field.GetFieldID())
	assert.True(t, field.GetElementNullable())
	assert.True(t, proto.Equal(typeSchema, field.GetTypeSchema()))
}

func TestLegacyCreateStripsSourcePreservationSwitch(t *testing.T) {
	source := originalStructSchema()
	source.Properties = append(source.Properties, &commonpb.KeyValuePair{Key: preserveFieldIDsProperty, Value: "true"})
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(false)
	ddlt := newTestCollDDLTask()
	ddlt.grpcCli = cli
	ddlt.collBackup = backupRawSchema(t, source)
	in, _, err := ddlt.createCollectionInput()
	require.NoError(t, err)
	assert.NotContains(t, pbconv.MilvusKVToMap(in.Properties), preserveFieldIDsProperty)
	assert.NotContains(t, pbconv.MilvusKVToMap(in.Schema.GetProperties()), preserveFieldIDsProperty)
}

func TestSkipCreateStillChecksTargetSchemaSupport(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(false)
	ddlt := newTestCollDDLTask()
	ddlt.grpcCli = cli
	ddlt.collBackup = backupRawSchema(t, evolvedSchema())
	ddlt.option.SkipCreateCollection = true
	ddlt.option.DropExistCollection = true
	assert.ErrorContains(t, ddlt.execute(context.Background()), "element_nullable")
}

func TestCreateCollectionUsesResponseProperties(t *testing.T) {
	source := originalStructSchema()
	source.EnableNamespace = true
	source.Properties = []*commonpb.KeyValuePair{{Key: "schema.only", Value: "keep"}, {Key: "namespace.mode", Value: "partition_key"}}
	coll := backupRawSchema(t, source)
	coll.Properties = []*backuppb.KeyValuePair{{Key: "max_field_id", Value: "200"}, {Key: "namespace.mode", Value: "partition"}, {Key: "response.only", Value: "carry"}, {Key: "skip.response", Value: "drop"}}
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(true)
	ddlt := newTestCollDDLTask()
	ddlt.grpcCli = cli
	ddlt.collBackup = coll
	ddlt.option.SkipParams.CollectionProperties = []string{"skip.response"}
	in, _, err := ddlt.createCollectionInput()
	require.NoError(t, err)
	properties := pbconv.MilvusKVToMap(in.Properties)
	assert.Equal(t, "200", properties[maxFieldIDProperty])
	assert.Equal(t, "partition", properties["namespace.mode"], "response mode must control the need for hidden namespace IDs")
	assert.Equal(t, "keep", properties["schema.only"])
	assert.Equal(t, "carry", properties["response.only"])
	assert.NotContains(t, properties, "skip.response")
	assert.Equal(t, "200", pbconv.MilvusKVToMap(in.Schema.GetProperties())[maxFieldIDProperty])
	assert.Len(t, in.Schema.GetFields(), 2)
	ddlt.option.SkipParams.CollectionProperties = []string{maxFieldIDProperty}
	_, _, err = ddlt.createCollectionInput()
	assert.ErrorContains(t, err, "cannot skip max_field_id")
}

func TestLegacyValidationUsesResponseHighWaterMark(t *testing.T) {
	source := originalStructSchema()
	source.Properties = nil
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(false)
	ddlt := newTestCollDDLTask()
	ddlt.grpcCli = cli
	ddlt.collBackup = backupRawSchema(t, source)
	ddlt.collBackup.Properties = []*backuppb.KeyValuePair{{Key: maxFieldIDProperty, Value: "200"}}
	ddlt.option.DropExistCollection = true
	assert.ErrorContains(t, ddlt.execute(context.Background()), "deleted field IDs")
}
