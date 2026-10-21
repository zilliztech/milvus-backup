package restore

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

func newTestCollDDLTask() *collDDLTask {
	return &collDDLTask{logger: zap.NewNop(), option: &Option{}}
}

func TestCollDDLTask_shardNum(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		assert.Equal(t, int32(10), ddlt.shardNum())
	})

	t.Run("OverwriteByRequest", func(t *testing.T) {
		ct := newTestCollDDLTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		ct.option.MaxShardNum = 5
		assert.Equal(t, int32(5), ct.shardNum())
	})

	t.Run("OverwriteByCollOverride", func(t *testing.T) {
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		ddlt.collOverride = CollOverride{ShardNum: 3}
		assert.Equal(t, int32(3), ddlt.shardNum())
	})

	t.Run("CollOverrideTakesPriorityOverMaxShardNum", func(t *testing.T) {
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		ddlt.option.MaxShardNum = 5
		ddlt.collOverride = CollOverride{ShardNum: 8}
		assert.Equal(t, int32(8), ddlt.shardNum())
	})
}

func TestCollDDLTask_description(t *testing.T) {
	t.Run("FromBackup", func(t *testing.T) {
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{Description: "original desc"},
		}
		assert.Equal(t, "original desc", ddlt.description())
	})

	t.Run("OverwriteByCollOverride", func(t *testing.T) {
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{Description: "original desc"},
		}
		ddlt.collOverride = CollOverride{Description: "new desc"}
		assert.Equal(t, "new desc", ddlt.description())
	})

	t.Run("EmptyOverrideFallsBackToBackup", func(t *testing.T) {
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{Description: "original desc"},
		}
		ddlt.collOverride = CollOverride{Description: ""}
		assert.Equal(t, "original desc", ddlt.description())
	})
}

func TestCollDDLTask_fields(t *testing.T) {
	t.Run("WithDynamicField", func(t *testing.T) {
		ddlt := newTestCollDDLTask()

		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{
					{Name: "field1", FieldID: common.StartOfUserFieldID},
					{Name: "field2", FieldID: common.StartOfUserFieldID + 1},
					{Name: "field3", FieldID: common.StartOfUserFieldID + 2},
					// $meta field's field id is common.StartOfUserFieldID + 3
					{Name: "field4", FieldID: common.StartOfUserFieldID + 4},
					{Name: "field5", FieldID: common.StartOfUserFieldID + 5},
				},
				EnableDynamicField: true,
			},
		}

		createFields, addFields, err := ddlt.fields()
		assert.NoError(t, err)
		assert.Len(t, createFields, 3)
		assert.Len(t, addFields, 2)

		assert.Equal(t, "field1", createFields[0].GetName())
		assert.Equal(t, "field2", createFields[1].GetName())
		assert.Equal(t, "field3", createFields[2].GetName())
		assert.Equal(t, "field4", addFields[0].GetName())
	})

	t.Run("WithoutDynamicField", func(t *testing.T) {
		ddlt := newTestCollDDLTask()

		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{
					{Name: "field1", FieldID: common.StartOfUserFieldID},
					{Name: "field2", FieldID: common.StartOfUserFieldID + 1},
				},
				EnableDynamicField: false,
			},
		}

		createFields, addFields, err := ddlt.fields()
		assert.NoError(t, err)
		assert.Len(t, createFields, 2)
		assert.Empty(t, addFields)

		assert.Equal(t, "field1", createFields[0].GetName())
		assert.Equal(t, "field2", createFields[1].GetName())
	})
}

func TestCollDDLTask_properties(t *testing.T) {
	t.Run("NotSupportFuncRuntimeCheck", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(false).Once()

		ddlt := newTestCollDDLTask()
		ddlt.grpcCli = grpcCli
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Properties: []*backuppb.KeyValuePair{{Key: "key", Value: "val"}},
				Functions:  []*backuppb.FunctionSchema{{Name: "func", InputFieldNames: []string{"hello"}}},
			},
		}

		props := ddlt.properties()
		assert.Len(t, props, 1)
		assert.Equal(t, "key", props[0].GetKey())
		assert.Equal(t, "val", props[0].GetValue())
	})

	t.Run("NoFunctions", func(t *testing.T) {
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Properties: []*backuppb.KeyValuePair{{Key: "key", Value: "val"}},
			},
		}

		props := ddlt.properties()
		assert.Len(t, props, 1)
		assert.Equal(t, "key", props[0].GetKey())
		assert.Equal(t, "val", props[0].GetValue())
	})

	t.Run("SupportFuncRuntimeCheck", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true).Once()
		ddlt := newTestCollDDLTask()
		ddlt.grpcCli = grpcCli
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Properties: []*backuppb.KeyValuePair{{Key: "key", Value: "val"}},
				Functions:  []*backuppb.FunctionSchema{{Name: "func", InputFieldNames: []string{"hello"}}},
			},
		}

		props := ddlt.properties()
		assert.Len(t, props, 2)
		assert.Equal(t, "key", props[0].GetKey())
		assert.Equal(t, "val", props[0].GetValue())
		assert.Equal(t, common.DisableFuncRuntimeCheck, props[1].GetKey())
		assert.Equal(t, "true", props[1].GetValue())
	})
}

func TestCollDDLTask_restoreFuncRuntimeCheck(t *testing.T) {
	t.Run("NotSupportFuncRuntimeCheck", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(false).Once()
		ddlt := newTestCollDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Functions: []*backuppb.FunctionSchema{{Name: "func", InputFieldNames: []string{"hello"}}},
			},
		}
		ddlt.grpcCli = grpcCli
		err := ddlt.restoreFuncRuntimeCheck(context.Background())
		assert.NoError(t, err)
	})

	t.Run("NoFunctions", func(t *testing.T) {
		ddlt := newTestCollDDLTask()

		err := ddlt.restoreFuncRuntimeCheck(context.Background())
		assert.NoError(t, err)
	})

	t.Run("SupportFuncRuntimeCheckAndOriginalDisabled", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)

		grpcCli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true).Once()

		ddlt := newTestCollDDLTask()
		ddlt.grpcCli = grpcCli
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Properties: []*backuppb.KeyValuePair{{Key: "disable_func_runtime_check", Value: "true"}},
				Functions:  []*backuppb.FunctionSchema{{Name: "func", InputFieldNames: []string{"hello"}}},
			},
		}
		ddlt.target = collref.New("db1", "coll1")
		err := ddlt.restoreFuncRuntimeCheck(context.Background())
		assert.NoError(t, err)
	})

	t.Run("SupportFuncRuntimeCheckAndNoOriginalValue", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true).Once()
		grpcCli.EXPECT().
			AlterCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Run(func(ctx context.Context, db string, collName string, properties []*commonpb.KeyValuePair) {
				assert.Len(t, properties, 1)
				assert.Equal(t, "disable_func_runtime_check", properties[0].GetKey())
				assert.Equal(t, "false", properties[0].GetValue())

				assert.Equal(t, "db1", db)
				assert.Equal(t, "coll1", collName)
			}).
			Once()

		ddlt := newTestCollDDLTask()
		ddlt.grpcCli = grpcCli
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Properties: []*backuppb.KeyValuePair{{Key: "key", Value: "val"}},
				Functions:  []*backuppb.FunctionSchema{{Name: "func", InputFieldNames: []string{"hello"}}},
			},
		}
		ddlt.target = collref.New("db1", "coll1")
		err := ddlt.restoreFuncRuntimeCheck(context.Background())
		assert.NoError(t, err)
	})
}

func TestCollDDLTask_createColl(t *testing.T) {
	t.Run("Skip", func(t *testing.T) {
		ct := newTestCollDDLTask()
		ct.option.SkipCreateCollection = true
		err := ct.createColl(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollDDLTask()
		ct.target = collref.New("db1", "coll1")
		ct.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{{
					FieldID:      common.StartOfUserFieldID,
					Name:         "field",
					DataType:     backuppb.DataType_Int64,
					IsPrimaryKey: true},
				},
				Properties:         []*backuppb.KeyValuePair{{Key: "key", Value: "val"}},
				Functions:          []*backuppb.FunctionSchema{{Name: "func", InputFieldNames: []string{"field"}}},
				EnableDynamicField: true,
				AutoID:             true,
				Description:        "desc",
			},
			ShardsNum:        10,
			ConsistencyLevel: backuppb.ConsistencyLevel_Bounded,
			PartitionBackups: []*backuppb.PartitionBackupInfo{{PartitionName: "part1"}},
			Properties:       []*backuppb.KeyValuePair{{Key: "key1", Value: "val1"}},
		}

		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Once().Run(func(args mock.Arguments) {
			coll := args[1].(milvus.CreateCollectionInput)
			assert.Equal(t, "db1", coll.DB)
			assert.Equal(t, "coll1", coll.Schema.Name)
			assert.Equal(t, "desc", coll.Schema.Description)
			assert.Equal(t, "key", coll.Schema.Properties[0].GetKey())
			assert.Equal(t, "val", coll.Schema.Properties[0].GetValue())
			assert.Equal(t, "func", coll.Schema.Functions[0].GetName())
			assert.Equal(t, "field", coll.Schema.Functions[0].GetInputFieldNames()[0])
			assert.Equal(t, "field", coll.Schema.Fields[0].GetName())
			assert.Equal(t, schemapb.DataType_Int64, coll.Schema.Fields[0].GetDataType())
			assert.Equal(t, true, coll.Schema.Fields[0].GetIsPrimaryKey())
			assert.Equal(t, true, coll.Schema.GetEnableDynamicField())
			assert.Equal(t, commonpb.ConsistencyLevel_Bounded, coll.ConsLevel)
			assert.Equal(t, int32(10), coll.ShardNum)
			assert.Equal(t, 0, coll.PartitionNum)

			assert.Equal(t, 2, len(coll.Properties))
			assert.Equal(t, "key", coll.Properties[0].GetKey())
			assert.Equal(t, "val", coll.Properties[0].GetValue())
			assert.Equal(t, common.DisableFuncRuntimeCheck, coll.Properties[1].GetKey())
			assert.Equal(t, "true", coll.Properties[1].GetValue())
		})
		cli.EXPECT().HasFeature(milvus.FuncRuntimeCheck).Return(true).Once()
		ct.grpcCli = cli
		err := ct.createColl(context.Background())
		assert.NoError(t, err)
	})

	t.Run("WithCollOverride", func(t *testing.T) {
		ct := newTestCollDDLTask()
		ct.target = collref.New("db1", "coll1")
		ct.collOverride = CollOverride{ShardNum: 2, Description: "overridden desc"}
		ct.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{{
					FieldID:      common.StartOfUserFieldID,
					Name:         "field",
					DataType:     backuppb.DataType_Int64,
					IsPrimaryKey: true},
				},
				Description: "original desc",
			},
			ShardsNum:        10,
			ConsistencyLevel: backuppb.ConsistencyLevel_Bounded,
		}

		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Once().Run(func(args mock.Arguments) {
			coll := args[1].(milvus.CreateCollectionInput)
			assert.Equal(t, "overridden desc", coll.Schema.Description)
			assert.Equal(t, int32(2), coll.ShardNum)
		})
		ct.grpcCli = cli
		err := ct.createColl(context.Background())
		assert.NoError(t, err)
	})
}

func TestSplitFunctions(t *testing.T) {
	createFields := []*schemapb.FieldSchema{{Name: "pk"}, {Name: "text"}, {Name: "emb"}}

	t.Run("EarlyWhenAllFieldsPresent", func(t *testing.T) {
		early, late := splitFunctions([]*backuppb.FunctionSchema{{
			Name:             "fn",
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"emb"},
		}}, createFields)
		assert.Len(t, early, 1)
		assert.Empty(t, late)
	})

	t.Run("LateWhenInputMissing", func(t *testing.T) {
		early, late := splitFunctions([]*backuppb.FunctionSchema{{
			Name:             "fn",
			InputFieldNames:  []string{"added_text"},
			OutputFieldNames: []string{"emb"},
		}}, createFields)
		assert.Empty(t, early)
		assert.Len(t, late, 1)
	})

	t.Run("LateWhenOutputMissing", func(t *testing.T) {
		early, late := splitFunctions([]*backuppb.FunctionSchema{{
			Name:             "fn",
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"added_out"},
		}}, createFields)
		assert.Empty(t, early)
		assert.Len(t, late, 1)
	})

	t.Run("Empty", func(t *testing.T) {
		early, late := splitFunctions(nil, createFields)
		assert.Empty(t, early)
		assert.Empty(t, late)
	})
}

func TestCollDDLTask_addFieldsAndFunctions(t *testing.T) {
	newTask := func(cli *milvus.MockGrpc) *collDDLTask {
		ddlt := newTestCollDDLTask()
		ddlt.target = collref.New("db1", "coll1")
		ddlt.grpcCli = cli
		return ddlt
	}

	t.Run("PlainFieldsInIDOrder", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		var order []string
		cli.EXPECT().AddField(mock.Anything, "db1", "coll1", mock.Anything).
			Run(func(ctx context.Context, db, collName string, field *schemapb.FieldSchema) {
				order = append(order, field.GetName())
			}).Return(nil)

		fields := []*schemapb.FieldSchema{{Name: "b", FieldID: 104}, {Name: "a", FieldID: 103}}
		err := newTask(cli).addFieldsAndFunctions(context.Background(), fields, nil)
		assert.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, order)
	})

	t.Run("LateFunctionFieldReplaysAtOutputPosition", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.AddFunctionField).Return(true)

		var order []string
		cli.EXPECT().AddField(mock.Anything, "db1", "coll1", mock.Anything).
			Run(func(ctx context.Context, db, collName string, field *schemapb.FieldSchema) {
				order = append(order, "field:"+field.GetName())
			}).Return(nil)
		cli.EXPECT().AddFunctionField(mock.Anything, "db1", "coll1", mock.Anything, mock.Anything, "idx_out", map[string]string{"index_type": "SPARSE_INVERTED_INDEX"}).
			Run(func(ctx context.Context, db, collName string, field *schemapb.FieldSchema, fn *schemapb.FunctionSchema, indexName string, indexParams map[string]string) {
				order = append(order, "func:"+fn.GetName())
				assert.Equal(t, "out", field.GetName())
			}).Return(nil)

		ddlt := newTask(cli)
		ddlt.collBackup = &backuppb.CollectionBackupInfo{
			IndexInfos: []*backuppb.IndexInfo{{
				FieldName: "out", IndexName: "idx_out", Params: map[string]string{"index_type": "SPARSE_INVERTED_INDEX"},
			}},
		}

		fields := []*schemapb.FieldSchema{
			{Name: "z", FieldID: 105},
			{Name: "text2", FieldID: 103},
			{Name: "out", FieldID: 104, IsFunctionOutput: true},
		}
		funcs := []*schemapb.FunctionSchema{{
			Name:             "bm25",
			InputFieldNames:  []string{"text2"},
			OutputFieldNames: []string{"out"},
		}}

		err := ddlt.addFieldsAndFunctions(context.Background(), fields, funcs)
		assert.NoError(t, err)
		// the function's output field is not added via AddField, it comes with the function
		assert.Equal(t, []string{"field:text2", "func:bm25", "field:z"}, order)
		assert.Contains(t, ddlt.boundIndexFields, "out")
	})

	t.Run("AttachOnlyFunctionUsesLegacyAPI", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.AddCollectionFunction).Return(true)

		var order []string
		cli.EXPECT().AddField(mock.Anything, "db1", "coll1", mock.Anything).
			Run(func(ctx context.Context, db, collName string, field *schemapb.FieldSchema) {
				order = append(order, "field:"+field.GetName())
			}).Return(nil)
		cli.EXPECT().AddCollectionFunction(mock.Anything, "db1", "coll1", mock.Anything).
			Run(func(ctx context.Context, db, collName string, fn *schemapb.FunctionSchema) {
				order = append(order, "func:"+fn.GetName())
			}).Return(nil)

		fields := []*schemapb.FieldSchema{{Name: "text2", FieldID: 103}}
		funcs := []*schemapb.FunctionSchema{{
			Name:             "embed",
			InputFieldNames:  []string{"text2"},
			OutputFieldNames: []string{"emb"},
		}}

		err := newTask(cli).addFieldsAndFunctions(context.Background(), fields, funcs)
		assert.NoError(t, err)
		assert.Equal(t, []string{"field:text2", "func:embed"}, order)
	})

	t.Run("NoAPIFails", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.AddFunctionField).Return(false)
		cli.EXPECT().HasFeature(milvus.AddCollectionFunction).Return(false)

		fields := []*schemapb.FieldSchema{{Name: "out", FieldID: 104, IsFunctionOutput: true}}
		funcs := []*schemapb.FunctionSchema{{
			Name:             "bm25",
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"out"},
		}}

		err := newTask(cli).addFieldsAndFunctions(context.Background(), fields, funcs)
		assert.ErrorContains(t, err, "cannot add function bm25")
	})
}

func TestCollDDLTask_createIndexSkipsBoundIndex(t *testing.T) {
	ddlt := newTestCollDDLTask()
	ddlt.option.RebuildIndex = true
	ddlt.target = collref.New("db1", "coll1")
	ddlt.boundIndexFields = map[string]struct{}{"out": {}}
	ddlt.collBackup = &backuppb.CollectionBackupInfo{
		Schema: &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{
				{Name: "pk", FieldID: common.StartOfUserFieldID, DataType: backuppb.DataType_Int64, IsPrimaryKey: true},
				{Name: "vec", FieldID: common.StartOfUserFieldID + 1, DataType: backuppb.DataType_FloatVector},
				{Name: "out", FieldID: common.StartOfUserFieldID + 2, DataType: backuppb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
		},
		IndexInfos: []*backuppb.IndexInfo{
			{FieldName: "out", IndexName: "idx_out", Params: map[string]string{"index_type": "SPARSE_INVERTED_INDEX"}},
			{FieldName: "vec", IndexName: "idx_vec", Params: map[string]string{"index_type": "HNSW", "metric_type": "L2"}},
		},
	}

	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().CreateIndex(mock.Anything, mock.Anything).
		Run(func(ctx context.Context, input milvus.CreateIndexInput) {
			assert.Equal(t, "vec", input.FieldName)
		}).Return(nil).Once()
	ddlt.grpcCli = cli

	err := ddlt.createIndex(context.Background())
	assert.NoError(t, err)
}

func TestCollDDLTask_createCollStripsMaxFieldID(t *testing.T) {
	ct := newTestCollDDLTask()
	ct.target = collref.New("db1", "coll1")
	ct.collBackup = &backuppb.CollectionBackupInfo{
		Schema: &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{{
				FieldID:      common.StartOfUserFieldID,
				Name:         "field",
				DataType:     backuppb.DataType_Int64,
				IsPrimaryKey: true},
			},
			Properties: []*backuppb.KeyValuePair{
				{Key: "key", Value: "val"},
				{Key: common.MaxFieldIDKey, Value: "104"},
			},
		},
		ShardsNum: 1,
	}

	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Once().Run(func(args mock.Arguments) {
		coll := args[1].(milvus.CreateCollectionInput)
		for _, kv := range coll.Schema.GetProperties() {
			assert.NotEqual(t, common.MaxFieldIDKey, kv.GetKey())
		}
		for _, kv := range coll.Properties {
			assert.NotEqual(t, common.MaxFieldIDKey, kv.GetKey())
		}
		assert.Len(t, coll.Schema.GetProperties(), 1)
	})
	ct.grpcCli = cli
	err := ct.createColl(context.Background())
	assert.NoError(t, err)
}
