package restore

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func newTestCollectionDDLTask() *collectionDDLTask {
	return &collectionDDLTask{logger: zap.NewNop(), option: &Option{}}
}

func TestCollectionDDLTask_shardNum(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		ddlt := newTestCollectionDDLTask()
		ddlt.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		assert.Equal(t, int32(10), ddlt.shardNum())
	})

	t.Run("OverwriteByRequest", func(t *testing.T) {
		ct := newTestCollectionDDLTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		ct.option.MaxShardNum = 5
		assert.Equal(t, int32(5), ct.shardNum())
	})
}

func TestCollectionTask_createColl(t *testing.T) {
	t.Run("Skip", func(t *testing.T) {
		ct := newTestCollectionDDLTask()
		ct.option.SkipCreateCollection = true
		err := ct.createColl(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollectionDDLTask()
		ct.targetNS = namespace.New("db1", "coll1")
		ct.collBackup = &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Fields:             []*backuppb.FieldSchema{{Name: "field", DataType: backuppb.DataType_Int64, IsPrimaryKey: true}},
				Properties:         []*backuppb.KeyValuePair{{Key: "key", Value: "val"}},
				Functions:          []*backuppb.FunctionSchema{{Name: "func", InputFieldNames: []string{"hello"}}},
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
			assert.Equal(t, "hello", coll.Schema.Functions[0].GetInputFieldNames()[0])
			assert.Equal(t, "field", coll.Schema.Fields[0].GetName())
			assert.Equal(t, schemapb.DataType_Int64, coll.Schema.Fields[0].GetDataType())
			assert.Equal(t, true, coll.Schema.Fields[0].GetIsPrimaryKey())
			assert.Equal(t, true, coll.Schema.GetEnableDynamicField())
			assert.Equal(t, commonpb.ConsistencyLevel_Bounded, coll.ConsLevel)
			assert.Equal(t, int32(10), coll.ShardNum)
			assert.Equal(t, 0, coll.PartitionNum)
			assert.Equal(t, "key1", coll.Properties[0].GetKey())
			assert.Equal(t, "val1", coll.Properties[0].GetValue())
		})
		ct.grpcCli = cli
		err := ct.createColl(context.Background())
		assert.NoError(t, err)
	})
}

func TestCollectionTask_getDefaultValue(t *testing.T) {
	t.Run("HasBase64", func(t *testing.T) {
		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: true}}
		bytes, err := proto.Marshal(defaultValue)
		assert.NoError(t, err)
		field := &backuppb.FieldSchema{DefaultValueBase64: base64.StdEncoding.EncodeToString(bytes)}

		task := newTestCollectionDDLTask()
		val, err := task.getDefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("HasProto", func(t *testing.T) {
		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: true}}
		bytes, err := proto.Marshal(defaultValue)
		assert.NoError(t, err)
		field := &backuppb.FieldSchema{DefaultValueProto: string(bytes)}

		task := newTestCollectionDDLTask()
		val, err := task.getDefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("WithoutDefault", func(t *testing.T) {
		field := &backuppb.FieldSchema{}
		task := newTestCollectionDDLTask()
		val, err := task.getDefaultValue(field)
		assert.NoError(t, err)
		assert.Nil(t, val)
	})
}
