package restore

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func newTestCollectionTask() *collectionTask {
	return &collectionTask{logger: zap.NewNop(), option: &Option{}}
}

func TestGetFailedReason(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "failed_reason", Value: "hello"}})
		assert.Equal(t, "hello", r)
	})

	t.Run("WithoutFailedReason", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, "", r)
	})
}

func TestGetProcess(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "progress_percent", Value: "100"}})
		assert.Equal(t, 100, r)
	})

	t.Run("WithoutProgress", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, 0, r)
	})
}

func TestCollectionTask_shardNum(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollectionTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		assert.Equal(t, int32(10), ct.shardNum())
	})

	t.Run("OverwriteByRequest", func(t *testing.T) {
		ct := newTestCollectionTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{ShardsNum: 10}
		ct.option.MaxShardNum = 5
		assert.Equal(t, int32(5), ct.shardNum())
	})
}

func TestCollectionTask_ezk(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollectionTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "hello"}
		assert.Equal(t, "hello", ct.ezk())
	})

	t.Run("WithoutEZK", func(t *testing.T) {
		ct := newTestCollectionTask()
		assert.Equal(t, "", ct.ezk())
	})
}

func TestCollectionTask_createColl(t *testing.T) {
	t.Run("Skip", func(t *testing.T) {
		ct := newTestCollectionTask()
		ct.option.SkipCreateCollection = true
		err := ct.createColl(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollectionTask()
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

		task := newTestCollectionTask()
		val, err := task.getDefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("HasProto", func(t *testing.T) {
		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: true}}
		bytes, err := proto.Marshal(defaultValue)
		assert.NoError(t, err)
		field := &backuppb.FieldSchema{DefaultValueProto: string(bytes)}

		task := newTestCollectionTask()
		val, err := task.getDefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("WithoutDefault", func(t *testing.T) {
		field := &backuppb.FieldSchema{}
		task := newTestCollectionTask()
		val, err := task.getDefaultValue(field)
		assert.NoError(t, err)
		assert.Nil(t, val)
	})
}

func TestToPaths(t *testing.T) {
	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	paths := toPaths(dir)
	assert.Equal(t, []string{"insert", "delta"}, paths)

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	paths = toPaths(dir)
	assert.Equal(t, []string{"insert", ""}, paths)

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	paths = toPaths(dir)
	assert.Equal(t, []string{"delta"}, paths)
}

func TestL0SegmentBatches(t *testing.T) {
	segs := make([]*backuppb.SegmentBackupInfo, 0, 10)
	for i := 0; i < 10; i++ {
		vch := fmt.Sprintf("vch%d", i%2)
		sv := int64(i % 2)
		seg := &backuppb.SegmentBackupInfo{
			SegmentId:      int64(i),
			PartitionId:    1,
			VChannel:       vch,
			Size:           1,
			StorageVersion: sv,
		}
		segs = append(segs, seg)
	}

	t.Run("SingleL0InOneJob", func(t *testing.T) {
		ct := newTestCollectionTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Once()
		ct.grpcCli = grpcCli

		batches, err := ct.l0SegmentBatches(segs)
		assert.NoError(t, err)
		assert.Len(t, batches, 10)

		for _, b := range batches {
			require.Len(t, b.partitionDirs, 1)
			for _, dir := range b.partitionDirs {
				require.Empty(t, dir.insertLogDir)
				require.NotEmpty(t, dir.deltaLogDir)
			}
		}
	})

	t.Run("MultiL0InOneJob", func(t *testing.T) {
		ct := newTestCollectionTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(true).Once()
		ct.grpcCli = grpcCli

		batches, err := ct.l0SegmentBatches(segs)
		assert.NoError(t, err)
		assert.Len(t, batches, 2)

		for _, b := range batches {
			require.Len(t, b.partitionDirs, 5)
			for _, dir := range b.partitionDirs {
				require.Empty(t, dir.insertLogDir)
				require.NotEmpty(t, dir.deltaLogDir)
			}
		}
	})
}
