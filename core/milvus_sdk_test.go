package core

import (
	"context"
	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"github.com/zilliztech/milvus-backup/internal/util/typeutil"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestProxyClient(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	ctx := context.Background()
	var Params paramtable.GrpcServerConfig
	Params.InitOnce(typeutil.ProxyRole)
	milvusAddr := Params.GetAddress()
	//c, err := proxy.NewClient(context, milvusAddr)
	//assert.NoError(t, err)

	c2, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)
	collections, err := c2.ListCollections(ctx)
	log.Info("collections", zap.Any("colls", collections))
	coll, err := c2.DescribeCollection(ctx, "hello_milvus")
	log.Info("collection", zap.Any("hello_milvus", coll))

	c2.DropCollection(ctx, "demo_bulkload")
	c2.DropCollection(ctx, "hello_milvus")

	//assert.NotEmpty(t, collections)
}

func TestBulkload(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	ctx := context.Background()
	var Params paramtable.GrpcServerConfig
	Params.InitOnce(typeutil.ProxyRole)
	milvusAddr := Params.GetAddress()
	//c, err := proxy.NewClient(context, milvusAddr)
	//assert.NoError(t, err)

	client, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)

	//MILVUS_DATA_PATH := "/tmp/milvus/data/"

	_COLLECTION_NAME := "demo_bulkload"
	_ID_FIELD_NAME := "id_field"
	_VECTOR_FIELD_NAME := "float_vector_field"
	_STR_FIELD_NAME := "str_field"

	// String field parameter
	_MAX_LENGTH := "65535"

	// Vector field parameter
	_DIM := "8"

	//field1 = FieldSchema(name=_ID_FIELD_NAME, dtype=DataType.INT64, description="int64", is_primary=True, auto_id=True)
	//field2 = FieldSchema(name=_VECTOR_FIELD_NAME, dtype=DataType.FLOAT_VECTOR, description="float vector", dim=_DIM,
	//	is_primary=False)
	//field3 = FieldSchema(name=_STR_FIELD_NAME, dtype=DataType.VARCHAR, description="string",
	//	max_length=_MAX_LENGTH, is_primary=False)
	//schema = CollectionSchema(fields=[field1, field2, field3], description="collection description")
	//collection = Collection(name=_COLLECTION_NAME, data=None, schema=schema)

	field1 := &entity.Field{
		Name:        _ID_FIELD_NAME,
		DataType:    entity.FieldTypeInt64,
		Description: "int64",
		PrimaryKey:  true,
		AutoID:      true,
	}
	field2 := &entity.Field{
		Name:        _VECTOR_FIELD_NAME,
		DataType:    entity.FieldTypeFloatVector,
		Description: "float vector",
		TypeParams: map[string]string{
			entity.TypeParamDim: _DIM,
		},
		PrimaryKey: false,
	}
	field3 := &entity.Field{
		Name:        _STR_FIELD_NAME,
		DataType:    entity.FieldTypeVarChar,
		Description: "string",
		PrimaryKey:  false,
		TypeParams: map[string]string{
			entity.TypeParamMaxLength: _MAX_LENGTH,
		},
	}
	schema := &entity.Schema{
		CollectionName: _COLLECTION_NAME,
		Description:    "demo bulkload",
		AutoID:         true,
		Fields:         []*entity.Field{field1, field2, field3},
	}
	client.DropCollection(ctx, _COLLECTION_NAME)

	client.CreateCollection(ctx, schema, 2)
	_PART_1 := "part_1"
	client.CreatePartition(ctx, _COLLECTION_NAME, _PART_1)
	file_names := make([]string, 0)
	file_names = append(file_names, "rows_0.json")
	file_names = append(file_names, "rows_1.json")
	file_names = append(file_names, "rows_2.json")

	ids, err := client.Bulkload(ctx, _COLLECTION_NAME, _PART_1, true, file_names, nil)

	log.Info("bulkload task ids", zap.Int64s("ids", ids))

	for {
		state, _ := client.GetBulkloadState(ctx, ids[0])
		log.Info("bulkload task state", zap.Any("state", state))
		time.Sleep(3 * time.Second)
	}
	//time.Sleep(30 * time.Second)

	client.DropCollection(ctx, _COLLECTION_NAME)
}
