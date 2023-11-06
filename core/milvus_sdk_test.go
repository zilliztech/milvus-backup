package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zilliztech/milvus-backup/internal/log"

	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProxyClient(t *testing.T) {
	ctx := context.Background()
	milvusAddr := "localhost:19530"
	//c, err := proxy.NewClient(context, milvusAddr)
	//assert.NoError(t, err)

	c2, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)
	collections, err := c2.ListCollections(ctx)
	for _, coll := range collections {
		log.Info("collections", zap.Any("coll", coll.Name), zap.Int64("id", coll.ID))
	}

	//coll, err := c2.DescribeCollection(ctx, "hello_milvus")
	//log.Info("collection", zap.Any("hello_milvus", coll))

	//c2.DropCollection(ctx, "demo_bulkinsert")
	//c2.DropCollection(ctx, "hello_milvus")
	//c2.DropCollection(ctx, "hello_milvus_recover")

	//idCol, randomCol, embeddingCol = "ID", "random", "embeddings"
	pks := entity.NewColumnInt64("ID", []int64{0, 1})
	res, err := c2.QueryByPks(ctx, "hello_milvus_recover", nil, pks, []string{"random"}, gomilvus.WithSearchQueryConsistencyLevel(entity.ClStrong))
	log.Info("query result", zap.Any("query result", res))

	//assert.Empty(t, collections)
}

func TestCreateCollection(t *testing.T) {
	ctx := context.Background()
	milvusAddr := "local:19530"
	//c, err := proxy.NewClient(context, milvusAddr)
	//assert.NoError(t, err)

	client, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)

	//MILVUS_DATA_PATH := "/tmp/milvus/data/"

	_COLLECTION_NAME := "create_demo"
	_ID_FIELD_NAME := "id_field"
	_VECTOR_FIELD_NAME := "float_vector_field"
	_STR_FIELD_NAME := "str_field"
	_ARRAY_FIELD_NAME := "arr_field"

	// String field parameter
	_MAX_LENGTH := "65535"
	_ARRAY_MAX_CAPACITY := "100"
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
	field4 := &entity.Field{
		Name:        _ARRAY_FIELD_NAME,
		DataType:    entity.FieldTypeArray,
		Description: "arr",
		ElementType: entity.FieldTypeInt64,
		TypeParams: map[string]string{
			entity.TypeParamMaxCapacity: _ARRAY_MAX_CAPACITY,
		},
	}
	schema := &entity.Schema{
		CollectionName: _COLLECTION_NAME,
		Description:    "demo bulkinsert",
		AutoID:         true,
		Fields:         []*entity.Field{field1, field2, field3, field4},
	}
	//client.DropCollection(ctx, _COLLECTION_NAME)

	err = client.CreateCollection(ctx, schema, 2)
	println(err)
	_PART_1 := "part_1"
	client.CreatePartition(ctx, _COLLECTION_NAME, _PART_1)

	//client.DropCollection(ctx, _COLLECTION_NAME)
}

func TestBulkInsert(t *testing.T) {
	ctx := context.Background()
	milvusAddr := "localhost:19530"
	//c, err := proxy.NewClient(context, milvusAddr)
	//assert.NoError(t, err)

	client, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)

	//MILVUS_DATA_PATH := "/tmp/milvus/data/"

	_COLLECTION_NAME := "demo_bulk_insert"
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
		Description:    "demo bulkinsert",
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

	id, err := client.BulkInsert(ctx, _COLLECTION_NAME, _PART_1, file_names)

	log.Info("bulk insert task ids", zap.Int64("id", id))

	for {
		state, _ := client.GetBulkInsertState(ctx, id)
		log.Info("bulk insert task state", zap.Any("state", state))
		time.Sleep(3 * time.Second)
	}
	//time.Sleep(30 * time.Second)

	client.DropCollection(ctx, _COLLECTION_NAME)
}

func TestGetIndex(t *testing.T) {
	ctx := context.Background()
	milvusAddr := "localhost:19530"
	c, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)

	coll, err := c.DescribeCollection(ctx, "hello_milvus_recover")

	//log.Info("coll", zap.Any("coll", coll))
	fmt.Println(coll.Schema.Fields[0])
	fmt.Println(coll.Schema.Fields[1])
	fmt.Println(coll.Schema.Fields[2])

	index, err := c.DescribeIndex(ctx, "hello_milvus_recover", "embeddings")
	fmt.Println(index)

	indexState, err := c.GetIndexState(ctx, "hello_milvus", "embeddings")
	fmt.Println(indexState)
	progress, err := c.GetLoadingProgress(ctx, "hello_milvus_recover", []string{})
	fmt.Println(progress)

	loadState, err := c.GetLoadState(ctx, "hello_milvus_recover", []string{})
	fmt.Println(loadState)

}

func TestCreateIndex(t *testing.T) {
	ctx := context.Background()
	milvusAddr := "localhost:19530"
	client, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)

	_COLLECTION_NAME := "demo_bulk_insert2"
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
		Description:    "demo bulkinsert",
		AutoID:         true,
		Fields:         []*entity.Field{field1, field2, field3},
	}
	//client.DropCollection(ctx, _COLLECTION_NAME)
	fmt.Println(schema)
	client.CreateCollection(ctx, schema, 2)

	idx := entity.NewScalarIndex()
	err = client.CreateIndex(ctx, _COLLECTION_NAME, _STR_FIELD_NAME, idx, false, gomilvus.WithIndexName("_default_idx_102"))
	fmt.Println(err)

	idx2, _ := entity.NewIndexHNSW(entity.L2, 8, 96)
	//err = client.DropIndex(ctx, _COLLECTION_NAME, _VECTOR_FIELD_NAME, gomilvus.WithIndexName("_default_idx_101"))
	//fmt.Println(err)
	err = client.CreateIndex(ctx, _COLLECTION_NAME, _VECTOR_FIELD_NAME, idx2, false, gomilvus.WithIndexName("_default_idx_102"))

	fmt.Println(err)
}

func TestDescribeIndex(t *testing.T) {
	ctx := context.Background()
	milvusAddr := "localhost:19530"
	client, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)

	_COLLECTION_NAME := "demo_bulk_insert2"
	_STR_FIELD_NAME := "str_field"
	_VECTOR_FIELD_NAME := "float_vector_field"

	idxs, err := client.DescribeIndex(ctx, _COLLECTION_NAME, _STR_FIELD_NAME)
	fmt.Println(idxs)
	fmt.Println(err)
	idxs2, err := client.DescribeIndex(ctx, _COLLECTION_NAME, _VECTOR_FIELD_NAME)
	fmt.Println(idxs2)
	fmt.Println(err)
}

func TestCleanAll(t *testing.T) {
	ctx := context.Background()
	milvusAddr := "10.102.9.64:19530"

	c2, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)

	dbs, err := c2.ListDatabases(ctx)

	for _, db := range dbs {
		c2.UsingDatabase(ctx, db.Name)
		collections, _ := c2.ListCollections(ctx)
		for _, coll := range collections {
			c2.DropCollection(ctx, coll.Name)
			log.Info("collections", zap.Any("coll", coll.Name), zap.Int64("id", coll.ID))
		}
		c2.DropDatabase(ctx, db.Name)
	}
}
