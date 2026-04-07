package main

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/stretchr/testify/assert"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

// mockKV returns pre-configured responses keyed by prefix match.
type mockKV struct {
	data map[string][]*mvccpb.KeyValue
}

func newMockKV() *mockKV {
	return &mockKV{data: make(map[string][]*mvccpb.KeyValue)}
}

func (m *mockKV) put(key string, msg proto.Message) {
	val, _ := proto.Marshal(msg)
	m.data[key] = append(m.data[key], &mvccpb.KeyValue{Key: []byte(key), Value: val})
}

func (m *mockKV) Get(_ context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	op := clientv3.OpGet(key, opts...)
	var kvs []*mvccpb.KeyValue
	if op.IsOptsWithPrefix() {
		for k, vs := range m.data {
			if len(k) >= len(key) && k[:len(key)] == key {
				kvs = append(kvs, vs...)
			}
		}
	} else {
		kvs = m.data[key]
	}
	return &clientv3.GetResponse{Kvs: kvs}, nil
}

// setupTestEtcd populates a mockKV with a full collection's metadata.
func setupTestEtcd() (*mockKV, *reader) {
	kv := newMockKV()

	kv.put("test-root/meta/root-coord/database/db-info/1", &etcdpb.DatabaseInfo{
		Id: 1, Name: "default", State: etcdpb.DatabaseState_DatabaseCreated,
	})

	kv.put("test-root/meta/root-coord/database/collection-info/1/100", &etcdpb.CollectionInfo{
		ID: 100, DbId: 1, ShardsNum: 2,
		Schema: &schemapb.CollectionSchema{
			Name: "test_coll",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			},
		},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})

	kv.put("test-root/meta/root-coord/fields/100/1", &schemapb.FieldSchema{
		FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64,
		IsPrimaryKey: true, State: schemapb.FieldState_FieldCreated,
	})
	kv.put("test-root/meta/root-coord/fields/100/2", &schemapb.FieldSchema{
		FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector,
		State: schemapb.FieldState_FieldCreated,
	})

	kv.put("test-root/meta/root-coord/partitions/100/10", &etcdpb.PartitionInfo{
		PartitionID: 10, PartitionName: "_default",
		State: etcdpb.PartitionState_PartitionCreated, CollectionId: 100,
	})
	kv.put("test-root/meta/root-coord/partitions/100/11", &etcdpb.PartitionInfo{
		PartitionID: 11, PartitionName: "part_a",
		State: etcdpb.PartitionState_PartitionCreated, CollectionId: 100,
	})

	kv.put("test-root/meta/field-index/100/1", &indexpb.FieldIndex{
		IndexInfo: &indexpb.IndexInfo{
			CollectionID: 100, FieldID: 2, IndexID: 1, IndexName: "idx_vec",
			IndexParams: []*commonpb.KeyValuePair{
				{Key: "index_type", Value: "IVF_FLAT"}, {Key: "nlist", Value: "128"},
			},
			State: commonpb.IndexState_Finished,
		},
	})

	kv.put("test-root/meta/root-coord/functions/100/1", &schemapb.FunctionSchema{
		Id: 1, Name: "bm25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{3}, InputFieldNames: []string{"text"},
		OutputFieldIds: []int64{4}, OutputFieldNames: []string{"sparse"},
	})

	return kv, &reader{cli: kv, rootPath: "test-root"}
}

func TestKvPairsToMap(t *testing.T) {
	t.Run("Nil", func(t *testing.T) {
		assert.Nil(t, kvPairsToMap(nil))
	})

	t.Run("Empty", func(t *testing.T) {
		assert.Nil(t, kvPairsToMap([]*commonpb.KeyValuePair{}))
	})

	t.Run("MultipleEntries", func(t *testing.T) {
		pairs := []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}, {Key: "metric", Value: "L2"}}
		m := kvPairsToMap(pairs)
		assert.Equal(t, "128", m["dim"])
		assert.Equal(t, "L2", m["metric"])
	})
}

func TestBuildFieldDump(t *testing.T) {
	t.Run("BasicField", func(t *testing.T) {
		fd := buildFieldDump(&schemapb.FieldSchema{
			FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64,
			IsPrimaryKey: true, AutoID: true, State: schemapb.FieldState_FieldCreated,
		})
		assert.Equal(t, int64(100), fd.FieldID)
		assert.Equal(t, "Int64", fd.DataType)
		assert.True(t, fd.IsPrimaryKey)
		assert.True(t, fd.AutoID)
		assert.Equal(t, "FieldCreated", fd.State)
		assert.Empty(t, fd.ElementType)
	})

	t.Run("ArrayWithElementType", func(t *testing.T) {
		fd := buildFieldDump(&schemapb.FieldSchema{
			FieldID: 102, Name: "tags",
			DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar,
		})
		assert.Equal(t, "VarChar", fd.ElementType)
	})

	t.Run("WithDefaultValue", func(t *testing.T) {
		fd := buildFieldDump(&schemapb.FieldSchema{
			FieldID: 106, Name: "status", DataType: schemapb.DataType_Int64,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 0}},
		})
		assert.NotEmpty(t, fd.DefaultValue)
	})

	t.Run("AllBoolFlags", func(t *testing.T) {
		fd := buildFieldDump(&schemapb.FieldSchema{
			FieldID: 107, Name: "f", DataType: schemapb.DataType_VarChar,
			IsPartitionKey: true, IsClusteringKey: true, IsFunctionOutput: true, Nullable: true,
		})
		assert.True(t, fd.IsPartitionKey)
		assert.True(t, fd.IsClusteringKey)
		assert.True(t, fd.IsFunctionOutput)
		assert.True(t, fd.Nullable)
	})
}

func TestMergeFields(t *testing.T) {
	t.Run("UpdateState", func(t *testing.T) {
		d := &CollectionDump{Fields: []FieldDump{{FieldID: 1, Name: "pk", State: ""}}}
		mergeFields(d, []*schemapb.FieldSchema{{FieldID: 1, State: schemapb.FieldState_FieldCreated}})
		assert.Equal(t, "FieldCreated", d.Fields[0].State)
	})

	t.Run("AddNewField", func(t *testing.T) {
		d := &CollectionDump{Fields: []FieldDump{{FieldID: 1, Name: "pk"}}}
		mergeFields(d, []*schemapb.FieldSchema{{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector}})
		assert.Len(t, d.Fields, 2)
		assert.Equal(t, "vec", d.Fields[1].Name)
	})

	t.Run("EmptyBoth", func(t *testing.T) {
		d := &CollectionDump{}
		mergeFields(d, nil)
		assert.Empty(t, d.Fields)
	})
}

func TestReaderPrefix(t *testing.T) {
	r := &reader{rootPath: "by-dev"}

	t.Run("RootcoordPath", func(t *testing.T) {
		assert.Equal(t, "by-dev/meta/root-coord/database/db-info", r.prefix("meta", "root-coord", "database", "db-info"))
	})

	t.Run("IndexPath", func(t *testing.T) {
		assert.Equal(t, "by-dev/meta/field-index/100", r.prefix("meta", "field-index", "100"))
	})
}

func TestListDatabases(t *testing.T) {
	t.Run("Found", func(t *testing.T) {
		_, r := setupTestEtcd()
		dbs, err := r.listDatabases(context.Background())
		assert.NoError(t, err)
		assert.Len(t, dbs, 1)
		assert.Equal(t, "default", dbs[0].GetName())
	})

	t.Run("FallbackDefault", func(t *testing.T) {
		r := &reader{cli: newMockKV(), rootPath: "empty"}
		dbs, err := r.listDatabases(context.Background())
		assert.NoError(t, err)
		assert.Len(t, dbs, 1)
		assert.Equal(t, int64(0), dbs[0].GetId())
		assert.Equal(t, "default", dbs[0].GetName())
	})
}

func TestListCollections(t *testing.T) {
	t.Run("V25Path", func(t *testing.T) {
		_, r := setupTestEtcd()
		colls, err := r.listCollections(context.Background(), 1)
		assert.NoError(t, err)
		assert.Len(t, colls, 1)
		assert.Equal(t, "test_coll", colls[0].GetSchema().GetName())
	})

	t.Run("LegacyFallback", func(t *testing.T) {
		kv := newMockKV()
		kv.put("r/meta/root-coord/collection/100", &etcdpb.CollectionInfo{
			ID: 100, Schema: &schemapb.CollectionSchema{Name: "legacy_coll"},
		})
		r := &reader{cli: kv, rootPath: "r"}
		colls, err := r.listCollections(context.Background(), 0)
		assert.NoError(t, err)
		assert.Len(t, colls, 1)
		assert.Equal(t, "legacy_coll", colls[0].GetSchema().GetName())
	})

	t.Run("NoFallbackForNonZeroDB", func(t *testing.T) {
		r := &reader{cli: newMockKV(), rootPath: "empty"}
		colls, err := r.listCollections(context.Background(), 99)
		assert.NoError(t, err)
		assert.Empty(t, colls)
	})
}

func TestListFields(t *testing.T) {
	_, r := setupTestEtcd()
	fields, err := r.listFields(context.Background(), 100)
	assert.NoError(t, err)
	assert.Len(t, fields, 2)
}

func TestListPartitions(t *testing.T) {
	_, r := setupTestEtcd()
	parts, err := r.listPartitions(context.Background(), 100)
	assert.NoError(t, err)
	assert.Len(t, parts, 2)
}

func TestListIndexes(t *testing.T) {
	_, r := setupTestEtcd()
	indexes, err := r.listIndexes(context.Background(), 100)
	assert.NoError(t, err)
	assert.Len(t, indexes, 1)
	assert.Equal(t, "idx_vec", indexes[0].IndexName)
	assert.Equal(t, "IVF_FLAT", indexes[0].IndexParams["index_type"])
}

func TestListFunctions(t *testing.T) {
	_, r := setupTestEtcd()
	funcs, err := r.listFunctions(context.Background(), 100)
	assert.NoError(t, err)
	assert.Len(t, funcs, 1)
	assert.Equal(t, "bm25", funcs[0].Name)
	assert.Equal(t, []int64{3}, funcs[0].InputFieldIDs)
}

func TestLoadCollectionDetail(t *testing.T) {
	_, r := setupTestEtcd()
	coll := &etcdpb.CollectionInfo{
		ID: 100, DbId: 1, ShardsNum: 2,
		Schema: &schemapb.CollectionSchema{
			Name: "test_coll",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	}

	t.Run("FullDump", func(t *testing.T) {
		dump, err := r.loadCollectionDetail(context.Background(), coll)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), dump.ID)
		assert.Equal(t, "test_coll", dump.Name)
		assert.Equal(t, int32(2), dump.ShardsNum)
		assert.Len(t, dump.Fields, 2)
		assert.Equal(t, "FieldCreated", dump.Fields[0].State)
		assert.Len(t, dump.Partitions, 2)
		assert.Len(t, dump.Indexes, 1)
		assert.Len(t, dump.Functions, 1)
	})

	t.Run("SortedByID", func(t *testing.T) {
		dump, err := r.loadCollectionDetail(context.Background(), coll)
		assert.NoError(t, err)
		assert.True(t, dump.Fields[0].FieldID < dump.Fields[1].FieldID)
		assert.True(t, dump.Partitions[0].PartitionID < dump.Partitions[1].PartitionID)
	})
}

func TestFindCollection(t *testing.T) {
	_, r := setupTestEtcd()

	t.Run("Found", func(t *testing.T) {
		dump, err := r.findCollection(context.Background(), "", "test_coll")
		assert.NoError(t, err)
		assert.Equal(t, int64(100), dump.ID)
	})

	t.Run("NotFound", func(t *testing.T) {
		_, err := r.findCollection(context.Background(), "", "nonexistent")
		assert.ErrorContains(t, err, "not found")
	})

	t.Run("FilterByDBNoMatch", func(t *testing.T) {
		_, err := r.findCollection(context.Background(), "other_db", "test_coll")
		assert.ErrorContains(t, err, "not found")
	})

	t.Run("FilterByDBMatch", func(t *testing.T) {
		dump, err := r.findCollection(context.Background(), "default", "test_coll")
		assert.NoError(t, err)
		assert.Equal(t, "test_coll", dump.Name)
	})

	t.Run("MultipleDBsAmbiguous", func(t *testing.T) {
		kv := newMockKV()
		kv.put("r/meta/root-coord/database/db-info/1", &etcdpb.DatabaseInfo{Id: 1, Name: "db1", State: etcdpb.DatabaseState_DatabaseCreated})
		kv.put("r/meta/root-coord/database/db-info/2", &etcdpb.DatabaseInfo{Id: 2, Name: "db2", State: etcdpb.DatabaseState_DatabaseCreated})
		kv.put("r/meta/root-coord/database/collection-info/1/100", &etcdpb.CollectionInfo{
			ID: 100, DbId: 1, Schema: &schemapb.CollectionSchema{Name: "dup"},
		})
		kv.put("r/meta/root-coord/database/collection-info/2/200", &etcdpb.CollectionInfo{
			ID: 200, DbId: 2, Schema: &schemapb.CollectionSchema{Name: "dup"},
		})
		r := &reader{cli: kv, rootPath: "r"}

		_, err := r.findCollection(context.Background(), "", "dup")
		assert.ErrorContains(t, err, "multiple databases")
	})

	t.Run("MultipleDBsResolved", func(t *testing.T) {
		kv := newMockKV()
		kv.put("r/meta/root-coord/database/db-info/1", &etcdpb.DatabaseInfo{Id: 1, Name: "db1", State: etcdpb.DatabaseState_DatabaseCreated})
		kv.put("r/meta/root-coord/database/db-info/2", &etcdpb.DatabaseInfo{Id: 2, Name: "db2", State: etcdpb.DatabaseState_DatabaseCreated})
		kv.put("r/meta/root-coord/database/collection-info/1/100", &etcdpb.CollectionInfo{
			ID: 100, DbId: 1, Schema: &schemapb.CollectionSchema{Name: "dup"},
		})
		kv.put("r/meta/root-coord/database/collection-info/2/200", &etcdpb.CollectionInfo{
			ID: 200, DbId: 2, Schema: &schemapb.CollectionSchema{Name: "dup"},
		})
		r := &reader{cli: kv, rootPath: "r"}

		dump, err := r.findCollection(context.Background(), "db2", "dup")
		assert.NoError(t, err)
		assert.Equal(t, int64(200), dump.ID)
	})
}
