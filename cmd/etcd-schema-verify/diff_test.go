package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiffCollections(t *testing.T) {
	base := func() *CollectionDump {
		return &CollectionDump{
			ID: 100, DBID: 1, Name: "coll", State: "CollectionCreated",
			ShardsNum: 2, ConsistencyLevel: "Strong",
			Properties: map[string]string{"ttl": "3600"},
			Fields: []FieldDump{
				{FieldID: 1, Name: "pk", DataType: "Int64", IsPrimaryKey: true},
				{FieldID: 2, Name: "vec", DataType: "FloatVector", TypeParams: map[string]string{"dim": "128"}},
			},
			Partitions: []PartitionDump{
				{PartitionID: 10, PartitionName: "_default", State: "PartitionCreated"},
			},
			Indexes: []IndexDump{
				{IndexID: 1, IndexName: "idx_vec", FieldID: 2, State: "Finished"},
			},
			Functions: []FunctionDump{
				{ID: 1, Name: "bm25", Type: "BM25", InputFieldNames: []string{"text"}},
			},
		}
	}

	t.Run("Aligned", func(t *testing.T) {
		diffs := diffCollections(base(), base())
		assert.Empty(t, diffs)
	})

	t.Run("CollectionLevelDiffs", func(t *testing.T) {
		dst := base()
		dst.ID = 101
		dst.ShardsNum = 4
		dst.ConsistencyLevel = "Bounded"
		dst.Properties = map[string]string{"ttl": "7200"}

		diffs := diffCollections(base(), dst)
		dm := toDiffMap(diffs)
		assert.Equal(t, "100", dm["id"].Src)
		assert.Equal(t, "101", dm["id"].Dst)
		assert.Equal(t, "2", dm["shards_num"].Src)
		assert.Equal(t, "4", dm["shards_num"].Dst)
		assert.Equal(t, "Strong", dm["consistency_level"].Src)
		assert.Equal(t, "Bounded", dm["consistency_level"].Dst)
		assert.Equal(t, "3600", dm["properties.ttl"].Src)
		assert.Equal(t, "7200", dm["properties.ttl"].Dst)
	})

	t.Run("DescriptionDiff", func(t *testing.T) {
		src := base()
		src.Description = "old"
		dst := base()
		dst.Description = "new"

		diffs := diffCollections(src, dst)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "description")
	})

	t.Run("DBIDDiff", func(t *testing.T) {
		dst := base()
		dst.DBID = 2

		diffs := diffCollections(base(), dst)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "db_id")
	})
}

func TestDiffFields(t *testing.T) {
	t.Run("MissingSrc", func(t *testing.T) {
		var diffs []Diff
		diffFields(&diffs, nil, []FieldDump{{FieldID: 1, Name: "pk"}})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Src)
		assert.Equal(t, "pk", diffs[0].Dst)
		assert.Contains(t, diffs[0].Path, "id=1")
	})

	t.Run("MissingDst", func(t *testing.T) {
		var diffs []Diff
		diffFields(&diffs, []FieldDump{{FieldID: 1, Name: "pk"}}, nil)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "pk", diffs[0].Src)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("DataTypeDiff", func(t *testing.T) {
		var diffs []Diff
		diffFields(&diffs,
			[]FieldDump{{FieldID: 2, Name: "vec", DataType: "FloatVector"}},
			[]FieldDump{{FieldID: 2, Name: "vec", DataType: "Float16Vector"}},
		)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "data_type")
	})

	t.Run("BoolFieldsDiff", func(t *testing.T) {
		var diffs []Diff
		diffFields(&diffs,
			[]FieldDump{{FieldID: 1, Name: "pk", IsPrimaryKey: true, AutoID: true, IsPartitionKey: false, IsClusteringKey: false, IsFunctionOutput: false, Nullable: false}},
			[]FieldDump{{FieldID: 1, Name: "pk", IsPrimaryKey: false, AutoID: false, IsPartitionKey: true, IsClusteringKey: true, IsFunctionOutput: true, Nullable: true}},
		)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "fields[id=1,name=pk].is_primary_key")
		assert.Contains(t, dm, "fields[id=1,name=pk].auto_id")
		assert.Contains(t, dm, "fields[id=1,name=pk].is_partition_key")
		assert.Contains(t, dm, "fields[id=1,name=pk].is_clustering_key")
		assert.Contains(t, dm, "fields[id=1,name=pk].is_function_output")
		assert.Contains(t, dm, "fields[id=1,name=pk].nullable")
	})

	t.Run("TypeParamsDiff", func(t *testing.T) {
		var diffs []Diff
		diffFields(&diffs,
			[]FieldDump{{FieldID: 2, Name: "vec", TypeParams: map[string]string{"dim": "128"}}},
			[]FieldDump{{FieldID: 2, Name: "vec", TypeParams: map[string]string{"dim": "256"}}},
		)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "type_params.dim")
	})

	t.Run("IndexParamsDiff", func(t *testing.T) {
		var diffs []Diff
		diffFields(&diffs,
			[]FieldDump{{FieldID: 2, Name: "f", IndexParams: map[string]string{"a": "1"}}},
			[]FieldDump{{FieldID: 2, Name: "f", IndexParams: map[string]string{"a": "2"}}},
		)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "index_params.a")
	})

	t.Run("DescriptionAndStateDiff", func(t *testing.T) {
		var diffs []Diff
		diffFields(&diffs,
			[]FieldDump{{FieldID: 1, Name: "f", Description: "old", State: "FieldCreated", DefaultValue: "0", ElementType: "Int64"}},
			[]FieldDump{{FieldID: 1, Name: "f", Description: "new", State: "FieldDropping", DefaultValue: "1", ElementType: "VarChar"}},
		)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "fields[id=1,name=f].description")
		assert.Contains(t, dm, "fields[id=1,name=f].state")
		assert.Contains(t, dm, "fields[id=1,name=f].default_value")
		assert.Contains(t, dm, "fields[id=1,name=f].element_type")
	})

	t.Run("Aligned", func(t *testing.T) {
		f := []FieldDump{{FieldID: 1, Name: "pk", DataType: "Int64"}}
		var diffs []Diff
		diffFields(&diffs, f, f)
		assert.Empty(t, diffs)
	})
}

func TestDiffPartitions(t *testing.T) {
	t.Run("Aligned", func(t *testing.T) {
		p := []PartitionDump{{PartitionID: 1, PartitionName: "_default", State: "PartitionCreated"}}
		var diffs []Diff
		diffPartitions(&diffs, p, p)
		assert.Empty(t, diffs)
	})

	t.Run("MissingSrc", func(t *testing.T) {
		var diffs []Diff
		diffPartitions(&diffs, nil, []PartitionDump{{PartitionID: 1, PartitionName: "p1"}})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Src)
	})

	t.Run("MissingDst", func(t *testing.T) {
		var diffs []Diff
		diffPartitions(&diffs,
			[]PartitionDump{{PartitionID: 2, PartitionName: "part_a", State: "PartitionCreated"}},
			nil,
		)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "part_a", diffs[0].Src)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("NameAndStateDiff", func(t *testing.T) {
		var diffs []Diff
		diffPartitions(&diffs,
			[]PartitionDump{{PartitionID: 1, PartitionName: "p1", State: "PartitionCreated"}},
			[]PartitionDump{{PartitionID: 1, PartitionName: "p2", State: "PartitionDropping"}},
		)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "partitions[id=1,name=p1].partition_name")
		assert.Contains(t, dm, "partitions[id=1,name=p1].state")
	})
}

func TestDiffIndexes(t *testing.T) {
	t.Run("Aligned", func(t *testing.T) {
		idx := []IndexDump{{IndexID: 1, IndexName: "idx", FieldID: 2, State: "Finished"}}
		var diffs []Diff
		diffIndexes(&diffs, idx, idx)
		assert.Empty(t, diffs)
	})

	t.Run("MissingSrc", func(t *testing.T) {
		var diffs []Diff
		diffIndexes(&diffs, nil, []IndexDump{{IndexID: 1, IndexName: "idx"}})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Src)
	})

	t.Run("MissingDst", func(t *testing.T) {
		var diffs []Diff
		diffIndexes(&diffs, []IndexDump{{IndexID: 1, IndexName: "idx"}}, nil)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("AllFieldsDiff", func(t *testing.T) {
		var diffs []Diff
		diffIndexes(&diffs,
			[]IndexDump{{IndexID: 1, IndexName: "a", FieldID: 2, State: "Finished", IsAutoIndex: false, Deleted: false,
				IndexParams: map[string]string{"nlist": "128"}, TypeParams: map[string]string{"t": "1"}, UserIndexParams: map[string]string{"u": "1"}}},
			[]IndexDump{{IndexID: 1, IndexName: "b", FieldID: 3, State: "InProgress", IsAutoIndex: true, Deleted: true,
				IndexParams: map[string]string{"nlist": "256"}, TypeParams: map[string]string{"t": "2"}, UserIndexParams: map[string]string{"u": "2"}}},
		)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "indexes[id=1,name=a].index_name")
		assert.Contains(t, dm, "indexes[id=1,name=a].field_id")
		assert.Contains(t, dm, "indexes[id=1,name=a].state")
		assert.Contains(t, dm, "indexes[id=1,name=a].is_auto_index")
		assert.Contains(t, dm, "indexes[id=1,name=a].deleted")
		assert.Contains(t, dm, "indexes[id=1,name=a].index_params.nlist")
		assert.Contains(t, dm, "indexes[id=1,name=a].type_params.t")
		assert.Contains(t, dm, "indexes[id=1,name=a].user_index_params.u")
	})
}

func TestDiffFunctions(t *testing.T) {
	t.Run("Aligned", func(t *testing.T) {
		f := []FunctionDump{{ID: 1, Name: "bm25", Type: "BM25",
			InputFieldNames: []string{"text"}, OutputFieldNames: []string{"sparse"},
			InputFieldIDs: []int64{3}, OutputFieldIDs: []int64{4},
			Params: map[string]string{"k": "v"},
		}}
		var diffs []Diff
		diffFunctions(&diffs, f, f)
		assert.Empty(t, diffs)
	})

	t.Run("MissingSrc", func(t *testing.T) {
		var diffs []Diff
		diffFunctions(&diffs, nil, []FunctionDump{{ID: 1, Name: "fn"}})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Src)
	})

	t.Run("MissingDst", func(t *testing.T) {
		var diffs []Diff
		diffFunctions(&diffs, []FunctionDump{{ID: 1, Name: "fn"}}, nil)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("AllFieldsDiff", func(t *testing.T) {
		var diffs []Diff
		diffFunctions(&diffs,
			[]FunctionDump{{ID: 1, Name: "fn", Description: "a", Type: "BM25",
				InputFieldIDs: []int64{3}, InputFieldNames: []string{"text"},
				OutputFieldIDs: []int64{4}, OutputFieldNames: []string{"sparse"},
				Params: map[string]string{"k": "v1"}}},
			[]FunctionDump{{ID: 1, Name: "fn2", Description: "b", Type: "Embed",
				InputFieldIDs: []int64{5}, InputFieldNames: []string{"doc"},
				OutputFieldIDs: []int64{6}, OutputFieldNames: []string{"dense"},
				Params: map[string]string{"k": "v2"}}},
		)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "functions[id=1,name=fn].name")
		assert.Contains(t, dm, "functions[id=1,name=fn].description")
		assert.Contains(t, dm, "functions[id=1,name=fn].type")
		assert.Contains(t, dm, "functions[id=1,name=fn].input_field_ids")
		assert.Contains(t, dm, "functions[id=1,name=fn].input_field_names")
		assert.Contains(t, dm, "functions[id=1,name=fn].output_field_ids")
		assert.Contains(t, dm, "functions[id=1,name=fn].output_field_names")
		assert.Contains(t, dm, "functions[id=1,name=fn].params.k")
	})
}

func TestDiffMap(t *testing.T) {
	t.Run("BothNil", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "p", nil, nil)
		assert.Empty(t, diffs)
	})

	t.Run("SrcOnly", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "p", map[string]string{"a": "1"}, nil)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "1", diffs[0].Src)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("DstOnly", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "p", nil, map[string]string{"a": "1"})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Src)
	})

	t.Run("ValueDiff", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "p", map[string]string{"k": "v1"}, map[string]string{"k": "v2"})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "p.k", diffs[0].Path)
	})

	t.Run("Equal", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "p", map[string]string{"k": "v"}, map[string]string{"k": "v"})
		assert.Empty(t, diffs)
	})
}

func TestDiffStrSlice(t *testing.T) {
	t.Run("Equal", func(t *testing.T) {
		var diffs []Diff
		diffStrSlice(&diffs, "f", []string{"a", "b"}, []string{"a", "b"})
		assert.Empty(t, diffs)
	})

	t.Run("DifferentLength", func(t *testing.T) {
		var diffs []Diff
		diffStrSlice(&diffs, "f", []string{"a"}, []string{"a", "b"})
		assert.Len(t, diffs, 1)
	})

	t.Run("DifferentValues", func(t *testing.T) {
		var diffs []Diff
		diffStrSlice(&diffs, "f", []string{"a", "b"}, []string{"a", "c"})
		assert.Len(t, diffs, 1)
	})

	t.Run("CommaEdgeCase", func(t *testing.T) {
		// ["a,b"] and ["a", "b"] must be treated as different
		var diffs []Diff
		diffStrSlice(&diffs, "f", []string{"a,b"}, []string{"a", "b"})
		assert.Len(t, diffs, 1)
	})

	t.Run("BothNil", func(t *testing.T) {
		var diffs []Diff
		diffStrSlice(&diffs, "f", nil, nil)
		assert.Empty(t, diffs)
	})
}

func TestDiffInt64Slice(t *testing.T) {
	t.Run("Equal", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", []int64{1, 2}, []int64{1, 2})
		assert.Empty(t, diffs)
	})

	t.Run("DifferentLength", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", []int64{1}, []int64{1, 2})
		assert.Len(t, diffs, 1)
	})

	t.Run("DifferentValues", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", []int64{1, 2}, []int64{1, 3})
		assert.Len(t, diffs, 1)
	})

	t.Run("BothNil", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", nil, nil)
		assert.Empty(t, diffs)
	})
}

func TestCollectIDs(t *testing.T) {
	t.Run("MergedAndSorted", func(t *testing.T) {
		ids := collectIDs(map[int64]string{3: "c", 1: "a"}, map[int64]string{2: "b", 1: "a"})
		assert.Equal(t, []int64{1, 2, 3}, ids)
	})

	t.Run("Empty", func(t *testing.T) {
		ids := collectIDs(map[int64]string{}, map[int64]string{})
		assert.Empty(t, ids)
	})
}

func toDiffMap(diffs []Diff) map[string]Diff {
	m := make(map[string]Diff, len(diffs))
	for _, d := range diffs {
		m[d.Path] = d
	}
	return m
}
