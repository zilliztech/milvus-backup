package l0compact

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// buildSingleInt64Parquet builds a bare single-column Int64 parquet blob,
// mirroring a v1 insert binlog payload (column 0 = the field's values).
func buildSingleInt64Parquet(t *testing.T, vals []int64) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "0", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.DefaultAllocator
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	fb := b.Field(0).(*array.Int64Builder)
	for _, v := range vals {
		fb.Append(v)
	}
	rec := b.NewRecord()
	defer rec.Release()
	blob, err := writeRecord(schema, rec)
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

func TestReadInsertPKV1(t *testing.T) {
	// v1 insert: envelope (Insert event, PayloadDataType=Int64) wrapping a single Int64 column parquet.
	payload := buildSingleInt64Parquet(t, []int64{11, 22}) // helper in test file
	blob, _ := BuildV1Envelope(eventInsert, 5, 100, map[string]string{}, payload)
	pks, err := ReadInsertPK([][]byte{blob}, KindV1, 100, PKInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(pks) != 2 || pks[0].Int != 11 || pks[1].Int != 22 {
		t.Fatalf("got %+v", pks)
	}
}

func TestReadInsertPKV2ByFieldID(t *testing.T) {
	// v2/v3: two group parquets; PK field id 100 lives in the second one.
	g0 := buildTaggedParquetInt64(t, map[int64][]int64{1: {0, 0}}) // system col, no pk
	g1 := buildTaggedParquetInt64(t, map[int64][]int64{100: {5, 6}})
	pks, err := ReadInsertPK([][]byte{g0, g1}, KindV2, 100, PKInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(pks) != 2 || pks[0].Int != 5 || pks[1].Int != 6 {
		t.Fatalf("got %+v", pks)
	}
}
