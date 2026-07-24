package l0compact

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTaggedParquetInt64 builds a parquet blob with one Int64 column per
// entry in cols, each tagged with PARQUET:field_id = the map key.
func buildTaggedParquetInt64(t *testing.T, cols map[int64][]int64) []byte {
	t.Helper()

	var fields []arrow.Field
	var fieldIDs []int64
	for fid := range cols {
		fields = append(fields, arrow.Field{
			Name:     fmt.Sprint(fid),
			Type:     arrow.PrimitiveTypes.Int64,
			Metadata: fieldIDMeta(fid),
		})
		fieldIDs = append(fieldIDs, fid)
	}
	schema := arrow.NewSchema(fields, nil)

	mem := memory.DefaultAllocator
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i, fid := range fieldIDs {
		fb := b.Field(i).(*array.Int64Builder)
		for _, v := range cols[fid] {
			fb.Append(v)
		}
	}
	rec := b.NewRecord()
	defer rec.Release()

	blob, err := writeRecord(schema, rec)
	require.NoError(t, err)
	return blob
}

func TestParquet2ColRoundTripInt64(t *testing.T) {
	pks := []PrimaryKey{{Type: PKInt64, Int: 1}, {Type: PKInt64, Int: 2}}
	tss := []uint64{10, 20}
	blob, err := WriteParquetPKTs(pks, tss, PKInt64)
	require.NoError(t, err)
	gotPKs, gotTs, err := ReadParquetPKTs(blob, PKInt64)
	require.NoError(t, err)
	assert.Equal(t, pks, gotPKs)
	assert.Equal(t, tss, gotTs)
}

func TestParquetColumnByFieldID(t *testing.T) {
	// A parquet with two field-id-tagged columns; PK field id = 100.
	blob := buildTaggedParquetInt64(t, map[int64][]int64{100: {5, 6, 7}, 1: {1, 1, 1}})
	pks, found, err := ReadParquetColumnByFieldID(blob, 100, PKInt64)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []PrimaryKey{
		{Type: PKInt64, Int: 5},
		{Type: PKInt64, Int: 6},
		{Type: PKInt64, Int: 7},
	}, pks)

	_, found, err = ReadParquetColumnByFieldID(blob, 999, PKInt64)
	assert.NoError(t, err)
	assert.False(t, found, "field 999 should not be found")
}
