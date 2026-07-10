package l0compact

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

const fieldIDMetaKey = "PARQUET:field_id"

func fieldIDMeta(fid int64) arrow.Metadata {
	return arrow.NewMetadata([]string{fieldIDMetaKey}, []string{strconv.FormatInt(fid, 10)})
}

func pkArrowType(t PKType) arrow.DataType {
	if t == PKInt64 {
		return arrow.PrimitiveTypes.Int64
	}
	return arrow.BinaryTypes.String
}

// WriteParquetPKTs writes a 2-column parquet {field_id 0 = pk, field_id 1 = ts(int64)}.
// This is the v2 deltalog on-disk format.
func WriteParquetPKTs(pks []PrimaryKey, tss []uint64, t PKType) ([]byte, error) {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "0", Type: pkArrowType(t), Metadata: fieldIDMeta(0)},
		{Name: "1", Type: arrow.PrimitiveTypes.Int64, Metadata: fieldIDMeta(1)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	if t == PKInt64 {
		pb := b.Field(0).(*array.Int64Builder)
		for _, pk := range pks {
			pb.Append(pk.Int)
		}
	} else {
		pb := b.Field(0).(*array.StringBuilder)
		for _, pk := range pks {
			pb.Append(pk.Str)
		}
	}
	tb := b.Field(1).(*array.Int64Builder)
	for _, ts := range tss {
		tb.Append(int64(ts))
	}
	rec := b.NewRecord()
	defer rec.Release()
	return writeRecord(schema, rec)
}

func writeRecord(schema *arrow.Schema, rec arrow.Record) ([]byte, error) {
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf,
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(false)),
		pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("l0compact: new parquet writer: %w", err)
	}
	if err := w.Write(rec); err != nil {
		return nil, fmt.Errorf("l0compact: write parquet: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("l0compact: close parquet: %w", err)
	}
	return buf.Bytes(), nil
}

// ReadParquetPKTs reads a 2-column {pk, ts} parquet (v2 deltalog): pk=col0, ts=col1.
func ReadParquetPKTs(blob []byte, t PKType) ([]PrimaryKey, []uint64, error) {
	tbl, rel, err := readTable(blob)
	if err != nil {
		return nil, nil, err
	}
	defer rel()
	pks, err := columnToPKs(tbl.Column(0), t)
	if err != nil {
		return nil, nil, err
	}
	tss, err := columnToUint64(tbl.Column(1))
	if err != nil {
		return nil, nil, err
	}
	return pks, tss, nil
}

// ReadParquetColumnByFieldID finds the column whose PARQUET:field_id == fieldID
// and returns it as PKs. found=false if no such column (caller tries next file).
func ReadParquetColumnByFieldID(blob []byte, fieldID int64, t PKType) ([]PrimaryKey, bool, error) {
	tbl, rel, err := readTable(blob)
	if err != nil {
		return nil, false, err
	}
	defer rel()
	want := strconv.FormatInt(fieldID, 10)
	for i := 0; i < int(tbl.NumCols()); i++ {
		f := tbl.Schema().Field(i)
		if v, ok := f.Metadata.GetValue(fieldIDMetaKey); ok && v == want {
			pks, err := columnToPKs(tbl.Column(i), t)
			return pks, err == nil, err
		}
		// fallback: column name equals field-id string (milvus useFieldID=true)
		if f.Name == want {
			pks, err := columnToPKs(tbl.Column(i), t)
			return pks, err == nil, err
		}
	}
	return nil, false, nil
}

func readTable(blob []byte) (arrow.Table, func(), error) {
	pf, err := file.NewParquetReader(bytes.NewReader(blob))
	if err != nil {
		return nil, nil, fmt.Errorf("l0compact: open parquet: %w", err)
	}
	r, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		pf.Close()
		return nil, nil, fmt.Errorf("l0compact: arrow reader: %w", err)
	}
	tbl, err := r.ReadTable(context.Background())
	if err != nil {
		pf.Close()
		return nil, nil, fmt.Errorf("l0compact: read table: %w", err)
	}
	return tbl, func() { tbl.Release(); pf.Close() }, nil
}

func columnToPKs(col *arrow.Column, t PKType) ([]PrimaryKey, error) {
	var out []PrimaryKey
	for _, chunk := range col.Data().Chunks() {
		switch t {
		case PKInt64:
			a := chunk.(*array.Int64)
			for i := 0; i < a.Len(); i++ {
				out = append(out, PrimaryKey{Type: PKInt64, Int: a.Value(i)})
			}
		case PKVarChar:
			a := chunk.(*array.String)
			for i := 0; i < a.Len(); i++ {
				out = append(out, PrimaryKey{Type: PKVarChar, Str: a.Value(i)})
			}
		}
	}
	return out, nil
}

func columnToUint64(col *arrow.Column) ([]uint64, error) {
	var out []uint64
	for _, chunk := range col.Data().Chunks() {
		a := chunk.(*array.Int64)
		for i := 0; i < a.Len(); i++ {
			out = append(out, uint64(a.Value(i)))
		}
	}
	return out, nil
}

// writeParquetStringColumn writes a single String column named/id = fieldID.
func writeParquetStringColumn(rows []string, fieldID int64) ([]byte, error) {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: strconv.FormatInt(fieldID, 10), Type: arrow.BinaryTypes.String, Metadata: fieldIDMeta(fieldID)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	sb := b.Field(0).(*array.StringBuilder)
	for _, s := range rows {
		sb.Append(s)
	}
	rec := b.NewRecord()
	defer rec.Release()
	return writeRecord(schema, rec)
}

// readParquetStringColumn reads column `col` as strings.
func readParquetStringColumn(blob []byte, col int) ([]string, error) {
	tbl, rel, err := readTable(blob)
	if err != nil {
		return nil, err
	}
	defer rel()
	var out []string
	for _, chunk := range tbl.Column(col).Data().Chunks() {
		a := chunk.(*array.String)
		for i := 0; i < a.Len(); i++ {
			out = append(out, a.Value(i))
		}
	}
	return out, nil
}
