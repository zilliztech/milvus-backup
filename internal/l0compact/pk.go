package l0compact

import "fmt"

// PKType mirrors the schemapb.DataType numeric values milvus writes into
// deltalog DeleteLog.pkType and parquet field metadata.
type PKType int

const (
	PKInt64   PKType = 5  // schemapb.DataType_Int64
	PKVarChar PKType = 21 // schemapb.DataType_VarChar
)

// PKTypeFromDataType maps a backuppb/schemapb DataType int to a PKType.
func PKTypeFromDataType(dt int32) (PKType, error) {
	switch PKType(dt) {
	case PKInt64:
		return PKInt64, nil
	case PKVarChar:
		return PKVarChar, nil
	default:
		return 0, fmt.Errorf("l0compact: data type %d is not a supported primary key type (need Int64=5 or VarChar=21)", dt)
	}
}

// PrimaryKey is an Int64 or VarChar primary key value. It is a comparable
// struct so it can be used directly as a map key (the delete map is pk -> ts).
type PrimaryKey struct {
	Type PKType
	Int  int64
	Str  string
}
