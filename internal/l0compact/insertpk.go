package l0compact

import "fmt"

// ReadInsertPK reads the primary-key column from a data segment's insert binlogs.
//   - KindV1: each blob is a v1 insert binlog of the PK field; PK is column 0.
//   - KindV2: blobs are column-group parquets; the PK column is the one whose
//     PARQUET:field_id == pkFieldID (scan groups until found).
func ReadInsertPK(blobs [][]byte, kind StorageKind, pkFieldID int64, t PKType) ([]PrimaryKey, error) {
	var out []PrimaryKey
	switch kind {
	case KindV1:
		for _, blob := range blobs {
			_, events, err := parseV1Envelope(blob)
			if err != nil {
				return nil, err
			}
			for _, ev := range events {
				pks, err := readParquetColumn0(ev.Payload, t)
				if err != nil {
					return nil, err
				}
				out = append(out, pks...)
			}
		}
		return out, nil
	case KindV2:
		found := false
		for _, blob := range blobs {
			pks, ok, err := ReadParquetColumnByFieldID(blob, pkFieldID, t)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, pks...)
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("l0compact: PK field %d not found in any insert group parquet", pkFieldID)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("l0compact: unknown storage kind %d", kind)
	}
}
