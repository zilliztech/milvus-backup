package l0compact

import (
	"encoding/json"
	"fmt"
)

// DeleteEntry is one delete: a primary key and its delete timestamp.
type DeleteEntry struct {
	PK PrimaryKey
	Ts uint64
}

// deleteLogJSON is the on-disk json-variant row (milvus storage.DeleteLog).
type deleteLogJSON struct {
	Pk     json.RawMessage `json:"pk"`
	Ts     uint64          `json:"ts"`
	PkType int64           `json:"pkType"`
}

// ReadDeltalog decodes a deltalog blob into delete entries.
func ReadDeltalog(blob []byte, kind StorageKind, t PKType) ([]DeleteEntry, error) {
	switch kind {
	case KindV2:
		pks, tss, err := ReadParquetPKTs(blob, t)
		if err != nil {
			return nil, err
		}
		return zipEntries(pks, tss), nil
	case KindV1:
		desc, events, err := parseV1Envelope(blob)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			return nil, nil
		}
		payload := events[0].Payload
		if desc.Extras["version"] == "MULTI_FIELD" {
			pks, tss, err := ReadParquetPKTs(payload, t)
			if err != nil {
				return nil, err
			}
			return zipEntries(pks, tss), nil
		}
		return readV1JSONDeltalog(payload, t)
	default:
		return nil, fmt.Errorf("l0compact: unknown storage kind %d", kind)
	}
}

func readV1JSONDeltalog(payload []byte, t PKType) ([]DeleteEntry, error) {
	// json variant: single String column, each value a DeleteLog JSON.
	rows, err := readParquetStringColumn(payload, 0)
	if err != nil {
		return nil, err
	}
	out := make([]DeleteEntry, 0, len(rows))
	for _, s := range rows {
		var dl deleteLogJSON
		if err := json.Unmarshal([]byte(s), &dl); err != nil {
			return nil, fmt.Errorf("l0compact: unmarshal DeleteLog: %w", err)
		}
		pk := PrimaryKey{Type: t}
		if t == PKInt64 {
			if err := json.Unmarshal(dl.Pk, &pk.Int); err != nil {
				return nil, err
			}
		} else {
			if err := json.Unmarshal(dl.Pk, &pk.Str); err != nil {
				return nil, err
			}
		}
		out = append(out, DeleteEntry{PK: pk, Ts: dl.Ts})
	}
	return out, nil
}

// WriteDeltalog encodes delete entries. KindV1 => v1 envelope + json payload;
// KindV2 => bare 2-col parquet.
func WriteDeltalog(entries []DeleteEntry, kind StorageKind, t PKType) ([]byte, error) {
	pks := make([]PrimaryKey, len(entries))
	tss := make([]uint64, len(entries))
	for i, e := range entries {
		pks[i], tss[i] = e.PK, e.Ts
	}
	switch kind {
	case KindV2:
		return WriteParquetPKTs(pks, tss, t)
	case KindV1:
		rows := make([]string, len(entries))
		for i, e := range entries {
			var pkRaw json.RawMessage
			if t == PKInt64 {
				pkRaw, _ = json.Marshal(e.PK.Int)
			} else {
				pkRaw, _ = json.Marshal(e.PK.Str)
			}
			b, _ := json.Marshal(deleteLogJSON{Pk: pkRaw, Ts: e.Ts, PkType: int64(t)})
			rows[i] = string(b)
		}
		payload, err := writeParquetStringColumn(rows, 0)
		if err != nil {
			return nil, err
		}
		// json variant: PayloadDataType = String(20), field id 0, no version extra.
		return BuildV1Envelope(eventDelete, 20, 0, map[string]string{}, payload)
	default:
		return nil, fmt.Errorf("l0compact: unknown storage kind %d", kind)
	}
}

func zipEntries(pks []PrimaryKey, tss []uint64) []DeleteEntry {
	out := make([]DeleteEntry, len(pks))
	for i := range pks {
		out[i] = DeleteEntry{PK: pks[i], Ts: tss[i]}
	}
	return out
}
