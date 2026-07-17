package l0compact

import "fmt"

// StorageKind is the on-disk read/write family. v3 reads identically to v2
// (plain parquet located by PARQUET:field_id; backup stores no loon manifest).
type StorageKind int

const (
	KindV1 StorageKind = iota // legacy event envelope + parquet payload
	KindV2                    // bare parquet, PARQUET:field_id metadata (covers v2 and v3)
)

// ClassifyVersion maps a milvus storage_version to a StorageKind, failing fast
// on anything unrecognized so a real backup surfaces unexpected versions.
func ClassifyVersion(v int64) (StorageKind, error) {
	switch v {
	case 0, 1:
		return KindV1, nil
	case 2, 3:
		return KindV2, nil
	default:
		return 0, fmt.Errorf("l0compact: unsupported storage_version %d (only 0/1=v1, 2=v2, 3=v3 handled)", v)
	}
}
