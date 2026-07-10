package l0compact

import "testing"

func TestPrimaryKeyMapKey(t *testing.T) {
	// PrimaryKey must be usable as a comparable map key (delete map is pk->ts).
	m := map[PrimaryKey]uint64{}
	m[PrimaryKey{Type: PKInt64, Int: 7}] = 100
	if m[PrimaryKey{Type: PKInt64, Int: 7}] != 100 {
		t.Fatal("int64 pk map lookup failed")
	}
	m[PrimaryKey{Type: PKVarChar, Str: "a"}] = 200
	if m[PrimaryKey{Type: PKVarChar, Str: "a"}] != 200 {
		t.Fatal("varchar pk map lookup failed")
	}
	if _, ok := m[PrimaryKey{Type: PKInt64, Int: 8}]; ok {
		t.Fatal("absent key present")
	}
}

func TestPKTypeFromDataType(t *testing.T) {
	if got, _ := PKTypeFromDataType(5); got != PKInt64 {
		t.Fatalf("5 -> %v", got)
	}
	if got, _ := PKTypeFromDataType(21); got != PKVarChar {
		t.Fatalf("21 -> %v", got)
	}
	if _, err := PKTypeFromDataType(101); err == nil {
		t.Fatal("want error for non-pk type")
	}
}
