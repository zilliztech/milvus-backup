package l0compact

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimaryKeyMapKey(t *testing.T) {
	// PrimaryKey must be usable as a comparable map key (delete map is pk->ts).
	m := map[PrimaryKey]uint64{}
	m[PrimaryKey{Type: PKInt64, Int: 7}] = 100
	assert.Equal(t, uint64(100), m[PrimaryKey{Type: PKInt64, Int: 7}])
	m[PrimaryKey{Type: PKVarChar, Str: "a"}] = 200
	assert.Equal(t, uint64(200), m[PrimaryKey{Type: PKVarChar, Str: "a"}])
	assert.NotContains(t, m, PrimaryKey{Type: PKInt64, Int: 8})
}

func TestPKTypeFromDataType(t *testing.T) {
	got, err := PKTypeFromDataType(5)
	assert.NoError(t, err)
	assert.Equal(t, PKInt64, got)

	got, err = PKTypeFromDataType(21)
	assert.NoError(t, err)
	assert.Equal(t, PKVarChar, got)

	_, err = PKTypeFromDataType(101)
	assert.Error(t, err, "want error for non-pk type")
}
