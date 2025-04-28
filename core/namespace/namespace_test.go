package namespace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	ns := New("db1", "coll1")
	assert.Equal(t, "db1", ns.DBName())
	assert.Equal(t, "coll1", ns.CollName())

	// default db
	ns = New("", "coll1")
	assert.Equal(t, DefaultDBName, ns.DBName())
	assert.Equal(t, "coll1", ns.CollName())
}

func TestString(t *testing.T) {
	ns := New("db1", "coll1")
	assert.Equal(t, "db1.coll1", ns.String())
}

func TestParse(t *testing.T) {
	t.Run("ValidDBAndColl", func(t *testing.T) {
		ns, err := Parse("db1.coll1")
		assert.NoError(t, err)
		assert.Equal(t, "db1", ns.DBName())
		assert.Equal(t, "coll1", ns.CollName())
	})

	t.Run("ValidCollOnly", func(t *testing.T) {
		ns, err := Parse("coll1")
		assert.NoError(t, err)
		assert.Equal(t, DefaultDBName, ns.DBName())
		assert.Equal(t, "coll1", ns.CollName())
	})

	t.Run("Empty", func(t *testing.T) {
		_, err := Parse("")
		assert.Error(t, err)
	})

	t.Run("InvalidFormat", func(t *testing.T) {
		_, err := Parse("db1.coll1.coll2")
		assert.Error(t, err)
	})
}
