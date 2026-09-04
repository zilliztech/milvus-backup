package collref

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	name := New("db1", "coll1")
	assert.Equal(t, "db1", name.DBName())
	assert.Equal(t, "coll1", name.CollName())

	// default db
	name = New("", "coll1")
	assert.Equal(t, DefaultDBName, name.DBName())
	assert.Equal(t, "coll1", name.CollName())
}

func TestString(t *testing.T) {
	name := New("db1", "coll1")
	assert.Equal(t, "db1.coll1", name.String())
}

func TestParse(t *testing.T) {
	t.Run("ValidDBAndColl", func(t *testing.T) {
		name, err := Parse("db1.coll1")
		assert.NoError(t, err)
		assert.Equal(t, "db1", name.DBName())
		assert.Equal(t, "coll1", name.CollName())
	})

	t.Run("ValidCollOnly", func(t *testing.T) {
		name, err := Parse("coll1")
		assert.NoError(t, err)
		assert.Equal(t, DefaultDBName, name.DBName())
		assert.Equal(t, "coll1", name.CollName())
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
