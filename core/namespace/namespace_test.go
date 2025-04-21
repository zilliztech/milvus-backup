package namespace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
