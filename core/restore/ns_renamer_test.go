package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/namespace"
)

func TestDefaultNSRenamer(t *testing.T) {
	renamer := newDefaultNSRenamer()
	ns := namespace.New("db1", "coll1")
	assert.Equal(t, ns, renamer.rename(ns))
}

func TestSuffixRenamer(t *testing.T) {
	renamer := newSuffixNameRenamer("_bak")
	ns := namespace.New("db1", "coll1")
	assert.Equal(t, namespace.New("db1", "coll1_bak"), renamer.rename(ns))
}

func TestMapRenamer(t *testing.T) {
	t.Run("DBWildcard", func(t *testing.T) {
		renamer, err := newMapRenamer(map[string]string{"db1.*": "db2.*"})
		assert.NoError(t, err)

		expect := namespace.New("db2", "coll1")
		in := namespace.New("db1", "coll1")
		assert.Equal(t, expect, renamer.rename(in))
		expect = namespace.New("db2", "coll2")
		in = namespace.New("db1", "coll2")
		assert.Equal(t, expect, renamer.rename(in))

		expect = namespace.New("db3", "coll1")
		in = namespace.New("db3", "coll1")
		assert.Equal(t, expect, renamer.rename(in))
	})

	t.Run("DBAndColl", func(t *testing.T) {
		renamer, err := newMapRenamer(map[string]string{"db1.coll1": "db2.coll2"})
		assert.NoError(t, err)

		expect := namespace.New("db2", "coll2")
		in := namespace.New("db1", "coll1")
		assert.Equal(t, expect, renamer.rename(in))

		expect = namespace.New("db1", "coll2")
		in = namespace.New("db1", "coll2")
		assert.Equal(t, expect, renamer.rename(in))
	})

	t.Run("OnlyColl", func(t *testing.T) {
		renamer, err := newMapRenamer(map[string]string{"coll1": "coll2"})
		assert.NoError(t, err)

		expect := namespace.New("", "coll2")
		in := namespace.New("", "coll1")
		assert.Equal(t, expect, renamer.rename(in))

		expect = namespace.New("db1", "coll1")
		in = namespace.New("db1", "coll1")
		assert.Equal(t, expect, renamer.rename(in))
	})

}
