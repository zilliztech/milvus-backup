package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestInferFilterRuleType(t *testing.T) {
	t.Run("Rule1", func(t *testing.T) {
		rule, err := inferFilterRuleType("db1.*")
		assert.NoError(t, err)
		assert.Equal(t, 1, rule)
	})

	t.Run("Rule2", func(t *testing.T) {
		rule, err := inferFilterRuleType("db1.coll1")
		assert.NoError(t, err)
		assert.Equal(t, 2, rule)
	})

	t.Run("Rule3", func(t *testing.T) {
		rule, err := inferFilterRuleType("coll1")
		assert.NoError(t, err)
		assert.Equal(t, 3, rule)
	})

	t.Run("Rule4", func(t *testing.T) {
		rule, err := inferFilterRuleType("db1.")
		assert.NoError(t, err)
		assert.Equal(t, 4, rule)
	})
}

func TestFromPB(t *testing.T) {
	pb := map[string]*backuppb.CollFilter{
		"db1": {Colls: []string{"*"}},
		"db2": {Colls: []string{"coll1", "coll2"}},
	}

	f, err := FromPB(pb)
	assert.NoError(t, err)
	assert.Equal(t, map[string]CollFilter{
		"db1": {AllowAll: true},
		"db2": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
	}, f.DBCollFilter)
}

func TestParse(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		f, err := Parse("")
		assert.NoError(t, err)
		assert.Empty(t, f.DBCollFilter)
	})

	t.Run("Normal", func(t *testing.T) {
		f, err := Parse("db1.*,db2.coll1,coll3,db3.")
		assert.NoError(t, err)
		assert.Equal(t, map[string]CollFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
			"db3":     {},
		}, f.DBCollFilter)
	})

	t.Run("Invalid", func(t *testing.T) {
		_, err := Parse("db1.*.,db2.coll1,coll3,db3.")
		assert.Error(t, err)
	})
}

func TestFilter_AllowDB(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		f := Filter{}
		assert.True(t, f.AllowDB("db1"))
	})

	t.Run("Filter", func(t *testing.T) {
		f := Filter{DBCollFilter: map[string]CollFilter{
			"db1": {AllowAll: true},
			"db2": {},
		}}
		assert.True(t, f.AllowDB("db1"))
		assert.True(t, f.AllowDB("db2"))
		assert.False(t, f.AllowDB("db3"))
	})
}

func TestFilter_AllowNS(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		f := Filter{}
		assert.True(t, f.AllowNS(namespace.New("db1", "coll1")))
	})

	t.Run("Filter", func(t *testing.T) {
		f := Filter{DBCollFilter: map[string]CollFilter{
			"db1": {AllowAll: true},
			"db2": {CollName: map[string]struct{}{"coll1": {}}},
		}}
		assert.True(t, f.AllowNS(namespace.New("db1", "coll1")))
		assert.True(t, f.AllowNS(namespace.New("db2", "coll1")))
		assert.False(t, f.AllowNS(namespace.New("db2", "coll2")))
		assert.False(t, f.AllowNS(namespace.New("db3", "coll1")))
	})
}

func TestFilter_AllowDBs(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		f := Filter{}
		assert.Equal(t, []string{"db1", "db2", "db3"}, f.AllowDBs([]string{"db1", "db2", "db3"}))
	})

	t.Run("Filter", func(t *testing.T) {
		f := Filter{DBCollFilter: map[string]CollFilter{
			"db1": {},
			"db2": {},
		}}
		assert.Equal(t, []string{"db1", "db2"}, f.AllowDBs([]string{"db1", "db2", "db3"}))
	})
}

func TestFilter_AllowNSS(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		f := Filter{}
		ns := []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db2", "coll2"),
			namespace.New("db3", "coll3"),
		}
		assert.ElementsMatch(t, ns, f.AllowNSS(ns))
	})

	t.Run("Filter", func(t *testing.T) {
		f := Filter{DBCollFilter: map[string]CollFilter{
			"db1": {AllowAll: true},
			"db2": {CollName: map[string]struct{}{"coll1": {}}},
		}}
		ns := []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db2", "coll1"),
			namespace.New("db2", "coll2"),
			namespace.New("db3", "coll1"),
		}

		expect := []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db2", "coll1"),
		}
		assert.ElementsMatch(t, expect, f.AllowNSS(ns))
	})
}
