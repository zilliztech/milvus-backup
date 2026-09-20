package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

func Test_inferFilterRuleType(t *testing.T) {
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

func TestFilter_AllowName(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		f := Filter{}
		assert.True(t, f.AllowName(collref.New("db1", "coll1")))
	})

	t.Run("Filter", func(t *testing.T) {
		f := Filter{DBCollFilter: map[string]CollFilter{
			"db1": {AllowAll: true},
			"db2": {CollName: map[string]struct{}{"coll1": {}}},
		}}
		assert.True(t, f.AllowName(collref.New("db1", "coll1")))
		assert.True(t, f.AllowName(collref.New("db2", "coll1")))
		assert.False(t, f.AllowName(collref.New("db2", "coll2")))
		assert.False(t, f.AllowName(collref.New("db3", "coll1")))
	})
}

func TestInferMapperRuleType(t *testing.T) {
	t.Run("Rule1", func(t *testing.T) {
		rule, err := InferMapperRuleType("db1.*", "db2.*")
		assert.NoError(t, err)
		assert.Equal(t, 1, rule)
	})

	t.Run("Rule2", func(t *testing.T) {
		rule, err := InferMapperRuleType("db1.coll1", "db2.coll2")
		assert.NoError(t, err)
		assert.Equal(t, 2, rule)
	})

	t.Run("Rule3", func(t *testing.T) {
		rule, err := InferMapperRuleType("coll1", "coll2")
		assert.NoError(t, err)
		assert.Equal(t, 3, rule)
	})

	t.Run("Rule4", func(t *testing.T) {
		rule, err := InferMapperRuleType("db1.", "db2.")
		assert.NoError(t, err)
		assert.Equal(t, 4, rule)
	})

	t.Run("MismatchedRule1AndRule2", func(t *testing.T) {
		_, err := InferMapperRuleType("db1.*", "db2.coll1")
		assert.Error(t, err)
	})

	t.Run("MismatchedRule1AndRule3", func(t *testing.T) {
		_, err := InferMapperRuleType("db1.*", "db2")
		assert.Error(t, err)
	})

	t.Run("MismatchedRule3AndRule1", func(t *testing.T) {
		_, err := InferMapperRuleType("db1", "db2.*")
		assert.Error(t, err)
	})

	t.Run("Invalid", func(t *testing.T) {
		_, err := InferMapperRuleType("db1.*.", "db2.*.")
		assert.Error(t, err)
	})
}
