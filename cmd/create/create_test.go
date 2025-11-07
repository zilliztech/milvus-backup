package create

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/filter"
)

func TestOptions_collectionNamesToFilter(t *testing.T) {
	o := &options{collectionNames: "coll1,db1.coll2,db2.coll3"}
	f, err := o.collectionNamesToFilter()
	assert.NoError(t, err)

	assert.Equal(t, map[string]filter.CollFilter{
		"default": {CollName: map[string]struct{}{"coll1": {}}},
		"db1":     {CollName: map[string]struct{}{"coll2": {}}},
		"db2":     {CollName: map[string]struct{}{"coll3": {}}},
	}, f.DBCollFilter)
}

func TestOptions_databasesToFilter(t *testing.T) {
	o := &options{databases: "db1,db2,db3"}
	f, err := o.databasesToFilter()
	assert.NoError(t, err)

	assert.Equal(t, map[string]filter.CollFilter{
		"db1": {AllowAll: true},
		"db2": {AllowAll: true},
		"db3": {AllowAll: true},
	}, f.DBCollFilter)
}

func TestOptions_dbCollectionsToFilter(t *testing.T) {
	o := &options{dbCollections: `{"db1":["coll1","coll2"],"db2":["coll3","coll4"],"db3":[]}`}
	f, err := o.dbCollectionsToFilter()
	assert.NoError(t, err)

	assert.Equal(t, map[string]filter.CollFilter{
		"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
		"db3": {AllowAll: true},
	}, f.DBCollFilter)
}
