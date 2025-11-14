package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCreateBackupHandler_dbCollectionsToFilter(t *testing.T) {
	dbColl := `{"db1":["coll1","coll2"],"db2":["coll3","coll4"],"db3":[]}`
	h := &createBackupHandler{}
	f, err := h.dbCollectionsToFilter(dbColl)
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
		"db3": {AllowAll: true},
	}, f.DBCollFilter)
}

func TestCreateBackupHandler_collectionNamesToFilter(t *testing.T) {
	h := &createBackupHandler{request: &backuppb.CreateBackupRequest{CollectionNames: []string{"coll1", "db1.coll2", "db2.coll3"}}}
	f, err := h.collectionNamesToFilter()
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"default": {CollName: map[string]struct{}{"coll1": {}}},
		"db1":     {CollName: map[string]struct{}{"coll2": {}}},
		"db2":     {CollName: map[string]struct{}{"coll3": {}}},
	}, f.DBCollFilter)
}

func TestCreateBackupHandler_filterToFilter(t *testing.T) {
	h := &createBackupHandler{request: &backuppb.CreateBackupRequest{Filter: map[string]*backuppb.CollFilter{
		"db1": {Colls: []string{"*"}},
		"db2": {Colls: []string{"coll1", "coll2"}},
	}}}
	f, err := h.filterToFilter()
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"db1": {AllowAll: true},
		"db2": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
	}, f.DBCollFilter)
}

func TestCreateBackupHandler_toFilter(t *testing.T) {
	t.Run("FromFilter", func(t *testing.T) {
		h := &createBackupHandler{request: &backuppb.CreateBackupRequest{Filter: map[string]*backuppb.CollFilter{
			"db1": {Colls: []string{"*"}},
			"db2": {Colls: []string{"coll1", "coll2"}},
		}}}

		f, err := h.toFilter()
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1": {AllowAll: true},
			"db2": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		}, f.DBCollFilter)
	})

	t.Run("FromDBCollections", func(t *testing.T) {
		h := &createBackupHandler{request: &backuppb.CreateBackupRequest{DbCollections: &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: `{"db1":["coll1","coll2"],"db2":["coll3","coll4"],"db3":[]}`},
		}}}

		f, err := h.toFilter()
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
			"db3": {AllowAll: true},
		}, f.DBCollFilter)
	})

	t.Run("FromCollectionNames", func(t *testing.T) {
		h := &createBackupHandler{request: &backuppb.CreateBackupRequest{CollectionNames: []string{"coll1", "db2.coll2"}}}
		f, err := h.toFilter()
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"default": {CollName: map[string]struct{}{"coll1": {}}},
			"db2":     {CollName: map[string]struct{}{"coll2": {}}},
		}, f.DBCollFilter)
	})
}
