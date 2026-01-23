package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestRestoreHandler_validate(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup"}
		h := newRestoreHandler(request, nil)
		err := h.validate()
		assert.NoError(t, err)
	})

	t.Run("BackupNameEmpty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		h := newRestoreHandler(request, nil)
		err := h.validate()
		assert.Error(t, err)
	})

	t.Run("DropAndNotCreate", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", DropExistCollection: true, SkipCreateCollection: true}
		h := newRestoreHandler(request, nil)
		err := h.validate()
		assert.Error(t, err)
	})

	t.Run("RestorePlanAndCollectionSuffix", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", RestorePlan: &backuppb.RestorePlan{}, CollectionSuffix: "_suffix"}
		h := newRestoreHandler(request, nil)
		err := h.validate()
		assert.Error(t, err)
	})

	t.Run("RestorePlanAndCollectionRenames", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", RestorePlan: &backuppb.RestorePlan{}, CollectionRenames: map[string]string{"db1.coll1": "db2.coll2"}}
		h := newRestoreHandler(request, nil)
		err := h.validate()
		assert.Error(t, err)
	})

	t.Run("InvalidCollectionSuffix", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", CollectionSuffix: "invalid-suffix"}
		h := newRestoreHandler(request, nil)
		err := h.validate()
		assert.Error(t, err)
	})
}

func TestInferRuleType(t *testing.T) {
	// rule 1
	rule, err := filter.InferMapperRuleType("db1.*", "db2.*")
	assert.NoError(t, err)
	assert.Equal(t, 1, rule)

	// rule 2
	rule, err = filter.InferMapperRuleType("db1.coll1", "db2.coll2")
	assert.NoError(t, err)
	assert.Equal(t, 2, rule)

	// rule 3
	rule, err = filter.InferMapperRuleType("coll1", "coll2")
	assert.NoError(t, err)
	assert.Equal(t, 3, rule)

	// rule 4
	rule, err = filter.InferMapperRuleType("db1.", "db2.")
	assert.NoError(t, err)
	assert.Equal(t, 4, rule)

	// invalid
	_, err = filter.InferMapperRuleType("db1.*", "db2")
	assert.Error(t, err)
	_, err = filter.InferMapperRuleType("db1", "db2.*")
	assert.Error(t, err)
}

func TestNewTableMapperFromCollRename(t *testing.T) {
	r, err := newTableMapperFromCollRename(map[string]string{
		"db1.*":     "db2.*",
		"db1.coll1": "db2.coll2",
		"coll1":     "coll2",
		"db1.":      "db2.",
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"db1": "db2"}, r.DBWildcard)
	assert.Equal(t, map[string][]namespace.NS{
		"db1.coll1": {
			namespace.New("db2", "coll2"),
		},
		"default.coll1": {
			namespace.New("", "coll2"),
		},
	}, r.NSMapping)
}

func TestNewCollMapperFromPlan(t *testing.T) {
	plan := &backuppb.RestorePlan{Mapping: []*backuppb.RestoreMapping{
		{
			Source: "db1",
			Target: "db2",
			Colls: []*backuppb.RestoreCollectionMapping{
				{Source: "coll1", Target: "coll2"},
				{Source: "coll2", Target: "coll3"},
			},
		},
		{
			Source: "db1",
			Target: "db3",
			Colls: []*backuppb.RestoreCollectionMapping{
				{Source: "coll1", Target: "coll2"},
				{Source: "coll2", Target: "coll3"},
			},
		},
	}}

	mapper, err := newCollMapperFromPlan(plan)
	assert.NoError(t, err)
	tMapper, ok := mapper.(*restore.TableMapper)
	assert.True(t, ok)
	assert.Equal(t, map[string][]namespace.NS{
		"db1.coll1": {
			namespace.New("db2", "coll2"),
			namespace.New("db3", "coll2"),
		},
		"db1.coll2": {
			namespace.New("db2", "coll3"),
			namespace.New("db3", "coll3"),
		},
	}, tMapper.NSMapping)
}

func TestNewCollMapper(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{RestorePlan: &backuppb.RestorePlan{
			Mapping: []*backuppb.RestoreMapping{
				{
					Source: "db1",
					Target: "db2",
					Colls: []*backuppb.RestoreCollectionMapping{
						{Source: "coll1", Target: "coll2"},
					},
				},
			},
		}}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		targetNS := mapper.TagetNS(namespace.New("db1", "coll1"))
		assert.ElementsMatch(t, []namespace.NS{namespace.New("db2", "coll2")}, targetNS)
	})

	t.Run("FromCollRename", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionRenames: map[string]string{
			"db1.coll1": "db2.coll2",
		}}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		targetNS := mapper.TagetNS(namespace.New("db1", "coll1"))
		assert.ElementsMatch(t, []namespace.NS{namespace.New("db2", "coll2")}, targetNS)
	})

	t.Run("FromCollSuffix", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionSuffix: "_suffix"}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		targetNS := mapper.TagetNS(namespace.New("db1", "coll1"))
		assert.ElementsMatch(t, []namespace.NS{namespace.New("db1", "coll1_suffix")}, targetNS)
	})
}

func TestNewDBMapper(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		plan := &backuppb.RestorePlan{Mapping: []*backuppb.RestoreMapping{{
			Source: "db1",
			Target: "db2",
		}, {
			Source: "db1",
			Target: "db3",
		}}}

		mapper, err := newDBMapper(plan)
		assert.NoError(t, err)
		assert.Equal(t, map[string][]restore.DBMapping{
			"db1": {
				{Target: "db2"},
				{Target: "db3"},
			},
		}, mapper)
	})

	t.Run("Empty", func(t *testing.T) {
		plan := &backuppb.RestorePlan{}
		mapper, err := newDBMapper(plan)
		assert.NoError(t, err)
		assert.Empty(t, mapper)
	})
}

func TestNewFilterFromDBCollections(t *testing.T) {
	f, err := newFilterFromDBCollections(`{"db1":[],"db2":["coll1","coll2"],"": ["coll3"]}`)
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"db1":     {AllowAll: true},
		"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"default": {CollName: map[string]struct{}{"coll3": {}}},
	}, f.DBCollFilter)
}

func TestNewFilterFromCollectionNames(t *testing.T) {
	f, err := newFilterFromCollectionNames([]string{"coll1", "db2.coll2"})
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"default": {CollName: map[string]struct{}{"coll1": {}}},
		"db2":     {CollName: map[string]struct{}{"coll2": {}}},
	}, f.DBCollFilter)
}

func TestNewBackupFilter(t *testing.T) {
	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{
			// CollectionNames will be ignored
			CollectionNames: []string{"coll1", "db2.coll2"},
			DbCollections: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
			}}
		f, err := newBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
		}, f.DBCollFilter)
	})

	t.Run("FromCollectionNames", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionNames: []string{"coll1", "db2.coll2"}}
		f, err := newBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"default": {CollName: map[string]struct{}{"coll1": {}}},
			"db2":     {CollName: map[string]struct{}{"coll2": {}}},
		}, f.DBCollFilter)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		f, err := newBackupFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, f.DBCollFilter)
	})
}

func TestNewFilterFromPlan(t *testing.T) {
	plan := &backuppb.RestorePlan{Filter: map[string]*backuppb.CollFilter{
		"db1": {Colls: []string{"coll1", "coll2"}},
		"db2": {Colls: []string{"coll3", "coll4"}},
	}}
	f, err := newFilterFromPlan(plan)
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
	}, f.DBCollFilter)
}

func TestNewTaskFilter(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{
			// dbCollectionsAfterRename will be ignored
			DbCollectionsAfterRename: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
			},
			RestorePlan: &backuppb.RestorePlan{
				Filter: map[string]*backuppb.CollFilter{
					"db1": {Colls: []string{"coll1", "coll2"}},
					"db2": {Colls: []string{"coll3", "coll4"}},
				},
			}}
		f, err := newTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
		}, f.DBCollFilter)
	})

	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{DbCollectionsAfterRename: &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
		}}
		f, err := newTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
		}, f.DBCollFilter)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		f, err := newTaskFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, f.DBCollFilter)
	})
}
