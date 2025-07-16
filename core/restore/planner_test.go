package restore

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestDefaultRenamer(t *testing.T) {
	r := newDefaultCollMapper()

	ns := namespace.New("db1", "coll1")
	assert.ElementsMatch(t, []namespace.NS{ns}, r.tagetNS(ns))
}

func TestSuffixRenamer(t *testing.T) {
	r := newSuffixMapper("_bak")
	ns := namespace.New("db1", "coll1")
	expect := []namespace.NS{namespace.New("db1", "coll1_bak")}
	assert.ElementsMatch(t, expect, r.tagetNS(ns))
}

func TestMapRenamer(t *testing.T) {
	t.Run("DBWildcard", func(t *testing.T) {
		r := &tableMapper{dbWildcard: map[string]string{"db1": "db2"}}

		expect := []namespace.NS{namespace.New("db2", "coll1")}
		in := namespace.New("db1", "coll1")
		assert.ElementsMatch(t, expect, r.tagetNS(in))

		expect = []namespace.NS{namespace.New("db2", "coll2")}
		in = namespace.New("db1", "coll2")
		assert.ElementsMatch(t, expect, r.tagetNS(in))

		expect = []namespace.NS{namespace.New("db3", "coll1")}
		in = namespace.New("db3", "coll1")
		assert.Equal(t, expect, r.tagetNS(in))
	})

	t.Run("CollMapper", func(t *testing.T) {
		r := &tableMapper{nsMapping: map[string][]namespace.NS{
			"db1.coll1": {
				namespace.New("db2", "coll2"),
				namespace.New("db3", "coll3"),
			},
		}}

		expect := []namespace.NS{
			namespace.New("db2", "coll2"),
			namespace.New("db3", "coll3"),
		}
		in := namespace.New("db1", "coll1")
		assert.ElementsMatch(t, expect, r.tagetNS(in))

		expect = []namespace.NS{namespace.New("db1", "coll2")}
		in = namespace.New("db1", "coll2")
		assert.ElementsMatch(t, expect, r.tagetNS(in))
	})
}

func TestInferRuleType(t *testing.T) {
	// rule 1
	rule, err := inferRuleType("db1.*", "db2.*")
	assert.NoError(t, err)
	assert.Equal(t, 1, rule)

	// rule 2
	rule, err = inferRuleType("db1.coll1", "db2.coll2")
	assert.NoError(t, err)
	assert.Equal(t, 2, rule)

	// rule 3
	rule, err = inferRuleType("coll1", "coll2")
	assert.NoError(t, err)
	assert.Equal(t, 3, rule)

	// rule 4
	rule, err = inferRuleType("db1.", "db2.")
	assert.NoError(t, err)
	assert.Equal(t, 4, rule)

	// invalid
	_, err = inferRuleType("db1.*", "db2")
	assert.Error(t, err)
	_, err = inferRuleType("db1", "db2.*")
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
	assert.Equal(t, map[string]string{"db1": "db2"}, r.dbWildcard)
	assert.Equal(t, map[string][]namespace.NS{
		"db1.coll1": {
			namespace.New("db2", "coll2"),
		},
		"default.coll1": {
			namespace.New("", "coll2"),
		},
	}, r.nsMapping)
}

func TestPlanner_filterDBBackup(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		p := &planner{}
		dbBackup := []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db2"},
			{DbName: "db3"},
		}
		assert.ElementsMatch(t, dbBackup, p.filterDBBackup(dbBackup))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &planner{dbBackupFilter: map[string]struct{}{
			"db1": {},
			"db3": {},
		}}
		dbBackup := []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db2"},
			{DbName: "db3"},
		}
		expect := []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db3"},
		}
		assert.ElementsMatch(t, expect, p.filterDBBackup(dbBackup))
	})
}

func TestPlanner_filterCollBackup(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		p := &planner{}
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}
		assert.ElementsMatch(t, collBackup, p.filterCollBackup(collBackup))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &planner{collBackupFilter: map[string]collFilter{
			"db1": {CollName: map[string]struct{}{
				"coll1": {},
			}},
		}}
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}
		expect := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
		}
		assert.ElementsMatch(t, expect, p.filterCollBackup(collBackup))
	})

	t.Run("AllowAll", func(t *testing.T) {
		p := &planner{collBackupFilter: map[string]collFilter{
			"db1": {AllowAll: true},
		}}
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}
		expect := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
		}
		assert.ElementsMatch(t, expect, p.filterCollBackup(collBackup))
	})
}

func TestPlanner_filterDBTask(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		p := &planner{}
		dbTasks := []*backuppb.RestoreDatabaseTask{
			{TargetDbName: "db1"},
			{TargetDbName: "db2"},
			{TargetDbName: "db3"},
		}
		assert.ElementsMatch(t, dbTasks, p.filterDBTask(dbTasks))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &planner{dbTaskFilter: map[string]struct{}{
			"db1": {},
			"db3": {},
		}}
		dbTasks := []*backuppb.RestoreDatabaseTask{
			{TargetDbName: "db1"},
			{TargetDbName: "db2"},
			{TargetDbName: "db3"},
		}
		expect := []*backuppb.RestoreDatabaseTask{
			{TargetDbName: "db1"},
			{TargetDbName: "db3"},
		}
		assert.ElementsMatch(t, expect, p.filterDBTask(dbTasks))
	})
}

func TestPlanner_filterCollTask(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		p := &planner{}
		collTasks := []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1", TargetCollectionName: "coll1"},
			{TargetDbName: "db1", TargetCollectionName: "coll2"},
			{TargetDbName: "db2", TargetCollectionName: "coll1"},
		}
		assert.ElementsMatch(t, collTasks, p.filterCollTask(collTasks))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &planner{collTaskFilter: map[string]collFilter{
			"db1": {CollName: map[string]struct{}{
				"coll1": {},
			}},
		}}
		collTasks := []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1", TargetCollectionName: "coll1"},
			{TargetDbName: "db1", TargetCollectionName: "coll2"},
			{TargetDbName: "db2", TargetCollectionName: "coll1"},
		}
		expect := []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1", TargetCollectionName: "coll1"},
		}
		assert.ElementsMatch(t, expect, p.filterCollTask(collTasks))
	})

	t.Run("AllowAll", func(t *testing.T) {
		p := &planner{collTaskFilter: map[string]collFilter{
			"db1": {AllowAll: true},
		}}
		collTasks := []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1", TargetCollectionName: "coll1"},
			{TargetDbName: "db1", TargetCollectionName: "coll2"},
			{TargetDbName: "db2", TargetCollectionName: "coll1"},
		}
		expect := []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1", TargetCollectionName: "coll1"},
			{TargetDbName: "db1", TargetCollectionName: "coll2"},
		}
		assert.ElementsMatch(t, expect, p.filterCollTask(collTasks))
	})
}

func TestPlanner_NewDBTaskPB(t *testing.T) {
	t.Run("NoMapping", func(t *testing.T) {
		p := &planner{logger: zap.NewNop()}
		dbBak := &backuppb.DatabaseBackupInfo{DbName: "db1"}
		tasks := p.newDBTaskPB(dbBak)
		assert.Equal(t, 1, len(tasks))
		assert.Equal(t, "db1", tasks[0].GetTargetDbName())
	})

	t.Run("WithMapping", func(t *testing.T) {
		p := &planner{
			logger:   zap.NewNop(),
			dbMapper: map[string][]dbMapping{"db1": {{Target: "db2"}, {Target: "db3"}}},
		}
		dbBak := &backuppb.DatabaseBackupInfo{DbName: "db1"}
		tasks := p.newDBTaskPB(dbBak)
		assert.Equal(t, 2, len(tasks))
		names := lo.Map(tasks, func(task *backuppb.RestoreDatabaseTask, _ int) string { return task.GetTargetDbName() })
		assert.ElementsMatch(t, []string{"db2", "db3"}, names)
	})
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
	tMapper, ok := mapper.(*tableMapper)
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
	}, tMapper.nsMapping)
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
		targetNS := mapper.tagetNS(namespace.New("db1", "coll1"))
		assert.ElementsMatch(t, []namespace.NS{namespace.New("db2", "coll2")}, targetNS)
	})

	t.Run("FromCollRename", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionRenames: map[string]string{
			"db1.coll1": "db2.coll2",
		}}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		targetNS := mapper.tagetNS(namespace.New("db1", "coll1"))
		assert.ElementsMatch(t, []namespace.NS{namespace.New("db2", "coll2")}, targetNS)
	})

	t.Run("FromCollSuffix", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionSuffix: "_suffix"}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		targetNS := mapper.tagetNS(namespace.New("db1", "coll1"))
		assert.ElementsMatch(t, []namespace.NS{namespace.New("db1", "coll1_suffix")}, targetNS)
	})
}

func TestNewDBMapper(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		plan := &backuppb.RestorePlan{Mapping: []*backuppb.RestoreMapping{{
			Source: "db1",
			Target: "db2",
		},
			{
				Source: "db1",
				Target: "db3",
			},
		}}

		mapper, err := newDBMapper(plan)
		assert.NoError(t, err)
		assert.Equal(t, map[string][]dbMapping{
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

func TestNewDBFilterFromDBCollections(t *testing.T) {
	dbFilter, err := newDBFilterFromDBCollections(`{"db1":[],"db2":["coll1","coll2"],"": ["coll3"]}`)
	assert.NoError(t, err)
	assert.Equal(t, map[string]struct{}{"db1": {}, "db2": {}, "default": {}}, dbFilter)
}

func TestNewDBBackupFilter(t *testing.T) {
	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{DbCollections: &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
		}}
		dbFilter, err := newDBBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"db1": {}, "db2": {}, "default": {}}, dbFilter)
	})

	t.Run("FromCollectionNames", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionNames: []string{"coll1", "db2.coll2"}}
		dbFilter, err := newDBBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"default": {}, "db2": {}}, dbFilter)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		dbFilter, err := newDBBackupFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, dbFilter)
	})
}

func TestNewCollFilterFromDBCollections(t *testing.T) {
	cf, err := newCollFilterFromDBCollections(`{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`)
	assert.NoError(t, err)
	assert.Equal(t, map[string]collFilter{
		"db1":     {AllowAll: true},
		"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"default": {CollName: map[string]struct{}{"coll3": {}}},
	}, cf)
}

func TestNewCollFilterFromCollectionNames(t *testing.T) {
	cf, err := newCollFilterFromCollectionNames([]string{"coll1", "db2.coll2"})
	assert.NoError(t, err)
	assert.Equal(t, map[string]collFilter{
		"default": {CollName: map[string]struct{}{"coll1": {}}},
		"db2":     {CollName: map[string]struct{}{"coll2": {}}},
	}, cf)
}

func TestNewCollBackupFilter(t *testing.T) {
	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{
			// CollectionNames will be ignored
			CollectionNames: []string{"coll1", "db2.coll2"},
			DbCollections: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
			}}
		cf, err := newCollBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]collFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
		}, cf)
	})

	t.Run("FromCollectionNames", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionNames: []string{"coll1", "db2.coll2"}}
		cf, err := newCollBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]collFilter{
			"default": {CollName: map[string]struct{}{"coll1": {}}},
			"db2":     {CollName: map[string]struct{}{"coll2": {}}},
		}, cf)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		cf, err := newCollBackupFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, cf)
	})
}

func TestNewDBTaskFilterFromPlan(t *testing.T) {
	plan := &backuppb.RestorePlan{Filter: map[string]*backuppb.RestoreFilter{
		"db1": {Colls: []string{"coll1", "coll2"}},
		"db2": {Colls: []string{"coll3", "coll4"}},
	}}
	dbFilter, err := newDBTaskFilterFromPlan(plan)
	assert.NoError(t, err)
	assert.Equal(t, map[string]struct{}{"db1": {}, "db2": {}}, dbFilter)
}

func TestNewDBTaskFilter(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{
			// dbCollectionsAfterRename will be ignored
			DbCollectionsAfterRename: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
			},
			RestorePlan: &backuppb.RestorePlan{
				Filter: map[string]*backuppb.RestoreFilter{
					"db1": {Colls: []string{"coll1", "coll2"}},
					"db2": {Colls: []string{"coll3", "coll4"}},
				},
			}}
		dbFilter, err := newDBTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"db1": {}, "db2": {}}, dbFilter)
	})

	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{DbCollectionsAfterRename: &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
		}}
		dbFilter, err := newDBTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"db1": {}, "db2": {}, "default": {}}, dbFilter)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		dbFilter, err := newDBTaskFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, dbFilter)
	})
}

func TestNewCollTaskFilterFromPlan(t *testing.T) {
	plan := &backuppb.RestorePlan{Filter: map[string]*backuppb.RestoreFilter{
		"db1": {Colls: []string{"coll1", "coll2"}},
		"db2": {Colls: []string{"coll3", "coll4"}},
	}}
	cf := newCollTaskFilterFromPlan(plan)
	assert.Equal(t, map[string]collFilter{
		"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
	}, cf)
}

func TestNewCollTaskFilterFromDBCollections(t *testing.T) {
	cf, err := newCollTaskFilterFromDBCollections(`{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`)
	assert.NoError(t, err)
	assert.Equal(t, map[string]collFilter{
		"db1":     {AllowAll: true},
		"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"default": {CollName: map[string]struct{}{"coll3": {}}},
	}, cf)
}

func TestNewCollTaskFilter(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{
			// dbCollectionsAfterRename will be ignored
			DbCollectionsAfterRename: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
			},
			RestorePlan: &backuppb.RestorePlan{
				Filter: map[string]*backuppb.RestoreFilter{
					"db1": {Colls: []string{"coll1", "coll2"}},
					"db2": {Colls: []string{"coll3", "coll4"}},
				},
			}}
		cf, err := newCollTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]collFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
		}, cf)
	})

	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{DbCollectionsAfterRename: &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
		}}
		cf, err := newCollTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]collFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
		}, cf)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		cf, err := newCollTaskFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, cf)
	})
}

func TestPlanner_newCollTaskPB(t *testing.T) {
	mapper := newMockcollMapper(t)

	ns := namespace.New("db1", "coll1")
	mapper.EXPECT().tagetNS(ns).Return([]namespace.NS{
		namespace.New("db2", "coll2"),
		namespace.New("db3", "coll3"),
	}).Once()

	p := &planner{collMapper: mapper, logger: zap.NewNop()}
	collBak := &backuppb.CollectionBackupInfo{DbName: "db1", CollectionName: "coll1"}
	tasks := p.newCollTaskPB(collBak)
	assert.Len(t, tasks, 2)
	dbNames := lo.Map(tasks, func(task *backuppb.RestoreCollectionTask, _ int) string { return task.GetTargetDbName() })
	assert.ElementsMatch(t, []string{"db2", "db3"}, dbNames)
	collNames := lo.Map(tasks, func(task *backuppb.RestoreCollectionTask, _ int) string { return task.GetTargetCollectionName() })
	assert.ElementsMatch(t, []string{"coll2", "coll3"}, collNames)
}
