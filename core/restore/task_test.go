package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func newTestTask() *Task {
	return &Task{
		logger: zap.NewNop(),

		args: TaskArgs{
			Plan:   &Plan{},
			Params: &cfg.Config{},
		},
	}
}

func TestTask_CheckCollExist(t *testing.T) {
	testCases := []struct {
		has        bool
		skipCreate bool
		dropExist  bool

		ok bool
	}{
		{has: true, skipCreate: false, dropExist: false, ok: false},
		{has: true, skipCreate: true, dropExist: false, ok: true},
		{has: true, skipCreate: false, dropExist: true, ok: true},
		{has: true, skipCreate: true, dropExist: true, ok: false},
		{has: false, skipCreate: false, dropExist: false, ok: true},
		{has: false, skipCreate: true, dropExist: false, ok: false},
		{has: false, skipCreate: false, dropExist: true, ok: true},
		{has: false, skipCreate: true, dropExist: true, ok: false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("has:%v, skipCreate:%v, dropExist:%v", tc.has, tc.skipCreate, tc.dropExist), func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			opt := &Option{SkipCreateCollection: tc.skipCreate, DropExistCollection: tc.dropExist}
			task := &collTask{
				targetNS: namespace.New("db1", "coll1"),
				option:   opt,
			}

			cli.EXPECT().HasCollection(mock.Anything, "db1", "coll1").Return(tc.has, nil).Once()

			rt := &Task{args: TaskArgs{Grpc: cli, Option: opt}, logger: zap.NewNop()}
			err := rt.checkCollExist(context.Background(), task)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestTask_filterDBBackup(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		dbBackup := []*backuppb.DatabaseBackupInfo{{DbName: "db1"}, {DbName: "db2"}, {DbName: "db3"}}
		assert.ElementsMatch(t, dbBackup, task.filterDBBackup(dbBackup))
	})

	t.Run("Filter", func(t *testing.T) {
		plan := &Plan{BackupFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
			"db3": {AllowAll: true},
		}}}
		dbBackup := []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db2"},
			{DbName: "db3"},
		}
		expect := []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db3"},
		}
		task := &Task{args: TaskArgs{Plan: plan}}
		assert.ElementsMatch(t, expect, task.filterDBBackup(dbBackup))
	})
}

func TestTask_filterCollBackup(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}

		assert.ElementsMatch(t, collBackup, task.filterCollBackup(collBackup))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &Plan{BackupFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{
				"coll1": {},
			}},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}
		expect := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
		}
		assert.ElementsMatch(t, expect, task.filterCollBackup(collBackup))
	})

	t.Run("AllowAll", func(t *testing.T) {
		p := &Plan{BackupFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}
		expect := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
		}
		assert.ElementsMatch(t, expect, task.filterCollBackup(collBackup))
	})
}

func TestTask_filterDBTask(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		dbTasks := []*databaseTask{
			{targetName: "db1"},
			{targetName: "db2"},
			{targetName: "db3"},
		}
		assert.ElementsMatch(t, dbTasks, task.filterDBTask(dbTasks))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &Plan{TaskFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
			"db3": {AllowAll: true},
		}}}
		task := newTestTask()
		task.args.Plan = p
		dbTasks := []*databaseTask{
			{targetName: "db1"},
			{targetName: "db2"},
			{targetName: "db3"},
		}
		expect := []*databaseTask{
			{targetName: "db1"},
			{targetName: "db3"},
		}
		assert.ElementsMatch(t, expect, task.filterDBTask(dbTasks))
	})
}

func TestTask_filterCollTask(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		collTasks := []*collTask{
			{targetNS: namespace.New("db1", "coll1")},
			{targetNS: namespace.New("db1", "coll2")},
			{targetNS: namespace.New("db2", "coll1")},
		}
		assert.ElementsMatch(t, collTasks, task.filterCollTask(collTasks))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &Plan{TaskFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}}},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collTasks := []*collTask{
			{targetNS: namespace.New("db1", "coll1")},
			{targetNS: namespace.New("db1", "coll2")},
			{targetNS: namespace.New("db2", "coll1")},
		}
		expect := []*collTask{
			{targetNS: namespace.New("db1", "coll1")},
		}
		assert.ElementsMatch(t, expect, task.filterCollTask(collTasks))
	})

	t.Run("AllowAll", func(t *testing.T) {
		p := &Plan{TaskFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collTasks := []*collTask{
			{targetNS: namespace.New("db1", "coll1")},
			{targetNS: namespace.New("db1", "coll2")},
			{targetNS: namespace.New("db2", "coll1")},
		}
		expect := []*collTask{
			{targetNS: namespace.New("db1", "coll1")},
			{targetNS: namespace.New("db1", "coll2")},
		}
		assert.ElementsMatch(t, expect, task.filterCollTask(collTasks))
	})
}

func TestTask_newDBTask(t *testing.T) {
	t.Run("NoMapping", func(t *testing.T) {
		task := newTestTask()
		dbBak := &backuppb.DatabaseBackupInfo{DbName: "db1"}
		dbTasks := task.newDBTask(dbBak)
		assert.Len(t, dbTasks, 1)
		assert.Equal(t, "db1", dbTasks[0].targetName)
	})

	t.Run("WithMapping", func(t *testing.T) {
		p := &Plan{DBMapper: map[string][]DBMapping{"db1": {{Target: "db2"}, {Target: "db3"}}}}
		task := newTestTask()
		task.args.Plan = p
		dbBak := &backuppb.DatabaseBackupInfo{DbName: "db1"}
		dbTasks := task.newDBTask(dbBak)
		assert.Len(t, dbTasks, 2)
		names := lo.Map(dbTasks, func(task *databaseTask, _ int) string { return task.targetName })
		assert.ElementsMatch(t, []string{"db2", "db3"}, names)
	})
}

func TestTask_newCollTasks(t *testing.T) {
	mapper := NewMockCollMapper(t)

	ns := namespace.New("db1", "coll1")
	mapper.EXPECT().TagetNS(ns).Return([]namespace.NS{
		namespace.New("db2", "coll2"),
		namespace.New("db3", "coll3"),
	}).Once()

	task := newTestTask()
	task.args.Plan = &Plan{CollMapper: mapper}
	mgr := taskmgr.NewMgr()
	mgr.AddRestoreTask("task1")
	task.args.TaskID = "task1"
	task.args.TaskMgr = mgr

	dbBackup := &backuppb.DatabaseBackupInfo{DbName: "db1"}
	collBackup := &backuppb.CollectionBackupInfo{DbName: "db1", CollectionName: "coll1"}
	tasks := task.newCollTask(dbBackup, collBackup)
	assert.Len(t, tasks, 2)
	nss := lo.Map(tasks, func(task *collTask, _ int) string { return task.targetNS.String() })
	assert.ElementsMatch(t, []string{"db2.coll2", "db3.coll3"}, nss)
}

func TestDefaultRenamer(t *testing.T) {
	r := NewDefaultCollMapper()

	ns := namespace.New("db1", "coll1")
	assert.ElementsMatch(t, []namespace.NS{ns}, r.TagetNS(ns))
}

func TestSuffixRenamer(t *testing.T) {
	r := NewSuffixMapper("_bak")
	ns := namespace.New("db1", "coll1")
	expect := []namespace.NS{namespace.New("db1", "coll1_bak")}
	assert.ElementsMatch(t, expect, r.TagetNS(ns))
}

func TestMapRenamer(t *testing.T) {
	t.Run("DBWildcard", func(t *testing.T) {
		r := &TableMapper{DBWildcard: map[string]string{"db1": "db2"}}

		expect := []namespace.NS{namespace.New("db2", "coll1")}
		in := namespace.New("db1", "coll1")
		assert.ElementsMatch(t, expect, r.TagetNS(in))

		expect = []namespace.NS{namespace.New("db2", "coll2")}
		in = namespace.New("db1", "coll2")
		assert.ElementsMatch(t, expect, r.TagetNS(in))

		expect = []namespace.NS{namespace.New("db3", "coll1")}
		in = namespace.New("db3", "coll1")
		assert.Equal(t, expect, r.TagetNS(in))
	})

	t.Run("CollMapper", func(t *testing.T) {
		r := &TableMapper{NSMapping: map[string][]namespace.NS{
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
		assert.ElementsMatch(t, expect, r.TagetNS(in))

		expect = []namespace.NS{namespace.New("db1", "coll2")}
		in = namespace.New("db1", "coll2")
		assert.ElementsMatch(t, expect, r.TagetNS(in))
	})
}
