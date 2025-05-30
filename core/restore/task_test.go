package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func newTestTask(bak *backuppb.BackupInfo) *Task {
	return &Task{backup: bak, logger: zap.NewNop()}
}

func TestTask_GetDBNameDBBackup(t *testing.T) {
	t.Run("WithDBBackup", func(t *testing.T) {
		bak := &backuppb.BackupInfo{DatabaseBackups: []*backuppb.DatabaseBackupInfo{{DbName: "db1"}, {DbName: "db2"}}}
		expect := map[string]*backuppb.DatabaseBackupInfo{"db1": {DbName: "db1"}, "db2": {DbName: "db2"}}
		task := &Task{backup: bak}
		result := task.getDBNameDBBackup()
		assert.Equal(t, expect, result)
	})

	t.Run("WithoutDBBackup", func(t *testing.T) {
		collsBak := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
			{DbName: "default", CollectionName: "coll1"},
			{DbName: "", CollectionName: "coll2"},
		}
		task := &Task{backup: &backuppb.BackupInfo{CollectionBackups: collsBak}}
		result := task.getDBNameDBBackup()
		assert.Empty(t, result)
	})
}

func TestTask_GetDBNameCollNameCollBackup(t *testing.T) {
	collsBak := []*backuppb.CollectionBackupInfo{
		{DbName: "db1", CollectionName: "coll1"},
		{DbName: "db1", CollectionName: "coll2"},
		{DbName: "db2", CollectionName: "coll1"},
		{DbName: "default", CollectionName: "coll1"},
		{DbName: "", CollectionName: "coll2"},
	}
	expect := map[string]map[string]*backuppb.CollectionBackupInfo{
		"db1": {
			"coll1": {DbName: "db1", CollectionName: "coll1"},
			"coll2": {DbName: "db1", CollectionName: "coll2"},
		},
		"db2": {"coll1": {DbName: "db2", CollectionName: "coll1"}},
		"default": {
			"coll1": {DbName: "default", CollectionName: "coll1"},
			"coll2": {DbName: "", CollectionName: "coll2"},
		},
	}
	task := &Task{backup: &backuppb.BackupInfo{CollectionBackups: collsBak}}
	result := task.getDBNameCollNameCollBackup()
	assert.Equal(t, expect, result)
}

func TestTask_FilterDBBackup(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		bak := &backuppb.BackupInfo{DatabaseBackups: []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db2"},
			{DbName: "db3"},
		}}
		task := newTestTask(bak)
		dbCollections := meta.DbCollections{"db1": {}, "db2": {}}
		expect := []*backuppb.DatabaseBackupInfo{{DbName: "db1"}, {DbName: "db2"}}
		result := task.filterDBBackup(dbCollections)
		assert.ElementsMatch(t, expect, result)
	})

	t.Run("DBBackupIsEmpty", func(t *testing.T) {
		task := newTestTask(&backuppb.BackupInfo{})
		dbCollections := meta.DbCollections{"db1": {}}
		result := task.filterDBBackup(dbCollections)
		assert.Empty(t, result)
	})

	t.Run("DBNotExist", func(t *testing.T) {
		bak := &backuppb.BackupInfo{DatabaseBackups: []*backuppb.DatabaseBackupInfo{{DbName: "db1"}}}
		task := newTestTask(bak)
		dbCollections := meta.DbCollections{"db2": {}}
		result := task.filterDBBackup(dbCollections)
		assert.Empty(t, result)
	})
}

func TestTask_FilterCollBackup(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		collsBak := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
			{DbName: "db2", CollectionName: "coll2"},
		}
		task := &Task{backup: &backuppb.BackupInfo{CollectionBackups: collsBak}}
		dbCollections := meta.DbCollections{"db1": {"coll1"}, "db2": {}}
		expect := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db2", CollectionName: "coll1"},
			{DbName: "db2", CollectionName: "coll2"},
		}
		result, err := task.filterCollBackup(dbCollections)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expect, result)
	})

	t.Run("DBNotExist", func(t *testing.T) {
		collsBak := []*backuppb.CollectionBackupInfo{{DbName: "db1", CollectionName: "coll1"}}
		task := &Task{backup: &backuppb.BackupInfo{CollectionBackups: collsBak}}
		// if db2 not exist, but hasn't collections, it should be ok, it means restore the empty db
		_, err := task.filterCollBackup(meta.DbCollections{"db2": {}})
		assert.NoError(t, err)
		// if db2 not exist, but has collections, it should be error
		_, err = task.filterCollBackup(meta.DbCollections{"db2": {"coll2"}})
		assert.Error(t, err)
	})
}

func TestTask_FilterDBTask(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		tasks := []*backuppb.RestoreDatabaseTask{{TargetDbName: "db1"}, {TargetDbName: "db2"}}
		task := newTestTask(nil)
		dbCollections := meta.DbCollections{"db1": {}}
		expect := []*backuppb.RestoreDatabaseTask{{TargetDbName: "db1"}}
		result := task.filterDBTask(dbCollections, map[string]*backuppb.RestoreDatabaseTask{"db1": tasks[0], "db2": tasks[1]})
		assert.ElementsMatch(t, expect, result)
	})

	t.Run("DBNotExist", func(t *testing.T) {
		task := newTestTask(nil)
		dbCollections := meta.DbCollections{"db1": {}}
		result := task.filterDBTask(dbCollections, map[string]*backuppb.RestoreDatabaseTask{"db2": {}})
		assert.Empty(t, result)
	})
}

func TestTask_FilterCollTask(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		tasks := []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1", TargetCollectionName: "coll1"},
			{TargetDbName: "db1", TargetCollectionName: "coll2"},
			{TargetDbName: "db2", TargetCollectionName: "coll1"},
			{TargetDbName: "db2", TargetCollectionName: "coll2"},
		}
		task := newTestTask(nil)
		dbCollections := meta.DbCollections{"db1": {"coll1"}, "db2": {}}
		expect := []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1", TargetCollectionName: "coll1"},
			{TargetDbName: "db2", TargetCollectionName: "coll1"},
			{TargetDbName: "db2", TargetCollectionName: "coll2"},
		}
		result, err := task.filterCollTask(dbCollections, map[string]map[string]*backuppb.RestoreCollectionTask{
			"db1": {"coll1": tasks[0], "coll2": tasks[1]},
			"db2": {"coll1": tasks[2], "coll2": tasks[3]},
		})
		assert.NoError(t, err)
		assert.ElementsMatch(t, expect, result)
	})

	t.Run("CollNotExist", func(t *testing.T) {
		task := newTestTask(nil)
		dbCollections := meta.DbCollections{"db1": {"coll1"}}
		_, err := task.filterCollTask(dbCollections, map[string]map[string]*backuppb.RestoreCollectionTask{
			"db1": {"coll2": {}},
		})
		assert.Error(t, err)
	})
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
			cli := client.NewMockGrpc(t)

			task := &backuppb.RestoreCollectionTask{
				TargetDbName:         "db1",
				TargetCollectionName: "coll1",
				SkipCreateCollection: tc.skipCreate,
				DropExistCollection:  tc.dropExist,
			}

			cli.EXPECT().HasCollection(mock.Anything, "db1", "coll1").Return(tc.has, nil).Once()

			rt := &Task{grpc: cli, logger: zap.NewNop()}
			err := rt.checkCollExist(context.Background(), task)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
