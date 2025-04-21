package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/mocks"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

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
		task := &Task{backup: bak}
		dbCollections := meta.DbCollections{"db1": {}, "db2": {}}
		expect := []*backuppb.DatabaseBackupInfo{{DbName: "db1"}, {DbName: "db2"}}
		result, err := task.filterDBBackup(dbCollections)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expect, result)
	})

	t.Run("DBNotExist", func(t *testing.T) {
		bak := &backuppb.BackupInfo{DatabaseBackups: []*backuppb.DatabaseBackupInfo{{DbName: "db1"}}}
		task := &Task{backup: bak}
		dbCollections := meta.DbCollections{"db2": {}}
		_, err := task.filterDBBackup(dbCollections)
		assert.Error(t, err)
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
		dbCollections := meta.DbCollections{"db2": {}}
		_, err := task.filterCollBackup(dbCollections)
		assert.Error(t, err)
	})

	t.Run("CollNotExist", func(t *testing.T) {
		collsBak := []*backuppb.CollectionBackupInfo{{DbName: "db1", CollectionName: "coll1"}}
		task := &Task{backup: &backuppb.BackupInfo{CollectionBackups: collsBak}}
		dbCollections := meta.DbCollections{"db1": {"coll2"}}
		_, err := task.filterCollBackup(dbCollections)
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
			cli := mocks.NewMockGrpc(t)

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
