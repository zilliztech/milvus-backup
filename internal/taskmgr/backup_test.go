package taskmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestBackupCollTask_Progress(t *testing.T) {
	t.Run("FixedProgress", func(t *testing.T) {
		task := &backupCollTask{stateCode: _backupCollStatusPending}
		assert.Equal(t, int32(1), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusDDLExecuting}
		assert.Equal(t, int32(5), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusDDLDone}
		assert.Equal(t, int32(10), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusDMLPrepare}
		assert.Equal(t, int32(15), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusDMLDone}
		assert.Equal(t, int32(100), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusSuccess}
		assert.Equal(t, int32(100), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusFailed}
		assert.Equal(t, int32(0), task.Progress())
	})

	t.Run("MappedProgress", func(t *testing.T) {
		task := &backupCollTask{stateCode: _backupCollStatusDMLExecuting, totalSize: 100, backupSize: 0}
		assert.Equal(t, int32(15), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusDMLExecuting, totalSize: 100, backupSize: 50}
		assert.Equal(t, int32(57), task.Progress())

		task = &backupCollTask{stateCode: _backupCollStatusDMLExecuting, totalSize: 100, backupSize: 100}
		assert.Equal(t, int32(100), task.Progress())

		// total size is 0, so we just return 15
		task = &backupCollTask{stateCode: _backupCollStatusDMLExecuting, totalSize: 0, backupSize: 100}
		assert.Equal(t, int32(15), task.Progress())
	})
}

func TestBackupTask_Progress(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		task := &BackupTask{stateCode: _backupTaskStateSuccess}
		assert.Equal(t, int32(100), task.Progress())
	})

	t.Run("DatabaseExecuting", func(t *testing.T) {
		task := &BackupTask{stateCode: _backupTaskSateDatabaseExecuting}
		assert.Equal(t, int32(5), task.Progress())
	})

	t.Run("CollectionExecuting", func(t *testing.T) {
		task := &BackupTask{stateCode: _backupTaskStateCollectionExecuting, collTask: map[namespace.NS]*backupCollTask{
			namespace.New("db1", "coll1"): {
				stateCode:  _backupCollStatusDMLExecuting,
				totalSize:  100,
				backupSize: 50,
			},
			namespace.New("db1", "coll2"): {stateCode: _backupCollStatusSuccess},
		}}

		progress := task.Progress()
		assert.Equal(t, int32(79), progress)
	})
}
