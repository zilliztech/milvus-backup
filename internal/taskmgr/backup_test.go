package taskmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestBackupCollTask_Progress(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		task := &backupCollTask{stateCode: backuppb.BackupTaskStateCode_BACKUP_SUCCESS}

		assert.Equal(t, int32(100), task.Progress())
	})

	t.Run("Initial", func(t *testing.T) {
		task := &backupCollTask{stateCode: backuppb.BackupTaskStateCode_BACKUP_INITIAL}

		assert.Equal(t, int32(0), task.Progress())
	})

	t.Run("Executing", func(t *testing.T) {
		task := &backupCollTask{stateCode: backuppb.BackupTaskStateCode_BACKUP_EXECUTING, totalSize: 100, backupSize: 50}

		assert.Equal(t, int32(50), task.Progress())
	})

	t.Run("TotalSizeZero", func(t *testing.T) {
		task := &backupCollTask{stateCode: backuppb.BackupTaskStateCode_BACKUP_EXECUTING, totalSize: 0, backupSize: 50}

		assert.Equal(t, int32(0), task.Progress())
	})
}

func TestBackupTask_Progress(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		task := &BackupTask{stateCode: backuppb.BackupTaskStateCode_BACKUP_SUCCESS}

		assert.Equal(t, int32(100), task.Progress())
	})

	t.Run("Executing", func(t *testing.T) {
		task := &BackupTask{
			stateCode: backuppb.BackupTaskStateCode_BACKUP_EXECUTING,

			collTask: map[namespace.NS]*backupCollTask{
				namespace.New("db1", "coll1"): {
					stateCode:  backuppb.BackupTaskStateCode_BACKUP_EXECUTING,
					totalSize:  100,
					backupSize: 50,
				},
				namespace.New("db1", "coll2"): {
					stateCode:  backuppb.BackupTaskStateCode_BACKUP_EXECUTING,
					totalSize:  200,
					backupSize: 100,
				},
			},
		}

		assert.Equal(t, int32(50), task.Progress())
	})

}
