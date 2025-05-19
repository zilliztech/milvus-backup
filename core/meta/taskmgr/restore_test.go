package taskmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/namespace"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestRestoreCollectionTask_Progress(t *testing.T) {
	task := &restoreCollectionTask{
		totalSize: 100,
		importJob: map[string]*importJob{
			"job1": {totalSize: 10, progress: 100},
			"job2": {totalSize: 20, progress: 50},
			"job3": {totalSize: 30, progress: 0},
		},
	}

	assert.Equal(t, int32(20), task.Progress())
}

func TestRestoreTask_Progress(t *testing.T) {
	task := &RestoreTask{
		totalSize: 100,
		collTask: map[namespace.NS]*restoreCollectionTask{
			namespace.New("db1", "coll1"): {totalSize: 10, importJob: map[string]*importJob{
				"job1": {totalSize: 10, progress: 100},
			}},
			namespace.New("db1", "coll2"): {totalSize: 20, importJob: map[string]*importJob{
				"job2": {totalSize: 20, progress: 50},
			}},
			namespace.New("db1", "coll3"): {totalSize: 30, importJob: map[string]*importJob{
				"job3": {totalSize: 30, progress: 0},
			}},
		},
	}

	assert.Equal(t, int32(20), task.Progress())
}

func TestSetRestoreExecuting(t *testing.T) {
	task := &RestoreTask{
		stateCode: backuppb.RestoreTaskStateCode_INITIAL,
	}

	SetRestoreExecuting()(task)

	assert.Equal(t, backuppb.RestoreTaskStateCode_EXECUTING, task.stateCode)
}

func TestSetRestoreSuccess(t *testing.T) {
	task := &RestoreTask{
		stateCode: backuppb.RestoreTaskStateCode_INITIAL,
	}

	SetRestoreSuccess()(task)

	assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, task.stateCode)
	assert.False(t, task.endTime.IsZero())
}

func TestSetRestoreFail(t *testing.T) {
	task := &RestoreTask{
		stateCode: backuppb.RestoreTaskStateCode_INITIAL,
	}

	SetRestoreFail(assert.AnError)(task)

	assert.Equal(t, backuppb.RestoreTaskStateCode_FAIL, task.stateCode)
	assert.False(t, task.endTime.IsZero())
	assert.Equal(t, assert.AnError.Error(), task.errorMessage)
}
