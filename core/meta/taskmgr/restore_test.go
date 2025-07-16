package taskmgr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestRestoreCollectionTask_Progress(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		task := &restoreCollectionTask{
			totalSize: 100,
			importJob: map[string]*importJob{
				"job1": {totalSize: 10, progress: 100},
				"job2": {totalSize: 20, progress: 50},
				"job3": {totalSize: 30, progress: 0},
			},
		}

		assert.Equal(t, int32(20), task.Progress())
	})

	t.Run("ZeroTotalSize", func(t *testing.T) {
		task := &restoreCollectionTask{totalSize: 0}

		assert.Equal(t, int32(1), task.Progress())
	})

	t.Run("ZeroRestoredSize", func(t *testing.T) {
		task := &restoreCollectionTask{
			totalSize: 100,
			importJob: map[string]*importJob{"job1": {totalSize: 10, progress: 0}},
		}

		assert.Equal(t, int32(1), task.Progress())
	})

	t.Run("Success", func(t *testing.T) {
		task := &restoreCollectionTask{stateCode: backuppb.RestoreTaskStateCode_SUCCESS}

		assert.Equal(t, int32(100), task.Progress())
	})
}

func TestRestoreCollectionTask_Getters(t *testing.T) {
	t.Run("ID", func(t *testing.T) {
		task := &restoreCollectionTask{id: "id1"}

		assert.Equal(t, "id1", task.ID())
	})

	t.Run("StateCode", func(t *testing.T) {
		task := &restoreCollectionTask{stateCode: backuppb.RestoreTaskStateCode_SUCCESS}

		assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, task.StateCode())
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		task := &restoreCollectionTask{errorMessage: "error message"}

		assert.Equal(t, "error message", task.ErrorMessage())
	})

	t.Run("StartTime", func(t *testing.T) {
		task := &restoreCollectionTask{startTime: time.Now()}

		assert.Equal(t, task.startTime, task.StartTime())
	})

	t.Run("EndTime", func(t *testing.T) {
		task := &restoreCollectionTask{endTime: time.Now()}

		assert.Equal(t, task.endTime, task.EndTime())
	})

	t.Run("TotalSize", func(t *testing.T) {
		task := &restoreCollectionTask{totalSize: 100}

		assert.Equal(t, int64(100), task.TotalSize())
	})
}

func TestRestoreTask_Progress(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
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
	})

	t.Run("TaskSuccess", func(t *testing.T) {
		task := &RestoreTask{stateCode: backuppb.RestoreTaskStateCode_SUCCESS}

		assert.Equal(t, int32(100), task.Progress())
	})

	t.Run("ZeroTotalSize", func(t *testing.T) {
		task := &RestoreTask{totalSize: 0}

		assert.Equal(t, int32(1), task.Progress())
	})

	t.Run("ZeroRestoredSize", func(t *testing.T) {
		task := &RestoreTask{
			totalSize: 100,
			collTask: map[namespace.NS]*restoreCollectionTask{
				namespace.New("db1", "coll1"): {totalSize: 10, importJob: map[string]*importJob{
					"job1": {totalSize: 10, progress: 0},
				}},
			}}
		assert.Equal(t, int32(1), task.Progress())
	})
}

func TestRestoreTask_Getters(t *testing.T) {
	t.Run("ID", func(t *testing.T) {
		task := &RestoreTask{id: "id1"}

		assert.Equal(t, "id1", task.ID())
	})

	t.Run("StateCode", func(t *testing.T) {
		task := &RestoreTask{stateCode: backuppb.RestoreTaskStateCode_SUCCESS}

		assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, task.StateCode())
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		task := &RestoreTask{errorMessage: "error message"}

		assert.Equal(t, "error message", task.ErrorMessage())
	})

	t.Run("StartTime", func(t *testing.T) {
		task := &RestoreTask{startTime: time.Now()}

		assert.Equal(t, task.startTime, task.StartTime())
	})

	t.Run("EndTime", func(t *testing.T) {
		task := &RestoreTask{endTime: time.Now()}

		assert.Equal(t, task.endTime, task.EndTime())
	})

	t.Run("TotalSize", func(t *testing.T) {
		task := &RestoreTask{totalSize: 100}

		assert.Equal(t, int64(100), task.TotalSize())
	})
}

func TestAddRestoreCollTask(t *testing.T) {
	task := &RestoreTask{
		collTask: make(map[namespace.NS]*restoreCollectionTask),
	}

	AddRestoreCollTask(namespace.New("db1", "coll1"), 100)(task)

	assert.Len(t, task.collTask, 1)
	assert.Equal(t, int64(100), task.collTask[namespace.New("db1", "coll1")].totalSize)
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

func TestSetRestoreCollExecuting(t *testing.T) {
	task := &RestoreTask{
		collTask: map[namespace.NS]*restoreCollectionTask{namespace.New("db1", "coll1"): {}},
	}

	SetRestoreCollExecuting(namespace.New("db1", "coll1"))(task)

	collTask := task.collTask[namespace.New("db1", "coll1")]
	assert.Equal(t, backuppb.RestoreTaskStateCode_EXECUTING, collTask.stateCode)
}

func TestSetRestoreCollSuccess(t *testing.T) {
	task := &RestoreTask{
		collTask: map[namespace.NS]*restoreCollectionTask{namespace.New("db1", "coll1"): {}},
	}

	SetRestoreCollSuccess(namespace.New("db1", "coll1"))(task)

	collTask := task.collTask[namespace.New("db1", "coll1")]
	assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, collTask.stateCode)
	assert.False(t, collTask.endTime.IsZero())
}

func TestSetRestoreCollFail(t *testing.T) {
	task := &RestoreTask{
		collTask: map[namespace.NS]*restoreCollectionTask{namespace.New("db1", "coll1"): {}},
	}

	SetRestoreCollFail(namespace.New("db1", "coll1"), assert.AnError)(task)

	collTask := task.collTask[namespace.New("db1", "coll1")]
	assert.Equal(t, backuppb.RestoreTaskStateCode_FAIL, collTask.stateCode)
	assert.False(t, collTask.endTime.IsZero())
	assert.Equal(t, assert.AnError.Error(), collTask.errorMessage)
}

func TestAddRestoreImportJob(t *testing.T) {
	task := &RestoreTask{
		collTask: map[namespace.NS]*restoreCollectionTask{namespace.New("db1", "coll1"): {
			importJob: make(map[string]*importJob),
		}},
	}

	AddRestoreImportJob(namespace.New("db1", "coll1"), "job1", 100)(task)

	collTask := task.collTask[namespace.New("db1", "coll1")]
	assert.Len(t, collTask.importJob, 1)
	assert.Equal(t, int64(100), collTask.importJob["job1"].totalSize)
}

func TestUpdateRestoreImportJob(t *testing.T) {
	task := &RestoreTask{
		collTask: map[namespace.NS]*restoreCollectionTask{namespace.New("db1", "coll1"): {
			importJob: map[string]*importJob{"job1": {totalSize: 100}}},
		},
	}

	UpdateRestoreImportJob(namespace.New("db1", "coll1"), "job1", 50)(task)

	collTask := task.collTask[namespace.New("db1", "coll1")]
	assert.Equal(t, 50, collTask.importJob["job1"].progress)
}
