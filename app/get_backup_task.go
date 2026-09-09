package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// GetBackupTask reads the state of one backup job. The job half of a backup
// lives only in the task manager, so like its sister GetRestore this usecase
// never touches storage, there is nothing to construct and construction
// cannot fail.
type GetBackupTask struct {
	taskMgr *taskmgr.Mgr
}

// NewGetBackupTask builds the usecase on the given task manager; the server
// wiring passes the process-local one. That manager's state dies with the
// process — an artifact whose creating process has restarted has no job half
// anymore and is read through GetBackup alone.
func NewGetBackupTask(taskMgr *taskmgr.Mgr) *GetBackupTask {
	return &GetBackupTask{taskMgr: taskMgr}
}

// GetBackupTaskRequest selects the job to read. Name and ID are alternative
// selectors; the ID wins when both are set, because only the ID can pin down
// one of several jobs that shared the name.
type GetBackupTaskRequest struct {
	Name string
	ID   string
}

// Execute returns the task manager's view of the selected job. An unknown
// selector is taskmgr.ErrTaskNotFound, not a silent success. The task
// manager's own view type travels unchanged, as in GetRestore: an app-defined
// struct would only rename it.
func (uc *GetBackupTask) Execute(ctx context.Context, req GetBackupTaskRequest) (taskmgr.BackupTaskView, error) {
	if req.Name == "" && req.ID == "" {
		return nil, fmt.Errorf("app: empty backup name and backup id")
	}

	if req.ID != "" {
		task, err := uc.taskMgr.GetBackupTask(req.ID)
		if err != nil {
			return nil, fmt.Errorf("app: get backup task %w", err)
		}
		return task, nil
	}

	task, err := uc.taskMgr.GetBackupTaskByName(req.Name)
	if err != nil {
		return nil, fmt.Errorf("app: get backup task %w", err)
	}
	return task, nil
}
