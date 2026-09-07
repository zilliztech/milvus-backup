package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// GetRestore reads the state of one restore job. Restore jobs keep all their
// state in the task manager, so this is the thinnest usecase in the layer:
// unlike its sister GetBackup it never touches storage, which also means
// there is nothing to construct and construction cannot fail.
type GetRestore struct {
	taskMgr *taskmgr.Mgr
}

// NewGetRestore builds the usecase on the given task manager; the server
// wiring passes the process-local one. That manager's state dies with the
// process — the existing get_restore contract: after a restart every task
// ID answers not-found.
func NewGetRestore(taskMgr *taskmgr.Mgr) *GetRestore {
	return &GetRestore{taskMgr: taskMgr}
}

// Execute returns the task manager's view of the restore job with the given
// ID. An unknown ID is an error, not a silent success. A restore has no
// artifact half to merge — the backup a job produces is read through
// GetBackup — so unlike GetBackup the task manager's own view type travels
// unchanged; an app-defined struct would only rename it.
func (uc *GetRestore) Execute(ctx context.Context, id string) (taskmgr.RestoreTaskView, error) {
	taskView, err := uc.taskMgr.GetRestoreTask(id)
	if err != nil {
		return nil, fmt.Errorf("app: get restore task %s: %w", id, err)
	}

	return taskView, nil
}
