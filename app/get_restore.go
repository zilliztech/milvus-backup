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

// NewGetRestore builds the usecase on the process-local task manager. The
// manager's state dies with the process — the existing get_restore contract:
// after a restart every task ID answers not-found.
func NewGetRestore() *GetRestore {
	return &GetRestore{taskMgr: taskmgr.DefaultMgr()}
}

// RestoreView is the job half alone. A restore has no artifact half of its
// own — the backup a job produces is read through GetBackup — but it still
// travels as an app-defined struct so transports never name the task
// manager's types.
type RestoreView struct {
	Task taskmgr.RestoreTaskView
}

// Execute returns the task manager's view of the restore job with the given
// ID. An unknown ID is an error, not a silent success.
func (uc *GetRestore) Execute(ctx context.Context, id string) (RestoreView, error) {
	taskView, err := uc.taskMgr.GetRestoreTask(id)
	if err != nil {
		return RestoreView{}, fmt.Errorf("app: get restore task %s: %w", id, err)
	}

	return RestoreView{Task: taskView}, nil
}
