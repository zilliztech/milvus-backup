package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// GetBackupTask reads the state of one backup job. The job half of a backup
// lives only in the job state store, so like its sister GetRestore this usecase
// never touches storage, there is nothing to construct and construction
// cannot fail.
type GetBackupTask struct {
	store *jobstate.Store
}

// NewGetBackupTask builds the usecase on the given job state store; the server
// wiring passes the process-local one. That manager's state dies with the
// process — an artifact whose creating process has restarted has no job half
// anymore and is read through GetBackup alone.
func NewGetBackupTask(store *jobstate.Store) *GetBackupTask {
	return &GetBackupTask{store: store}
}

// GetBackupTaskRequest selects the job to read. Name and ID are alternative
// selectors; the ID wins when both are set, because only the ID can pin down
// one of several jobs that shared the name.
type GetBackupTaskRequest struct {
	Name string
	ID   string
}

// Execute returns the job state store's view of the selected job. An unknown
// selector is jobstate.ErrTaskNotFound, not a silent success. The task
// manager's own view type travels unchanged, as in GetRestore: an app-defined
// struct would only rename it.
func (uc *GetBackupTask) Execute(ctx context.Context, req GetBackupTaskRequest) (jobstate.BackupTaskView, error) {
	if req.Name == "" && req.ID == "" {
		return nil, fmt.Errorf("app: empty backup name and backup id")
	}

	if req.ID != "" {
		task, err := uc.store.GetBackupTask(req.ID)
		if err != nil {
			return nil, fmt.Errorf("app: get backup task %w", err)
		}
		return task, nil
	}

	task, err := uc.store.GetBackupTaskByName(req.Name)
	if err != nil {
		return nil, fmt.Errorf("app: get backup task %w", err)
	}
	return task, nil
}
