package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// GetRestore reads the state of one restore job. Restore jobs keep all their
// state in the job state store, so this is the thinnest usecase in the layer:
// unlike its sister GetBackup it never touches storage, which also means
// there is nothing to construct and construction cannot fail.
type GetRestore struct {
	store *jobstate.Store
}

// NewGetRestore builds the usecase on the given job state store; the server
// wiring passes the process-local one. That manager's state dies with the
// process — the existing get_restore contract: after a restart every task
// ID answers not-found.
func NewGetRestore(store *jobstate.Store) *GetRestore {
	return &GetRestore{store: store}
}

// Execute returns the job state store's view of the restore job with the given
// ID. An unknown ID is an error, not a silent success. A restore has no
// artifact half to merge — the backup a job produces is read through
// GetBackup — so unlike GetBackup the job state store's own view type travels
// unchanged; an app-defined struct would only rename it.
func (uc *GetRestore) Execute(ctx context.Context, id string) (jobstate.RestoreTaskView, error) {
	taskView, err := uc.store.GetRestoreTask(id)
	if err != nil {
		return nil, fmt.Errorf("app: get restore task %s: %w", id, err)
	}

	return taskView, nil
}
