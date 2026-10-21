package app

import (
	"context"

	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// The layer speaks two contracts. AsyncJob is the registered run: an id and
// a state in the job store, released into the background to outlive the
// request that started it. The synchronous contract is tasklet.Tasklet —
// Execute calls composing into trees, a task holding child tasklets — and a
// job's body is usually the root of one such tree. The tasklet half already
// has its name in core/tasklet; this file deliberately mints no second word
// for the same shape.
type AsyncJob[Status any] interface {
	// Run releases the job into the background and answers nothing: once
	// released there is nobody on the call path left to act on an error, so
	// the outcome lives in the job store and in Wait.
	//
	// The context must NOT be the request's. A request context is canceled
	// when the request ends, and cancellation walks every context derived
	// from it, so a job released with one is canceled the moment the caller
	// goes away — the opposite of what running async exists for. Hand Run a
	// process-scoped context: the server's base context or
	// context.Background().
	Run(ctx context.Context)

	// Wait blocks until Run's job settles and answers its status. It is the
	// request-side half: taking the request's context here is fine, because
	// abandoning a Wait only stops the waiting, never the job. Waiting on a
	// job that was never started blocks until the context is done.
	Wait(ctx context.Context) (Status, error)
}

var _ AsyncJob[jobstate.DeleteStatus] = (*DeleteJob)(nil)
var _ AsyncJob[jobstate.MigrateStatus] = (*MigrateJob)(nil)
var _ AsyncJob[jobstate.BackupStatus] = (*BackupJob)(nil)
var _ AsyncJob[jobstate.RestoreStatus] = (*RestoreJob)(nil)
var _ AsyncJob[jobstate.RestoreStatus] = (*RestoreSecondaryJob)(nil)
