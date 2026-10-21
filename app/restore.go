package app

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// RestoreJob is one registered restore job, ready to execute. A job is what
// a restore call makes: the restored collections are what a successful job
// leaves behind in the target Milvus.
//
// Registering a job and running it are separate steps: registration happens
// in NewRestoreJob, synchronously, so a missing backup or an unreadable meta
// is the caller's immediate answer. Running is Run, which releases the job
// into the background; Wait blocks on the settlement.
type RestoreJob struct {
	// task is the restore run itself. Held as the shared tasklet interface so
	// tests stub the run and exercise the job's lifecycle without a cluster
	// behind the restore.
	task tasklet.Tasklet

	// tracker is the job's handle on its own state: the task settles it before
	// the run returns, and Wait reads the settlement through it.
	tracker *jobstate.RestoreTracker

	// done closes when the run settles. Wait selects on it, so the caller
	// learns the outcome without polling the job store.
	done chan struct{}
}

var _ AsyncJob[jobstate.RestoreStatus] = (*RestoreJob)(nil)

// Run releases the job into the background and answers nothing: once the job
// is released there is nobody on the call path who could act on an error, so
// the outcome lives only in the job store, where Wait and the task APIs read
// it. The context must be process-scoped, not the request's — see AsyncJob.Run.
func (j *RestoreJob) Run(ctx context.Context) { go j.run(ctx) }

// run is the job's whole body. The task settles the tracker itself, before
// Execute returns, so closing done is the signal that the terminal state is
// already in the store.
func (j *RestoreJob) run(ctx context.Context) {
	defer close(j.done)

	if err := j.task.Execute(ctx); err != nil {
		log.Error("restore task fail", zap.Error(err))
	}
}

// Wait blocks until a Run settles and answers with the job's snapshot in the
// job store — the terminal state, error message and per-collection halves are
// on it. It is only meaningful after Run; waiting on a job that was never
// started blocks until the context is done.
func (j *RestoreJob) Wait(ctx context.Context) (jobstate.RestoreStatus, error) {
	select {
	case <-ctx.Done():
		return jobstate.RestoreStatus{}, ctx.Err()
	case <-j.done:
		return j.tracker.Snapshot(), nil
	}
}

// RestoreRequest selects and shapes one restore. Plan and Option are the
// restore task's own vocabulary on purpose: each transport translates its
// grammar into them — the CLI from flags, v1 from its pb fields — and the two
// grammars do not map onto each other, so there is nothing transport-neutral
// to put here instead.
type RestoreRequest struct {
	// TaskID identifies the restore job in the job state store. The transport
	// defaults it when its contract carries no id.
	TaskID string
	// BackupName names the backup artifact to restore.
	BackupName string

	Plan   *restore.Plan
	Option *restore.Option
}

// NewRestoreJob creates both storage clients from the config and registers
// the job: validation against the backup storage first — the backup must
// exist and its meta must be readable, because a not-found backup is the
// caller's mistake and has to be answered before the job is registered —
// then the job state store. NewBackupStorage also creates the backup bucket when
// it is missing: a restore may target a bucket nothing has written yet. The
// clients are created per call; sharing them across calls is a lifecycle
// decision this layer deliberately does not make.
func NewRestoreJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, req RestoreRequest) (*RestoreJob, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return newRestoreJob(ctx, params, store, backupStorage, milvusStorage, req)
}

// newRestoreJob is NewRestoreJob with the storage clients injected, so tests
// exercise validation and registration without real storage behind the
// clients.
func newRestoreJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, backupStorage, milvusStorage storage.Client, req RestoreRequest) (*RestoreJob, error) {
	// A per-call root path is the transport forking the config, not a field
	// of the request: the artifact directory resolves from the config as
	// given.
	backupDir, backup, err := backupMeta(ctx, backupStorage, params.Backup.Storage.RootPath.Val, req.BackupName)
	if err != nil {
		return nil, err
	}

	task, err := restore.NewTask(ctx, restore.TaskArgs{
		TaskID:        req.TaskID,
		Backup:        backup,
		Plan:          req.Plan,
		Option:        req.Option,
		Params:        params,
		BackupDir:     backupDir,
		BackupStorage: backupStorage,
		MilvusStorage: milvusStorage,

		Store: store,
	})
	if err != nil {
		return nil, fmt.Errorf("app: new restore task: %w", err)
	}

	return &RestoreJob{
		task:    task,
		tracker: task.Tracker(),
		done:    make(chan struct{}),
	}, nil
}

// backupMeta reads the meta of the backup named name under rootPath,
// answering ErrBackupNotFound when nothing is persisted there. Both restore
// job constructors do exactly this dance before they can build a task.
func backupMeta(ctx context.Context, cli storage.Client, rootPath, name string) (string, *backuppb.BackupInfo, error) {
	backupDir := mpath.BackupDir(rootPath, name)
	exist, err := meta.Exist(ctx, cli, backupDir)
	if err != nil {
		return "", nil, fmt.Errorf("app: %w", err)
	}
	if !exist {
		return "", nil, fmt.Errorf("app: backup %s: %w", name, ErrBackupNotFound)
	}

	backup, err := meta.Read(ctx, cli, backupDir)
	if err != nil {
		return "", nil, fmt.Errorf("app: read backup: %w", err)
	}

	return backupDir, backup, nil
}
