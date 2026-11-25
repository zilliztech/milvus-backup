package app

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/validate"
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
	once sync.Once

	execute func(context.Context) (jobstate.RestoreStatus, error)
	done    chan struct{}
	result  jobstate.RestoreStatus
}

var _ AsyncJob[jobstate.RestoreStatus] = (*RestoreJob)(nil)

// Run releases the job into the background using the process-scoped context.
func (j *RestoreJob) Run(ctx context.Context) { j.once.Do(func() { go j.run(ctx) }) }

func (j *RestoreJob) run(ctx context.Context) {
	defer close(j.done)
	result, err := j.execute(ctx)
	j.execute = nil // Wait only needs the result; release the bound task and its plan.
	j.result = result
	if err != nil {
		log.Error("restore task fail", zap.Error(err))
	}
}

// Wait returns the result of this job's Run. A later Resume does not change it.
func (j *RestoreJob) Wait(ctx context.Context) (jobstate.RestoreStatus, error) {
	select {
	case <-ctx.Done():
		return jobstate.RestoreStatus{}, ctx.Err()
	case <-j.done:
		return j.result.Clone(), nil
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
	if err := validate.RestoreTaskID(req.TaskID); err != nil {
		return nil, err
	}
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
		execute: task.ExecuteWithResult,
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
