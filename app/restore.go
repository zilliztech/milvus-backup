package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// RestoreJob is one assembled restore job: its task exists and the task
// manager knows it, but nothing has run yet. The transport decides how the
// job executes — synchronously, or in the transport's own goroutine when it
// restores asynchronously.
type RestoreJob interface {
	// Run executes the job to completion. The outcome is also recorded in
	// the task manager, where get_restore and the task API read it from.
	Run(ctx context.Context) error
}

// Restore restores a backup into the target Milvus.
type Restore struct {
	params *v2.Config

	backupStorage storage.Client
	milvusStorage storage.Client

	taskMgr  *taskmgr.Mgr
	rootPath string
}

// NewRestore builds the usecase from config and the given task manager,
// creating both storage clients itself so the transports never import
// internal/storage. NewBackupStorage also creates the backup bucket when it
// is missing: a restore may target a bucket nothing has written yet. The
// clients are created per call; sharing them across calls is a lifecycle
// decision this layer deliberately does not make.
func NewRestore(ctx context.Context, params *v2.Config, taskMgr *taskmgr.Mgr) (*Restore, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return &Restore{
		params:        params,
		backupStorage: backupStorage,
		milvusStorage: milvusStorage,
		taskMgr:       taskMgr,
		rootPath:      params.Backup.Storage.RootPath.Val,
	}, nil
}

// RestoreRequest selects and shapes one restore. Plan and Option are the
// restore task's own vocabulary on purpose: each transport translates its
// grammar into them — the CLI from flags, v1 from its pb fields — and the two
// grammars do not map onto each other, so there is nothing transport-neutral
// to put here instead.
type RestoreRequest struct {
	// TaskID identifies the restore job in the task manager. The transport
	// defaults it when its contract carries no id.
	TaskID string
	// BackupName names the backup artifact to restore.
	BackupName string

	Plan   *restore.Plan
	Option *restore.Option
}

// Start validates the request against the backup storage — the backup must
// exist and its meta must be readable, because a not-found backup is the
// caller's mistake and has to be answered before the job is registered — and
// builds the task, which is what registers the job. Running is a separate
// step — Run for the synchronous case, the transport's own goroutine for the
// asynchronous one.
func (uc *Restore) Start(ctx context.Context, req RestoreRequest) (RestoreJob, error) {
	backupDir, backup, err := backupMeta(ctx, uc.backupStorage, uc.rootPath, req.BackupName)
	if err != nil {
		return nil, err
	}

	args := restore.TaskArgs{
		TaskID:        req.TaskID,
		Backup:        backup,
		Plan:          req.Plan,
		Option:        req.Option,
		Params:        uc.params,
		BackupDir:     backupDir,
		BackupStorage: uc.backupStorage,
		MilvusStorage: uc.milvusStorage,

		TaskMgr: uc.taskMgr,
	}
	task, err := restore.NewTask(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("app: new restore task: %w", err)
	}

	return &restoreJob{task: task}, nil
}

// backupMeta reads the meta of the backup named name under rootPath,
// answering ErrBackupNotFound when nothing is persisted there. Both restore
// usecases do exactly this dance before they can build a task.
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

// restoreJob hides the two task implementations behind the interface the
// transports see.
type restoreJob struct {
	task tasklet.Tasklet
}

func (j *restoreJob) Run(ctx context.Context) error { return j.task.Execute(ctx) }
