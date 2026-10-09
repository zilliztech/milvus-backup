package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// RestoreJob is one registered restore job, ready to execute. A job is what
// a restore call makes: the restored collections are what a successful job
// leaves behind in the target Milvus.
//
// Registering a job and running it are separate steps: registration happens
// in NewRestoreJob, synchronously, so a missing backup or an unreadable meta
// is the caller's immediate answer. Running is Run, and where its goroutine
// goes — the request path or a background one — is the transport's
// deployment decision.
type RestoreJob struct {
	task *restore.Task
}

// Run executes the job. The outcome is also recorded in the task manager,
// where get_restore and the task API read it from.
func (j *RestoreJob) Run(ctx context.Context) error { return j.task.Execute(ctx) }

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

// NewRestoreJob creates both storage clients from the config and registers
// the job: validation against the backup storage first — the backup must
// exist and its meta must be readable, because a not-found backup is the
// caller's mistake and has to be answered before the job is registered —
// then the task manager. NewBackupStorage also creates the backup bucket when
// it is missing: a restore may target a bucket nothing has written yet. The
// clients are created per call; sharing them across calls is a lifecycle
// decision this layer deliberately does not make.
func NewRestoreJob(ctx context.Context, params *cfg.Config, taskMgr *taskmgr.Mgr, req RestoreRequest) (*RestoreJob, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return newRestoreJob(ctx, params, taskMgr, backupStorage, milvusStorage, req)
}

// newRestoreJob is NewRestoreJob with the storage clients injected, so tests
// exercise validation and registration without real storage behind the
// clients.
func newRestoreJob(ctx context.Context, params *cfg.Config, taskMgr *taskmgr.Mgr, backupStorage, milvusStorage storage.Client, req RestoreRequest) (*RestoreJob, error) {
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

		TaskMgr: taskMgr,
	})
	if err != nil {
		return nil, fmt.Errorf("app: new restore task: %w", err)
	}

	return &RestoreJob{task: task}, nil
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
