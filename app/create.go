package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/backup"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// BackupJob is one registered create-backup run, ready to execute. Starting a
// job and running it are separate steps so a transport that executes jobs
// asynchronously can decide itself where the goroutine goes.
type BackupJob interface {
	// Run executes the job. The outcome is also recorded in the task manager,
	// where get_backup and the task API read it from.
	Run(ctx context.Context) error
}

// CreateBackup registers backup jobs. A job is what a create call makes: the
// backup artifact is what a successful job leaves behind in storage, a
// different resource with its own usecase (GetBackup). This usecase therefore
// answers with the job and nothing of the artifact — the split the v2 API
// draws between jobs/backup/create, which returns the job, and
// backups/describe, which reads the artifact.
type CreateBackup struct {
	params *v2.Config

	milvusStorage storage.Client
	backupStorage storage.Client

	taskMgr  *taskmgr.Mgr
	rootPath string
}

// NewCreateBackup builds the usecase from config and the given task manager,
// creating both storage clients itself so the transports never import
// internal/storage. The clients are created per call; sharing them across
// calls is a lifecycle decision this layer deliberately does not make.
func NewCreateBackup(ctx context.Context, params *v2.Config, taskMgr *taskmgr.Mgr) (*CreateBackup, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return &CreateBackup{
		params:        params,
		milvusStorage: milvusStorage,
		backupStorage: backupStorage,
		taskMgr:       taskMgr,
		rootPath:      params.Backup.Storage.RootPath.Val,
	}, nil
}

// CreateBackupRequest describes one backup job. It is the transport-neutral
// whole of what the action accepts: both transports derive the task id from
// their request-id conventions and parse their own input format into Option
// before calling.
type CreateBackupRequest struct {
	// TaskID is the id the job registers under in the task manager.
	TaskID string

	// Option carries the parsed backup parameters: the artifact name the job
	// registers under, strategy, format, collection filter, GC pause and the
	// like. Option.BackupName is also the key the job is visible under to
	// the task APIs, so it must be the one name the transport validated.
	Option backup.Option
}

// Start registers the job in the task manager and returns it ready to run.
// Registration is the synchronous part of starting: from here on the job is
// visible to the task APIs under its task id and backup name. Running is a
// separate step — Execute for the synchronous case, the transport's own
// goroutine for the asynchronous one.
func (uc *CreateBackup) Start(req CreateBackupRequest) (BackupJob, error) {
	task, err := backup.NewTask(uc.toArgs(req))
	if err != nil {
		return nil, fmt.Errorf("app: new backup task: %w", err)
	}

	return backupJob{task: task}, nil
}

// Execute runs the job synchronously on the calling goroutine and returns the
// task manager's view of it: id, state, progress and the rest of the job half
// — the whole answer of a create call, the shape v2's jobs/backup/create
// responds with. Nothing of the produced artifact is read here; a transport
// whose contract merges the two resources (v1) assembles what it needs itself.
func (uc *CreateBackup) Execute(ctx context.Context, req CreateBackupRequest) (taskmgr.BackupTaskView, error) {
	job, err := uc.Start(req)
	if err != nil {
		return nil, err
	}

	if err := job.Run(ctx); err != nil {
		return nil, err
	}

	view, err := uc.taskMgr.GetBackupTask(req.TaskID)
	if err != nil {
		return nil, fmt.Errorf("app: get backup task: %w", err)
	}

	return view, nil
}

// backupDir resolves the artifact directory: the root path comes from the
// config the usecase was built with, the artifact name from the option. A
// per-call root path is the transport forking the config, not a field here.
func (uc *CreateBackup) backupDir(name string) string {
	return mpath.BackupDir(uc.rootPath, name)
}

func (uc *CreateBackup) toArgs(req CreateBackupRequest) backup.TaskArgs {
	return backup.TaskArgs{
		TaskID:        req.TaskID,
		Option:        req.Option,
		MilvusStorage: uc.milvusStorage,
		BackupStorage: uc.backupStorage,
		BackupDir:     uc.backupDir(req.Option.BackupName),
		Params:        uc.params,
		TaskMgr:       uc.taskMgr,
	}
}

// backupJob hides the core/backup task, the action's engine, behind the
// interface the transports see.
type backupJob struct {
	task *backup.Task
}

func (j backupJob) Run(ctx context.Context) error { return j.task.Execute(ctx) }
