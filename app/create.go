package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/meta"
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

// CreateBackup starts one backup job that writes an artifact into the backup
// storage, copying from the milvus storage.
type CreateBackup struct {
	params *v2.Config

	milvusStorage storage.Client
	backupStorage storage.Client

	taskMgr  *taskmgr.Mgr
	rootPath string
}

// NewCreateBackup builds the usecase from config, creating both storage
// clients itself so the transports never import internal/storage. The clients
// are created per call; sharing them across calls is a lifecycle decision
// this layer deliberately does not make.
func NewCreateBackup(ctx context.Context, params *v2.Config) (*CreateBackup, error) {
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
		taskMgr:       taskmgr.DefaultMgr(),
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

	// RootPath overrides the configured backup root path. Empty keeps the
	// configured one.
	RootPath string

	// Option carries the parsed backup parameters: the artifact name the job
	// registers under, strategy, format, collection filter, GC pause and the
	// like. Option.BackupName is also the key the job is visible under to
	// the task APIs, so it must be the one name the transport validated.
	Option backup.Option
}

// BackupView keeps the two resource halves separate. Merging them is a
// rendering decision of the transport, not a property of the action.
type BackupView struct {
	// Task is the job half; always known here, the job was just registered.
	Task taskmgr.BackupTaskView
	// Meta is the artifact half; non-nil once the job has written it.
	Meta     *backuppb.BackupInfo
	MetaSize int64
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
// view of what it produced: the task view of the job overlaid on the meta it
// persisted, the same shape get_backup answers with.
func (uc *CreateBackup) Execute(ctx context.Context, req CreateBackupRequest) (*BackupView, error) {
	job, err := uc.Start(req)
	if err != nil {
		return nil, err
	}

	if err := job.Run(ctx); err != nil {
		return nil, err
	}

	return uc.readView(ctx, req.TaskID, uc.backupDir(req))
}

// readView assembles the finished job's view: the task view from the manager,
// the persisted meta and the size of the meta dir that produced it.
func (uc *CreateBackup) readView(ctx context.Context, taskID, backupDir string) (*BackupView, error) {
	taskView, err := uc.taskMgr.GetBackupTask(taskID)
	if err != nil {
		return nil, fmt.Errorf("app: get backup task: %w", err)
	}

	backupInfo, err := meta.Read(ctx, uc.backupStorage, backupDir)
	if err != nil {
		return nil, fmt.Errorf("app: read backup meta: %w", err)
	}

	metaSize, err := storage.Size(ctx, uc.backupStorage, mpath.MetaDir(backupDir))
	if err != nil {
		return nil, fmt.Errorf("app: get meta size: %w", err)
	}

	return &BackupView{Task: taskView, Meta: backupInfo, MetaSize: metaSize}, nil
}

// backupDir resolves the artifact directory: the request's root path wins
// over the configured one, and the artifact name comes from the option.
func (uc *CreateBackup) backupDir(req CreateBackupRequest) string {
	rootPath := uc.rootPath
	if req.RootPath != "" {
		rootPath = req.RootPath
	}

	return mpath.BackupDir(rootPath, req.Option.BackupName)
}

func (uc *CreateBackup) toArgs(req CreateBackupRequest) backup.TaskArgs {
	return backup.TaskArgs{
		TaskID:        req.TaskID,
		Option:        req.Option,
		MilvusStorage: uc.milvusStorage,
		BackupStorage: uc.backupStorage,
		BackupDir:     uc.backupDir(req),
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
