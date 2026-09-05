package app

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// BackupNotFoundError reports a restore request naming a backup that has no
// artifact in the backup storage. Transports map it to their parameter-error
// code: the caller asked for something that is not there.
type BackupNotFoundError struct {
	Name string
}

func (e *BackupNotFoundError) Error() string {
	return fmt.Sprintf("app: backup %s not found", e.Name)
}

// RestoreTaskView aliases the task manager's view of one restore job. It is
// what a transport renders into its own wire shape; the alias exists so the
// transports need not import internal/taskmgr just to name the type.
type RestoreTaskView = taskmgr.RestoreTaskView

// RestoreJob is one assembled restore job: its task exists and the task
// manager knows it, but nothing has run yet. The transport decides how the
// job executes — synchronously via Execute, or in the transport's own
// goroutine when it restores asynchronously.
type RestoreJob interface {
	// Execute runs the job to completion.
	Execute(ctx context.Context) error

	// TaskID identifies the job in the task manager; TaskView takes it.
	TaskID() string
}

// Restore restores a backup into the target Milvus.
type Restore struct {
	params *v2.Config

	// newBackupStorage and newMilvusStorage build the two clients one restore
	// runs on. They are per-Start calls, not constructor state: the backup
	// client depends on the request's bucket override, which no constructor
	// knows. They are fields so tests inject mocks instead of a real backend.
	newBackupStorage func(ctx context.Context, bucketName string) (storage.Client, error)
	newMilvusStorage func(ctx context.Context) (storage.Client, error)
}

// NewRestore builds the usecase from config.
func NewRestore(params *v2.Config) *Restore {
	return &Restore{
		params: params,
		newBackupStorage: func(ctx context.Context, bucketName string) (storage.Client, error) {
			conf := storage.BackupStorageConfig(params)
			if bucketName != "" {
				log.Info("use bucket name from request", zap.String("bucketName", bucketName))
				conf.Bucket = bucketName
			}

			cli, err := storage.NewClient(ctx, conf)
			if err != nil {
				return nil, fmt.Errorf("app: create backup storage: %w", err)
			}
			if err := storage.CreateBucketIfNotExist(ctx, cli, ""); err != nil {
				return nil, fmt.Errorf("app: create backup bucket: %w", err)
			}

			return cli, nil
		},
		newMilvusStorage: func(ctx context.Context) (storage.Client, error) {
			cli, err := storage.NewMilvusStorage(ctx, params)
			if err != nil {
				return nil, fmt.Errorf("app: create milvus storage: %w", err)
			}

			return cli, nil
		},
	}
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
	// BucketName overrides the configured backup bucket when set.
	BucketName string
	// Path overrides the configured backup root path when set.
	Path string

	Plan   *restore.Plan
	Option *restore.Option
}

// Start assembles the restore job and registers it with the task manager,
// executing nothing: storage clients are created, the backup's existence is
// checked, its meta is read and the task is built — task creation is what
// registers the job. Execute runs the returned job; a transport that restores
// asynchronously runs it in its own goroutine instead.
func (uc *Restore) Start(ctx context.Context, req RestoreRequest) (RestoreJob, error) {
	backupStorage, err := uc.newBackupStorage(ctx, req.BucketName)
	if err != nil {
		return nil, err
	}

	backupRootPath := uc.params.Backup.Storage.RootPath.Val
	if req.Path != "" {
		log.Info("use path from request", zap.String("path", req.Path))
		backupRootPath = req.Path
	}

	milvusStorage, err := uc.newMilvusStorage(ctx)
	if err != nil {
		return nil, err
	}

	backupDir := mpath.BackupDir(backupRootPath, req.BackupName)
	exist, err := meta.Exist(ctx, backupStorage, backupDir)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}
	if !exist {
		return nil, &BackupNotFoundError{Name: req.BackupName}
	}

	backup, err := meta.Read(ctx, backupStorage, backupDir)
	if err != nil {
		return nil, fmt.Errorf("app: read backup: %w", err)
	}

	args := restore.TaskArgs{
		TaskID:        req.TaskID,
		Backup:        backup,
		Plan:          req.Plan,
		Option:        req.Option,
		Params:        uc.params,
		BackupDir:     backupDir,
		BackupStorage: backupStorage,
		MilvusStorage: milvusStorage,

		TaskMgr: taskmgr.DefaultMgr(),
	}
	task, err := restore.NewTask(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("app: new restore task: %w", err)
	}

	return &restoreJob{task: task, taskID: req.TaskID}, nil
}

// TaskView returns the current view of the restore job registered under
// taskID.
func (uc *Restore) TaskView(taskID string) (RestoreTaskView, error) {
	view, err := taskmgr.DefaultMgr().GetRestoreTask(taskID)
	if err != nil {
		return nil, fmt.Errorf("app: get restore task %w", err)
	}

	return view, nil
}

// restoreJob adapts the two task implementations to RestoreJob so the
// transports drive a job without importing the task layer.
type restoreJob struct {
	task   tasklet.Tasklet
	taskID string
}

func (j *restoreJob) Execute(ctx context.Context) error { return j.task.Execute(ctx) }
func (j *restoreJob) TaskID() string                    { return j.taskID }
