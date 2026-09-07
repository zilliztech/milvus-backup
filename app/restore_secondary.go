package app

import (
	"context"
	"fmt"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"

	"github.com/zilliztech/milvus-backup/core/restore/secondary"
)

// RestoreSecondary restores a backup to a secondary cluster: it replays the
// source cluster's DDL verbatim under the target cluster's id.
type RestoreSecondary struct {
	params *v2.Config

	// newBackupStorage and newMilvusStorage build the two clients one restore
	// runs on. They are per-Start calls, not constructor state, and fields so
	// tests inject mocks instead of a real backend.
	newBackupStorage func(ctx context.Context) (storage.Client, error)
	newMilvusStorage func(ctx context.Context) (storage.Client, error)
}

// NewRestoreSecondary builds the usecase from config.
func NewRestoreSecondary(params *v2.Config) *RestoreSecondary {
	return &RestoreSecondary{
		params: params,
		newBackupStorage: func(ctx context.Context) (storage.Client, error) {
			cli, err := storage.NewBackupStorage(ctx, params)
			if err != nil {
				return nil, fmt.Errorf("app: create backup storage: %w", err)
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

// RestoreSecondaryRequest selects one secondary restore.
type RestoreSecondaryRequest struct {
	// TaskID identifies the restore job in the task manager. The transport
	// defaults it when its contract carries no id.
	TaskID string
	// BackupName names the backup artifact to restore.
	BackupName string
	// SourceClusterID is the cluster the backup's DDL was taken from.
	SourceClusterID string
	// TargetClusterID is the cluster the DDL is replayed under.
	TargetClusterID string
	// Path overrides the configured backup root path when set.
	Path string
}

// Start assembles the restore job and registers it with the task manager,
// executing nothing: the backup storage client is created, the backup's
// existence is checked, its meta is read, the target cluster's own storage
// client is created, and the task is built — task creation is what registers
// the job. Execute runs the returned job; a transport that restores
// asynchronously runs it in its own goroutine instead.
func (uc *RestoreSecondary) Start(ctx context.Context, req RestoreSecondaryRequest) (RestoreJob, error) {
	backupStorage, err := uc.newBackupStorage(ctx)
	if err != nil {
		return nil, err
	}

	backupRootPath := uc.params.Backup.Storage.RootPath.Val
	if req.Path != "" {
		backupRootPath = req.Path
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

	milvusStorage, err := uc.newMilvusStorage(ctx)
	if err != nil {
		return nil, err
	}

	args := secondary.TaskArgs{
		TaskID: req.TaskID,

		SourceClusterID: req.SourceClusterID,
		TargetClusterID: req.TargetClusterID,

		Backup:        backup,
		Params:        uc.params,
		BackupDir:     backupDir,
		BackupStorage: backupStorage,
		MilvusStorage: milvusStorage,

		TaskMgr: taskmgr.DefaultMgr(),
	}
	task, err := secondary.NewTask(args)
	if err != nil {
		return nil, fmt.Errorf("app: new restore task: %w", err)
	}

	return &restoreJob{task: task, taskID: req.TaskID}, nil
}

// TaskView returns the current view of the restore job registered under
// taskID.
func (uc *RestoreSecondary) TaskView(taskID string) (RestoreTaskView, error) {
	view, err := taskmgr.DefaultMgr().GetRestoreTask(taskID)
	if err != nil {
		return nil, fmt.Errorf("app: get restore task %w", err)
	}

	return view, nil
}
