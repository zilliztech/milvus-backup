package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/restore/secondary"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// RestoreSecondary restores a backup to a secondary cluster: it replays the
// source cluster's DDL verbatim under the target cluster's id.
type RestoreSecondary struct {
	params *v2.Config

	backupStorage storage.Client
	milvusStorage storage.Client

	taskMgr  *taskmgr.Mgr
	rootPath string
}

// NewRestoreSecondary builds the usecase from config and the given task
// manager, creating both storage clients itself so the transports never
// import internal/storage. The clients are created per call; sharing them
// across calls is a lifecycle decision this layer deliberately does not make.
func NewRestoreSecondary(ctx context.Context, params *v2.Config, taskMgr *taskmgr.Mgr) (*RestoreSecondary, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return &RestoreSecondary{
		params:        params,
		backupStorage: backupStorage,
		milvusStorage: milvusStorage,
		taskMgr:       taskMgr,
		rootPath:      params.Backup.Storage.RootPath.Val,
	}, nil
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
}

// Start validates the request against the backup storage — the backup must
// exist and its meta must be readable, because a not-found backup is the
// caller's mistake and has to be answered before the job is registered — and
// builds the task, which is what registers the job. Running is a separate
// step — Run for the synchronous case, the transport's own goroutine for the
// asynchronous one.
func (uc *RestoreSecondary) Start(ctx context.Context, req RestoreSecondaryRequest) (RestoreJob, error) {
	backupDir, backup, err := backupMeta(ctx, uc.backupStorage, uc.rootPath, req.BackupName)
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
		BackupStorage: uc.backupStorage,
		MilvusStorage: uc.milvusStorage,

		TaskMgr: uc.taskMgr,
	}
	task, err := secondary.NewTask(args)
	if err != nil {
		return nil, fmt.Errorf("app: new restore task: %w", err)
	}

	return &restoreJob{task: task}, nil
}
