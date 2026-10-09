package app

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/restore/secondary"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// RestoreSecondaryJob is one registered secondary-restore job, ready to
// execute. A secondary restore replays the source cluster's DDL verbatim
// under the target cluster's id. Registration happens in
// NewRestoreSecondaryJob, synchronously; running is Run, exactly as on
// RestoreJob.
type RestoreSecondaryJob struct {
	task *secondary.Task
}

// Run executes the job. The outcome is also recorded in the job state store,
// where get_restore and the task API read it from.
func (j *RestoreSecondaryJob) Run(ctx context.Context) error { return j.task.Execute(ctx) }

// RestoreSecondaryRequest selects one secondary restore.
type RestoreSecondaryRequest struct {
	// TaskID identifies the restore job in the job state store. The transport
	// defaults it when its contract carries no id.
	TaskID string
	// BackupName names the backup artifact to restore.
	BackupName string
	// SourceClusterID is the cluster the backup's DDL was taken from.
	SourceClusterID string
	// TargetClusterID is the cluster the DDL is replayed under.
	TargetClusterID string
}

// NewRestoreSecondaryJob creates both storage clients from the config and
// registers one secondary-restore job: validation against the backup storage
// first, then the job state store. The clients are created per call; sharing
// them across calls is a lifecycle decision this layer deliberately does not
// make.
func NewRestoreSecondaryJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, req RestoreSecondaryRequest) (*RestoreSecondaryJob, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return newRestoreSecondaryJob(ctx, params, store, backupStorage, milvusStorage, req)
}

// newRestoreSecondaryJob is NewRestoreSecondaryJob with the storage clients
// injected, so tests exercise validation and registration without real
// storage behind the clients.
func newRestoreSecondaryJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, backupStorage, milvusStorage storage.Client, req RestoreSecondaryRequest) (*RestoreSecondaryJob, error) {
	backupDir, backup, err := backupMeta(ctx, backupStorage, params.Backup.Storage.RootPath.Val, req.BackupName)
	if err != nil {
		return nil, err
	}

	task, err := secondary.NewTask(secondary.TaskArgs{
		TaskID: req.TaskID,

		SourceClusterID: req.SourceClusterID,
		TargetClusterID: req.TargetClusterID,

		Backup:        backup,
		Params:        params,
		BackupDir:     backupDir,
		BackupStorage: backupStorage,
		MilvusStorage: milvusStorage,

		Store: store,
	})
	if err != nil {
		return nil, fmt.Errorf("app: new restore task: %w", err)
	}

	return &RestoreSecondaryJob{task: task}, nil
}
