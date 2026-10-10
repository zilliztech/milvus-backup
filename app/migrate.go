package app

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/migrate"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/cloud"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// MigrateRequest describes one migrate job. TaskID is the id the job
// registers under in the job store; BackupName is the artifact it uploads;
// ClusterID is the Zilliz Cloud cluster the backup migrates to.
type MigrateRequest struct {
	TaskID     string
	BackupName string
	ClusterID  string
}

// MigrateJob is one registered migrate run — the migrate counterpart of
// DeleteJob. Registering (NewMigrateJob) is synchronous and proves the
// backup's meta is readable: a migrate that cannot prove what it is
// uploading is the caller's immediate answer, and there is no task object
// that can be run unregistered.
type MigrateJob struct {
	// task is the migrate run itself. Held as the shared tasklet interface
	// so tests stub the run and exercise the job's lifecycle without cloud
	// storage behind the copy.
	task    tasklet.Tasklet
	tracker *jobstate.MigrateTracker

	// done closes when the run settles. Wait selects on it, so the caller
	// learns the outcome without polling the job store.
	done chan struct{}
}

// NewMigrateJob creates the cloud and backup storage clients from the config
// and registers the job. The clients are created per call; sharing one across
// calls is a lifecycle decision this layer deliberately does not make.
func NewMigrateJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, req MigrateRequest) (*MigrateJob, error) {
	cloudCli := cloud.NewClient(params.Cloud.Endpoint.Val, params.Cloud.APIKey.Val)

	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return newMigrateJob(ctx, store, cloudCli, backupStorage, params, req)
}

// newMigrateJob is NewMigrateJob with the clients injected, so tests exercise
// admission and registration without real storage behind them.
func newMigrateJob(ctx context.Context, store *jobstate.Store, cloudCli cloud.Client, backupStorage storage.Client, params *cfg.Config, req MigrateRequest) (*MigrateJob, error) {
	backupDir := mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.BackupName)
	backupInfo, err := meta.Read(ctx, backupStorage, backupDir)
	if err != nil {
		return nil, fmt.Errorf("app: read backup info: %w", err)
	}

	tracker := store.AddMigrateTask(req.TaskID, backupInfo.GetSize())

	task := migrate.NewTask(migrate.TaskArgs{
		TaskID:        req.TaskID,
		ClusterID:     req.ClusterID,
		CloudCli:      cloudCli,
		BackupStorage: backupStorage,
		BackupDir:     backupDir,
		Tracker:       tracker,
		Concurrency:   params.Transfer.Concurrency.Val,
	})

	return &MigrateJob{task: task, tracker: tracker, done: make(chan struct{})}, nil
}

// Run releases the job into the background and answers nothing: once the job
// is released there is nobody on the call path who could act on an error, so
// the outcome lives only in the job store, where Wait reads it.
func (j *MigrateJob) Run(ctx context.Context) {
	j.tracker.SetRunning()

	go j.run(ctx)
}

// run is the job's whole body. Whatever the task answers settles the
// store: a migrate that dies mid-copy must not leave its snapshot looking
// like it is still running.
func (j *MigrateJob) run(ctx context.Context) {
	defer close(j.done)

	if err := j.task.Execute(ctx); err != nil {
		j.tracker.SetFail(err)
		log.Error("migrate backup fail", zap.String("task_id", j.tracker.Snapshot().ID), zap.Error(err))
		return
	}

	j.tracker.SetSuccess()
	log.Info("migrate backup done", zap.String("migrate_job_id", j.tracker.Snapshot().MigrateJobID))
}

// Wait blocks until a Run settles and answers with the final status. It is
// only meaningful after Run; waiting on a job that was never started blocks
// until the context is done.
func (j *MigrateJob) Wait(ctx context.Context) (jobstate.MigrateStatus, error) {
	select {
	case <-ctx.Done():
		return jobstate.MigrateStatus{}, ctx.Err()
	case <-j.done:
		return j.tracker.Snapshot(), nil
	}
}
