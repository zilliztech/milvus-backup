package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// ErrStorageNotReady means storage admission failed before a job was registered.
// Transports may ask the caller to retry the same create request.
var ErrStorageNotReady = errors.New("storage not ready")

const createStoragePreflightTimeout = 10 * time.Second

// BackupJob is one registered create-backup run — the create counterpart of
// DeleteJob. A job is what a create call makes: the backup artifact is what a
// successful job leaves behind in storage, a different resource with its own
// usecase (GetBackup) — the split the v2 API draws between jobs/backup/create,
// which returns the job, and backups/describe, which reads the artifact.
//
// Registering a job and running it are separate steps: admission happens in
// NewBackupJob, synchronously, so a duplicate name, an unreachable storage or
// an artifact name that is already taken is the caller's immediate answer, and
// a registered job is visible in the task APIs before it runs. Running is Run,
// which releases the job into the background; Wait blocks on the settlement.
type BackupJob struct {
	// task is the backup run itself. Held as the shared tasklet interface so
	// tests stub the run and exercise the job's lifecycle without a cluster
	// behind the backup.
	task tasklet.Tasklet
	view jobstate.BackupTaskView

	// done closes when the run settles. Wait selects on it, so the caller
	// learns the outcome without polling the job store.
	done chan struct{}
}

var _ AsyncJob[jobstate.BackupTaskView] = (*BackupJob)(nil)

// Run releases the job into the background and answers nothing: once the job
// is released there is nobody on the call path who could act on an error, so
// the outcome lives only in the job store, where Wait and the task APIs read
// it. The context must be process-scoped, not the request's — see AsyncJob.Run.
func (j *BackupJob) Run(ctx context.Context) {
	go j.run(ctx)
}

// run is the job's whole body. The task settles its own terminal state: it
// records Success or Fail in the job store before it answers, so a backup
// that dies mid-copy does not leave its snapshot looking like it is still
// running.
func (j *BackupJob) run(ctx context.Context) {
	defer close(j.done)

	start := time.Now()
	if err := j.task.Execute(ctx); err != nil {
		log.Error("create backup fail", zap.String("backup_name", j.view.Name()), zap.Error(err))
		return
	}

	log.Info("create backup done", zap.String("backup_name", j.view.Name()), zap.Duration("cost", time.Since(start)))
}

// Wait blocks until a Run settles and answers with the job's view in the job
// store — the terminal state, error message and progress are on it. It is
// only meaningful after Run; waiting on a job that was never started blocks
// until the context is done.
func (j *BackupJob) Wait(ctx context.Context) (jobstate.BackupTaskView, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-j.done:
		return j.view, nil
	}
}

// CreateBackupRequest describes one backup job. It is the transport-neutral
// whole of what the action accepts: both transports derive the task id from
// their request-id conventions and parse their own input format into Option
// before calling.
type CreateBackupRequest struct {
	// TaskID is the id the job registers under in the job state store.
	TaskID string

	// Option carries the parsed backup parameters: the artifact name the job
	// registers under, strategy, format, collection filter, GC pause and the
	// like. Option.BackupName is also the key the job is visible under to
	// the task APIs, so it must be the one name the transport validated.
	Option backup.Option
}

// NewBackupJob creates both storage clients from the config and registers the
// job: source list preflight, artifact dir preflight, then the job state
// store. The clients are created per call; sharing them across calls is a
// lifecycle decision this layer deliberately does not make.
func NewBackupJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, req CreateBackupRequest) (*BackupJob, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return newBackupJob(ctx, params, store, milvusStorage, backupStorage, req)
}

// newBackupJob is NewBackupJob with the storage clients injected, so tests
// exercise admission and registration without real storage behind the clients.
func newBackupJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, milvusStorage, backupStorage storage.Client, req CreateBackupRequest) (*BackupJob, error) {
	if err := preflightSourceStorage(ctx, params, milvusStorage, req.Option.Strategy); err != nil {
		return nil, err
	}

	// A per-call root path is the transport forking the config, not a field of
	// the request: the artifact directory resolves from the config as given.
	backupDir := mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.Option.BackupName)
	if err := preflightBackupDir(ctx, backupStorage, backupDir, req.Option.BackupName); err != nil {
		return nil, err
	}

	task, err := backup.NewTask(backup.TaskArgs{
		TaskID:        req.TaskID,
		Option:        req.Option,
		MilvusStorage: milvusStorage,
		BackupStorage: backupStorage,
		BackupDir:     backupDir,
		Params:        params,
		Store:         store,
	})
	if err != nil {
		return nil, fmt.Errorf("app: new backup task: %w", err)
	}

	view, err := store.GetBackupTask(req.TaskID)
	if err != nil {
		return nil, fmt.Errorf("app: fetch registered backup task: %w", err)
	}

	return &BackupJob{task: task, view: view, done: make(chan struct{})}, nil
}

func preflightSourceStorage(ctx context.Context, params *cfg.Config, milvusStorage storage.Client, strategy backup.Strategy) error {
	if strategy == backup.StrategyMetaOnly {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, createStoragePreflightTimeout)
	defer cancel()
	prefix := mpath.MilvusInsertLogDir(params.Milvus.Storage.RootPath.Val)
	// The sequence lists lazily: the range itself is what sends the request,
	// so one iteration is the least work that proves access; an empty result
	// also means the request succeeded. A yield that races context
	// cancellation still counts: an object came back, so access is real.
	for _, err := range milvusStorage.NewObjectIter(ctx, prefix, true) {
		if err != nil {
			conf := milvusStorage.Config()
			return fmt.Errorf("app: %w: source list preflight failed (provider=%s, endpoint=%s, bucket=%s, prefix=%s): %w",
				ErrStorageNotReady, conf.Provider, conf.Endpoint, conf.Bucket, prefix, err)
		}
		// One listed object proves access; returning here stops the listing
		// through the sequence itself.
		return nil
	}
	return nil
}

// preflightBackupDir refuses an artifact directory that already holds a
// backup. It is admission, not a task step: a name that cannot be written is
// the caller's immediate answer, not a failure recorded on a job that is
// already registered.
func preflightBackupDir(ctx context.Context, backupStorage storage.Client, backupDir, backupName string) error {
	ctx, cancel := context.WithTimeout(ctx, createStoragePreflightTimeout)
	defer cancel()
	exist, err := storage.Exist(ctx, backupStorage, backupDir)
	if err != nil {
		conf := backupStorage.Config()
		return fmt.Errorf("app: %w: backup dir preflight failed (provider=%s, endpoint=%s, bucket=%s, prefix=%s): %w",
			ErrStorageNotReady, conf.Provider, conf.Endpoint, conf.Bucket, backupDir, err)
	}
	if exist {
		return fmt.Errorf("app: backup with name %s already exist", backupName)
	}
	return nil
}
