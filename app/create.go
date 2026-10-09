package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// ErrStorageNotReady means storage admission failed before a job was registered.
// Transports may ask the caller to retry the same create request.
var ErrStorageNotReady = errors.New("storage not ready")

const createStoragePreflightTimeout = 10 * time.Second

// BackupJob is one registered create-backup run, ready to execute. A job is
// what a create call makes: the backup artifact is what a successful job
// leaves behind in storage, a different resource with its own usecase
// (GetBackup) — the split the v2 API draws between jobs/backup/create, which
// returns the job, and backups/describe, which reads the artifact.
//
// Registering a job and running it are separate steps: registration happens in
// NewBackupJob, synchronously, so a duplicate name or an unreachable source
// storage is the caller's immediate answer. Running is Run, and where its
// goroutine goes — the request path or a background one — is the transport's
// deployment decision.
type BackupJob struct {
	task *backup.Task
}

// Run executes the job. The outcome is also recorded in the task manager,
// where get_backup and the task API read it from.
func (j *BackupJob) Run(ctx context.Context) error { return j.task.Execute(ctx) }

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

// NewBackupJob creates both storage clients from the config and registers the
// job: source List preflight first, then the task manager. The clients are
// created per call; sharing them across calls is a lifecycle decision this
// layer deliberately does not make.
func NewBackupJob(ctx context.Context, params *cfg.Config, taskMgr *taskmgr.Mgr, req CreateBackupRequest) (*BackupJob, error) {
	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return newBackupJob(ctx, params, taskMgr, milvusStorage, backupStorage, req)
}

// newBackupJob is NewBackupJob with the storage clients injected, so tests
// exercise admission and registration without real storage behind the clients.
func newBackupJob(ctx context.Context, params *cfg.Config, taskMgr *taskmgr.Mgr, milvusStorage, backupStorage storage.Client, req CreateBackupRequest) (*BackupJob, error) {
	if err := preflightSourceStorage(ctx, params, milvusStorage, req.Option.Strategy); err != nil {
		return nil, err
	}

	// A per-call root path is the transport forking the config, not a field of
	// the request: the artifact directory resolves from the config as given.
	task, err := backup.NewTask(backup.TaskArgs{
		TaskID:        req.TaskID,
		Option:        req.Option,
		MilvusStorage: milvusStorage,
		BackupStorage: backupStorage,
		BackupDir:     mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.Option.BackupName),
		Params:        params,
		TaskMgr:       taskMgr,
	})
	if err != nil {
		return nil, fmt.Errorf("app: new backup task: %w", err)
	}

	return &BackupJob{task: task}, nil
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
