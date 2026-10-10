package app

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// DeleteBackupRequest describes one delete job. TaskID is the id the job
// registers under in the job store; BackupName is the artifact it removes.
type DeleteBackupRequest struct {
	TaskID     string
	BackupName string
}

// DeleteJob is one registered delete-backup run — the delete counterpart of
// BackupJob. Registering (NewDeleteJob) is synchronous and proves the
// artifact's meta is readable: a delete that cannot prove what it is
// deleting is refused, so a wrong root path or a corrupted backup is the
// caller's immediate answer instead of wiping whatever prefix happens to sit
// under the path.
type DeleteJob struct {
	cli       storage.Client
	backupDir string
	tracker   *jobstate.DeleteTracker

	// done closes when the run settles. Wait selects on it, so the caller
	// learns the outcome without polling the job store.
	done chan struct{}
}

// NewDeleteJob creates the backup storage client from the config and
// registers the job. The client is created per call; sharing one across
// calls is a lifecycle decision this layer deliberately does not make.
func NewDeleteJob(ctx context.Context, params *cfg.Config, store *jobstate.Store, req DeleteBackupRequest) (*DeleteJob, error) {
	cli, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return newDeleteJob(ctx, store, cli, params.Backup.Storage.RootPath.Val, req)
}

// newDeleteJob is NewDeleteJob with the storage client injected, so tests
// exercise admission and registration without real storage behind the client.
func newDeleteJob(ctx context.Context, store *jobstate.Store, cli storage.Client, rootPath string, req DeleteBackupRequest) (*DeleteJob, error) {
	backupDir := mpath.BackupDir(rootPath, req.BackupName)
	if _, err := meta.Read(ctx, cli, backupDir); err != nil {
		return nil, fmt.Errorf("app: read backup info: %w", err)
	}

	tracker, err := store.AddDeleteTask(req.TaskID, req.BackupName)
	if err != nil {
		return nil, fmt.Errorf("app: register delete task: %w", err)
	}

	return &DeleteJob{cli: cli, backupDir: backupDir, tracker: tracker, done: make(chan struct{})}, nil
}

// Run releases the job into the background and answers nothing: once the job
// is released there is nobody on the call path who could act on an error, so
// the outcome lives only in the job store, where Wait and the future async
// API read it.
func (j *DeleteJob) Run(ctx context.Context) {
	j.tracker.SetRunning()

	go j.run(ctx)
}

// run is the job's whole body. It deletes the backup directory with two
// goroutines: a lister that only counts the objects under the prefix, and
// the delete itself. The lister runs at listing speed, unthrottled by delete
// backpressure, so the progress total locks in early instead of trailing one
// buffer-length ahead of the deletion. A lister failure fails the whole job:
// a store that cannot be listed cannot be deleted either, and limping on
// would silently lose the total the split exists to provide.
func (j *DeleteJob) run(ctx context.Context) {
	defer close(j.done)

	log.Info("start delete backup", zap.String("backup_dir", j.backupDir))
	start := time.Now()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer j.tracker.SetListingDone()
		for _, err := range j.cli.NewObjectIter(ctx, j.backupDir, true) {
			if err != nil {
				return fmt.Errorf("app: count backup objects: %w", err)
			}
			j.tracker.AddDiscovered(1)
		}
		return nil
	})
	g.Go(func() error {
		return storage.DeleteWithCallback(ctx, j.cli, j.backupDir, j.tracker.AddDeleted)
	})

	if err := g.Wait(); err != nil {
		j.tracker.SetFail(err)
		log.Error("delete backup fail", zap.String("backup_dir", j.backupDir), zap.Error(err))
		return
	}

	j.tracker.SetSuccess()
	log.Info("delete backup done", zap.String("backup_dir", j.backupDir), zap.Duration("cost", time.Since(start)))
}

// Wait blocks until a Run settles and answers with the final status. It is
// only meaningful after Run; waiting on a job that was never started blocks
// until the context is done.
func (j *DeleteJob) Wait(ctx context.Context) (jobstate.DeleteStatus, error) {
	select {
	case <-ctx.Done():
		return jobstate.DeleteStatus{}, ctx.Err()
	case <-j.done:
		return j.tracker.Snapshot(), nil
	}
}
