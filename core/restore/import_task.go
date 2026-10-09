package restore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const (
	_bulkInsertTimeout       = 60 * time.Minute
	_bulkInsertCheckInterval = 3 * time.Second
)

// importPlanner turns the dir groups of one partition into the import tasks
// that restore them. It is the protocol seam of the data restore: the v1 grpc
// and v2 restful apis differ in how many dirs one import job takes, so each
// owns its planner. Planning is pure; running the tasks is the caller's job.
type importPlanner interface {
	// partitionName is empty for the collection-global L0 segments.
	planTasks(partitionName string, groups []dirGroup) []tasklet.Tasklet
}

// runTasks runs the import tasks concurrently under the bulk insert semaphore.
func runTasks(ctx context.Context, sem *semaphore.Weighted, tasks []tasklet.Tasklet) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("restore_collection: acquire bulk insert semaphore: %w", err)
		}

		g.Go(func() error {
			defer sem.Release(1)

			if err := task.Execute(subCtx); err != nil {
				return fmt.Errorf("restore_collection: execute import task: %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for import tasks: %w", err)
	}

	return nil
}

// importJobState is one snapshot of an import job, normalized across the grpc
// and restful apis.
type importJobState struct {
	progress int
	// completed reports whether the job reached its success terminal state.
	completed bool
	// failedReason is non-empty when the job failed, carrying why it did.
	failedReason string
}

// waitImportJob polls an import job every _bulkInsertCheckInterval until it
// reaches a terminal state, reporting progress to the task manager and
// warning when the job stops making progress. state fetches one snapshot of
// the job; it is what differs between the grpc and restful apis.
func waitImportJob(
	ctx context.Context,
	logger *zap.Logger,
	taskMgr *taskmgr.Mgr,
	taskID string,
	target collref.Name,
	jobID string,
	state func(ctx context.Context) (importJobState, error),
) error {
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		s, err := state(ctx)
		if err != nil {
			return fmt.Errorf("restore: failed to get bulk insert state: %w", err)
		}

		if s.failedReason != "" {
			return fmt.Errorf("restore: bulk insert failed: %s", s.failedReason)
		}
		if s.completed {
			logger.Info("bulk insert task success", zap.String("job_id", jobID))
			return nil
		}

		taskMgr.UpdateRestoreTask(taskID, taskmgr.UpdateRestoreImportJob(target, jobID, s.progress))
		if s.progress > lastProgress {
			lastProgress = s.progress
			lastUpdateTime = time.Now()
		} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
			logger.Warn("bulk insert task no progress for too long, may milvus is not healthy",
				zap.String("job_id", jobID),
				zap.Duration("timeout", _bulkInsertTimeout))
			lastUpdateTime = time.Now()
		}
	}

	return errors.New("restore: walk into unreachable code")
}

// importPath maps a bucket-relative restore key onto the path handed to the
// bulk insert API. A local target needs the path the Milvus process resolves
// (milvus.storage.localPath, falling back to rootPath), with a trailing slash
// kept because the LocalChunkManager lists a prefix by globbing it directly;
// paths already absolute are left alone so a same-directory local backup can be
// imported without a copy. Any other provider keeps the bucket-relative path
// as-is.
func importPath(cli storage.Client, localPath, p string) string {
	if cli.Config().Provider != cfg.ProviderLocal || p == "" {
		return p
	}
	if path.IsAbs(p) {
		return p
	}
	return joinLocal(localPath, p)
}

func getProcess(infos []*commonpb.KeyValuePair) int {
	m := lo.SliceToMap(infos, func(info *commonpb.KeyValuePair) (string, string) {
		return info.Key, info.Value
	})
	if val, ok := m["progress_percent"]; ok {
		progress, err := strconv.Atoi(val)
		if err != nil {
			return 0
		}
		return progress
	}
	return 0
}

func getFailedReason(infos []*commonpb.KeyValuePair) string {
	m := lo.SliceToMap(infos, func(info *commonpb.KeyValuePair) (string, string) {
		return info.Key, info.Value
	})

	if val, ok := m["failed_reason"]; ok {
		return val
	}
	return ""
}
