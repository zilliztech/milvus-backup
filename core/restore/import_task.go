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
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

const (
	_bulkInsertTimeout       = 60 * time.Minute
	_bulkInsertCheckInterval = 3 * time.Second
)

// importTaskArgs assembles everything it takes to build and run one
// collection's import tasks: where the collection lands, how the target's
// storage is reached, and how the job is reported. The plan that picked the
// import protocol builds it; the planners and the tasks they mint hold it as a
// single field, so no import type knows any task type. It is data only —
// behavior lives in free functions and the tasks.
type importTaskArgs struct {
	taskID string
	target collref.Name
	ezk    string

	// staging reports whether import jobs copy the backup's data into the
	// target's storage first. The decision's inputs — both storages and the
	// streaming flag — are job-level invariants, so the plan makes it once and
	// the tasks below only read it.
	staging       bool
	backupDir     string
	backupStorage storage.Client
	copySem       *semaphore.Weighted

	keepTempFiles bool
	// maxSegsPerImportJob caps how many dirs one import task carries, because
	// Milvus limits how many files one restful request may carry.
	maxSegsPerImportJob int

	milvusStorage storage.Client
	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath), and milvusLocalPath is what the
	// Milvus process itself resolves (milvus.storage.localPath, falling back
	// to rootPath). Both empty unless the target storage provider is local.
	milvusRootPath  string
	milvusLocalPath string

	grpcCli    milvus.Grpc
	restfulCli milvus.Restful

	// jobs mints the collection's job halves. The planners call it when they
	// build the tasks — a half exists from plan build and binds its
	// server-side job id at submit.
	jobs   *jobstate.RestoreCollTracker
	logger *zap.Logger
}

// importPlanner turns the dir groups of one partition into the import tasks
// that restore them. It is the protocol seam of the data restore: the v1 grpc
// and v2 restful apis differ in how many dirs one import job takes, so each
// owns its planner. Planning is pure; running the tasks is the caller's job.
type importPlanner interface {
	// partitionName is empty for the collection-global L0 segments.
	planTasks(partitionName string, groups []dirGroup) []tasklet.Tasklet
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
// reaches the state the caller waits for, reporting progress on the job's half
// and warning when the job stops making progress. state fetches one snapshot
// of the job; it is what differs between the grpc and restful apis. Reaching
// the awaited state only ends the wait: settling the half is the caller's —
// the two-phase commit waits to Uncommitted, which is no completion.
func waitImportJob(
	ctx context.Context,
	logger *zap.Logger,
	job *jobstate.RestoreJobTracker,
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
			logger.Info("bulk insert task reached awaited state", zap.String("job_id", jobID))
			return nil
		}

		job.SetProgress(int32(s.progress))
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
