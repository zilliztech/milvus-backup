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

	"github.com/zilliztech/milvus-backup/core/tasklet"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const (
	_bulkInsertTimeout       = 60 * time.Minute
	_bulkInsertCheckInterval = 3 * time.Second
)

// importJobFactory builds the import jobs that restore one batch. The v1 grpc
// API takes one directory pair per call, so a batch becomes one job per
// partition dir; the v2 restful API takes them all at once, so a batch is one
// job.
type importJobFactory func(partitionName string, b batch) []tasklet.Tasklet

// importJobBase holds what every flavor of import job needs: the data to load,
// the optional copy task that stages it where Milvus can read it, and the
// progress reporting. A job is self-contained: it prepares its data, submits
// the import, waits for it to finish, and removes its own temp files.
type importJobBase struct {
	taskID string
	target collref.Name

	partitionName  string
	dirs           []partitionDir
	timestamp      uint64
	isL0           bool
	storageVersion int64
	ezk            string

	// copy stages the data into the target's storage. nil when backup and
	// target share a bucket, in which case Milvus imports the backup in place.
	copy *copyTask

	keepTempFiles bool

	milvusStorage storage.Client
	// milvusLocalPath is what the Milvus process itself resolves
	// (milvus.storage.localPath, falling back to rootPath). Empty unless the
	// target storage provider is local.
	milvusLocalPath string

	taskMgr *taskmgr.Mgr
	logger  *zap.Logger
}

// prepare stages the data where Milvus can read it. With no copy task the dirs
// already point into the shared bucket and are handed over as-is.
func (j *importJobBase) prepare(ctx context.Context) error {
	if j.copy == nil {
		return nil
	}

	dirs, err := j.copy.Execute(ctx, j.dirs)
	if err != nil {
		return fmt.Errorf("restore_collection: import job copy files: %w", err)
	}
	j.dirs = dirs
	return nil
}

// cleanup removes the temp files the copy staged. It runs once the import is
// over, succeeded or not: the import has either consumed the data or never will.
func (j *importJobBase) cleanup(ctx context.Context) {
	if j.copy == nil || j.keepTempFiles {
		return
	}
	if err := j.copy.Cleanup(ctx); err != nil {
		j.logger.Warn("clean temporary restore files failed", zap.Error(err))
	}
}

// importPath maps a bucket-relative restore key onto the path handed to the
// bulk insert API. A local target needs the path the Milvus process resolves
// (milvus.storage.localPath, falling back to rootPath), with a trailing slash
// kept because the LocalChunkManager lists a prefix by globbing it directly;
// paths already absolute are left alone so a same-directory local backup can be
// imported without a copy. Any other provider keeps the bucket-relative path
// as-is.
func (j *importJobBase) importPath(p string) string {
	if j.milvusStorage.Config().Provider != v2.ProviderLocal || p == "" {
		return p
	}
	if path.IsAbs(p) {
		return p
	}
	return joinLocal(j.milvusLocalPath, p)
}

// grpcImportJob imports one partition dir through the v1 grpc bulk insert API.
type grpcImportJob struct {
	importJobBase

	grpcCli milvus.Grpc
}

func (j *grpcImportJob) Execute(ctx context.Context) error {
	defer j.cleanup(ctx)

	if err := j.prepare(ctx); err != nil {
		return err
	}

	jobID, err := j.submit(ctx)
	if err != nil {
		return err
	}

	return j.wait(ctx, jobID)
}

// toGrpcPaths builds the [insertLogDir, deltaLogDir] argument for the grpc bulk
// insert API, mapping each directory through the target storage's path
// convention (absolute paths under localPath for a local target).
func (j *grpcImportJob) toGrpcPaths(dir partitionDir) []string {
	if len(dir.insertLogDir) == 0 {
		return []string{j.importPath(dir.deltaLogDir)}
	}
	return []string{j.importPath(dir.insertLogDir), j.importPath(dir.deltaLogDir)}
}

func (j *grpcImportJob) submit(ctx context.Context) (int64, error) {
	// a grpc job carries exactly one partition dir: the v1 API takes one pair
	// of log dirs per call.
	dir := j.dirs[0]

	paths := j.toGrpcPaths(dir)
	j.logger.Info("start bulk insert via grpc", zap.Strings("paths", paths), zap.String("partition", j.partitionName))
	in := milvus.GrpcBulkInsertInput{
		DB:             j.target.DBName(),
		CollectionName: j.target.CollName(),
		PartitionName:  j.partitionName,
		Paths:          paths,
		BackupTS:       j.timestamp,
		IsL0:           j.isL0,
		StorageVersion: j.storageVersion,
		EZK:            j.ezk,
	}

	jobID, err := j.grpcCli.BulkInsert(ctx, in)
	if err != nil {
		return 0, fmt.Errorf("restore_collection: failed to bulk insert via grpc: %w", err)
	}
	j.taskMgr.UpdateRestoreTask(j.taskID,
		taskmgr.AddRestoreImportJob(j.target, strconv.FormatInt(jobID, 10), dir.size))
	j.logger.Info("create bulk insert via grpc success", zap.Int64("job_id", jobID))

	return jobID, nil
}

func (j *grpcImportJob) wait(ctx context.Context, jobID int64) error {
	// wait for bulk insert job done
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		state, err := j.grpcCli.GetBulkInsertState(ctx, jobID)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to get bulk insert state: %w", err)
		}

		j.logger.Info("bulk insert task state", zap.Int64("jobID", jobID), zap.Any("state", state.State),
			zap.Any("backup", state.Infos))
		switch state.State {
		case commonpb.ImportState_ImportFailed:
			return fmt.Errorf("restore_collection: bulk insert failed: %s", getFailedReason(state.Infos))
		case commonpb.ImportState_ImportCompleted:
			j.logger.Info("bulk insert task success", zap.Int64("job_id", jobID))
			return nil
		default:
			currentProgress := getProcess(state.Infos)
			j.taskMgr.UpdateRestoreTask(j.taskID,
				taskmgr.UpdateRestoreImportJob(j.target, strconv.FormatInt(jobID, 10), currentProgress))
			if currentProgress > lastProgress {
				lastProgress = currentProgress
				lastUpdateTime = time.Now()
			} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
				j.logger.Warn("bulk insert task no progress for too long, may milvus is not healthy",
					zap.Int64("job_id", jobID),
					zap.Duration("timeout", _bulkInsertTimeout))
				lastUpdateTime = time.Now()
			}
			continue
		}
	}

	return errors.New("restore_collection: walk into unreachable code")
}

// restfulImportJob imports a whole batch through the v2 restful bulk insert API.
type restfulImportJob struct {
	importJobBase

	restfulCli milvus.Restful
}

func (j *restfulImportJob) Execute(ctx context.Context) error {
	defer j.cleanup(ctx)

	if err := j.prepare(ctx); err != nil {
		return err
	}

	jobID, err := j.submit(ctx)
	if err != nil {
		return err
	}

	return j.wait(ctx, jobID)
}

// toPaths builds the [insertLogDir, deltaLogDir] argument for the restful bulk
// insert API, mapping each directory through the target storage's path
// convention (absolute paths under localPath for a local target).
func (j *restfulImportJob) toPaths(dir partitionDir) []string {
	paths := make([]string, 0, 2)
	if dir.insertLogDir != "" {
		paths = append(paths, j.importPath(dir.insertLogDir))
	}
	if dir.deltaLogDir != "" {
		paths = append(paths, j.importPath(dir.deltaLogDir))
	}
	return paths
}

func (j *restfulImportJob) submit(ctx context.Context) (string, error) {
	j.logger.Info("start bulk insert via restful",
		zap.Int("dir_num", len(j.dirs)), zap.String("partition", j.partitionName))
	paths := lo.Map(j.dirs, func(dir partitionDir, _ int) []string { return j.toPaths(dir) })
	in := milvus.BulkInsertV2Input{
		DB:             j.target.DBName(),
		CollectionName: j.target.CollName(),
		PartitionName:  j.partitionName,
		Paths:          paths,
		BackupTS:       j.timestamp,
		IsL0:           j.isL0,
		StorageVersion: j.storageVersion,
		EZK:            j.ezk,
	}

	jobID, err := j.restfulCli.BulkInsert(ctx, in)
	if err != nil {
		return "", fmt.Errorf("restore_collection: failed to bulk insert via restful: %w", err)
	}
	j.logger.Info("create bulk insert via restful success", zap.String("job_id", jobID))

	size := lo.SumBy(j.dirs, func(dir partitionDir) int64 { return dir.size })
	j.taskMgr.UpdateRestoreTask(j.taskID, taskmgr.AddRestoreImportJob(j.target, jobID, size))

	return jobID, nil
}

func (j *restfulImportJob) wait(ctx context.Context, jobID string) error {
	// wait for bulk insert job done
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		resp, err := j.restfulCli.GetBulkInsertState(ctx, j.target.DBName(), jobID)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to get bulk insert state: %w", err)
		}

		j.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return fmt.Errorf("restore_collection: bulk insert failed: %s", resp.Data.Reason)
		case string(milvus.ImportStateCompleted):
			j.logger.Info("bulk insert task success", zap.String("job_id", jobID))
			j.taskMgr.UpdateRestoreTask(j.taskID, taskmgr.UpdateRestoreImportJob(j.target, jobID, 100))
			return nil
		default:
			currentProgress := resp.Data.Progress
			j.taskMgr.UpdateRestoreTask(j.taskID, taskmgr.UpdateRestoreImportJob(j.target, jobID, currentProgress))
			if currentProgress > lastProgress {
				lastProgress = currentProgress
				lastUpdateTime = time.Now()
			} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
				j.logger.Warn("bulk insert task no progress for too long, may milvus is not healthy",
					zap.String("job_id", jobID),
					zap.Duration("timeout", _bulkInsertTimeout))
				lastUpdateTime = time.Now()
			}
			continue
		}
	}

	return errors.New("restore_collection: walk into unreachable code")
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
