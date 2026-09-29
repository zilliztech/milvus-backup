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

// importViaGRPCTask imports one partition dir through the v1 grpc bulk insert
// API. The task is self-contained: it stages its data where Milvus can read
// it, submits the import, waits for the job to finish, and removes its own
// temp files.
type importViaGRPCTask struct {
	taskID        string
	target        collref.Name
	partitionName string

	// dir is the single partition dir the v1 api takes per call; the caller
	// splits a batch into one task per dir.
	dir            partitionDir
	timestamp      uint64
	isL0           bool
	storageVersion int64
	ezk            string

	// staging copies the data into the target's storage. nil when backup and
	// target share a bucket, in which case Milvus imports the backup in place.
	staging *copyTask

	keepTempFiles bool

	milvusStorage storage.Client
	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath), and milvusLocalPath is what the
	// Milvus process itself resolves (milvus.storage.localPath, falling back
	// to rootPath). Both empty unless the target storage provider is local.
	milvusRootPath  string
	milvusLocalPath string

	grpcCli milvus.Grpc
	taskMgr *taskmgr.Mgr
	logger  *zap.Logger
}

func (gt *importViaGRPCTask) Execute(ctx context.Context) error {
	defer gt.clearTempFiles(ctx)

	if err := gt.copyAndRewriteDir(ctx); err != nil {
		return err
	}

	jobID, err := gt.sendImportReq(ctx)
	if err != nil {
		return err
	}

	return gt.waitImport(ctx, jobID)
}

// copyAndRewriteDir stages the dir into the target's storage and rewrites it
// to point at the copy. Without a staging task the dir already sits in the
// shared bucket and is handed over as-is.
func (gt *importViaGRPCTask) copyAndRewriteDir(ctx context.Context) error {
	if gt.staging == nil {
		return nil
	}

	copied, err := gt.staging.Execute(ctx, []partitionDir{gt.dir})
	if err != nil {
		return fmt.Errorf("restore: import task copy files: %w", err)
	}
	gt.dir = copied[0]

	return nil
}

func (gt *importViaGRPCTask) sendImportReq(ctx context.Context) (int64, error) {
	paths := gt.toGrpcPaths(gt.dir)
	gt.logger.Info("start bulk insert via grpc", zap.Strings("paths", paths), zap.String("partition", gt.partitionName))
	in := milvus.GrpcBulkInsertInput{
		DB:             gt.target.DBName(),
		CollectionName: gt.target.CollName(),
		PartitionName:  gt.partitionName,
		Paths:          paths,
		BackupTS:       gt.timestamp,
		IsL0:           gt.isL0,
		StorageVersion: gt.storageVersion,
		EZK:            gt.ezk,
	}

	jobID, err := gt.grpcCli.BulkInsert(ctx, in)
	if err != nil {
		return 0, fmt.Errorf("restore: failed to bulk insert via grpc: %w", err)
	}
	gt.taskMgr.UpdateRestoreTask(gt.taskID,
		taskmgr.AddRestoreImportJob(gt.target, strconv.FormatInt(jobID, 10), gt.dir.size))
	gt.logger.Info("create bulk insert via grpc success", zap.Int64("job_id", jobID))

	return jobID, nil
}

func (gt *importViaGRPCTask) waitImport(ctx context.Context, jobID int64) error {
	state := func(ctx context.Context) (importJobState, error) {
		s, err := gt.grpcCli.GetBulkInsertState(ctx, jobID)
		if err != nil {
			return importJobState{}, err
		}

		gt.logger.Info("bulk insert task state", zap.Int64("jobID", jobID), zap.Any("state", s.State),
			zap.Any("backup", s.Infos))
		switch s.State {
		case commonpb.ImportState_ImportFailed:
			return importJobState{failedReason: getFailedReason(s.Infos)}, nil
		case commonpb.ImportState_ImportCompleted:
			return importJobState{completed: true}, nil
		default:
			return importJobState{progress: getProcess(s.Infos)}, nil
		}
	}

	return waitImportJob(ctx, gt.logger, gt.taskMgr, gt.taskID, gt.target, strconv.FormatInt(jobID, 10), state)
}

// toGrpcPaths builds the [insertLogDir, deltaLogDir] argument for the grpc bulk
// insert API, mapping each directory through the target storage's path
// convention (absolute paths under localPath for a local target).
func (gt *importViaGRPCTask) toGrpcPaths(dir partitionDir) []string {
	if len(dir.insertLogDir) == 0 {
		return []string{importPath(gt.milvusStorage, gt.milvusLocalPath, dir.deltaLogDir)}
	}
	return []string{
		importPath(gt.milvusStorage, gt.milvusLocalPath, dir.insertLogDir),
		importPath(gt.milvusStorage, gt.milvusLocalPath, dir.deltaLogDir),
	}
}

// clearTempFiles removes the staged copy once the import is over, succeeded or
// not: the import has either consumed the data or never will. A copy that
// failed midway is safe too, whatever was copied sits under the same temp dir.
func (gt *importViaGRPCTask) clearTempFiles(ctx context.Context) {
	if gt.staging == nil {
		return
	}
	if gt.keepTempFiles {
		gt.logger.Info("skip clean temporary files")
		return
	}

	gt.logger.Info("delete temporary file", zap.String("dir", gt.staging.tempDir))
	if err := storage.DeletePrefix(ctx, gt.milvusStorage, destKey(gt.milvusStorage, gt.milvusRootPath, gt.staging.tempDir)); err != nil {
		gt.logger.Warn("clean temporary restore files failed", zap.Error(err))
	}
}

// importViaRESTFulTask imports a whole batch through the v2 restful bulk
// insert API: the api takes every dir pair of the batch in one request.
type importViaRESTFulTask struct {
	taskID        string
	target        collref.Name
	partitionName string

	dirs           []partitionDir
	timestamp      uint64
	isL0           bool
	storageVersion int64
	ezk            string

	// staging copies the data into the target's storage. nil when backup and
	// target share a bucket, in which case Milvus imports the backup in place.
	staging *copyTask

	keepTempFiles bool

	milvusStorage storage.Client
	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath), and milvusLocalPath is what the
	// Milvus process itself resolves (milvus.storage.localPath, falling back
	// to rootPath). Both empty unless the target storage provider is local.
	milvusRootPath  string
	milvusLocalPath string

	restfulCli milvus.Restful
	taskMgr    *taskmgr.Mgr
	logger     *zap.Logger
}

func (rt *importViaRESTFulTask) Execute(ctx context.Context) error {
	defer rt.clearTempFiles(ctx)

	if err := rt.copyAndRewriteDir(ctx); err != nil {
		return err
	}

	jobID, err := rt.sendImportReq(ctx)
	if err != nil {
		return err
	}

	return rt.waitImport(ctx, jobID)
}

// copyAndRewriteDir stages the dirs into the target's storage and rewrites
// them to point at the copies. Without a staging task the dirs already sit in
// the shared bucket and are handed over as-is.
func (rt *importViaRESTFulTask) copyAndRewriteDir(ctx context.Context) error {
	if rt.staging == nil {
		return nil
	}

	copied, err := rt.staging.Execute(ctx, rt.dirs)
	if err != nil {
		return fmt.Errorf("restore: import task copy files: %w", err)
	}
	rt.dirs = copied

	return nil
}

func (rt *importViaRESTFulTask) sendImportReq(ctx context.Context) (string, error) {
	rt.logger.Info("start bulk insert via restful",
		zap.Int("batch_num", len(rt.dirs)), zap.String("partition", rt.partitionName))
	paths := lo.Map(rt.dirs, func(dir partitionDir, _ int) []string { return rt.toPaths(dir) })
	in := milvus.BulkInsertV2Input{
		DB:             rt.target.DBName(),
		CollectionName: rt.target.CollName(),
		PartitionName:  rt.partitionName,
		Paths:          paths,
		BackupTS:       rt.timestamp,
		IsL0:           rt.isL0,
		StorageVersion: rt.storageVersion,
		EZK:            rt.ezk,
	}

	jobID, err := rt.restfulCli.BulkInsert(ctx, in)
	if err != nil {
		return "", fmt.Errorf("restore: failed to bulk insert via restful: %w", err)
	}
	rt.logger.Info("create bulk insert via restful success", zap.String("job_id", jobID))

	size := lo.SumBy(rt.dirs, func(dir partitionDir) int64 { return dir.size })
	rt.taskMgr.UpdateRestoreTask(rt.taskID, taskmgr.AddRestoreImportJob(rt.target, jobID, size))

	return jobID, nil
}

func (rt *importViaRESTFulTask) waitImport(ctx context.Context, jobID string) error {
	state := func(ctx context.Context) (importJobState, error) {
		resp, err := rt.restfulCli.GetBulkInsertState(ctx, rt.target.DBName(), jobID)
		if err != nil {
			return importJobState{}, err
		}

		rt.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return importJobState{failedReason: resp.Data.Reason}, nil
		case string(milvus.ImportStateCompleted):
			rt.taskMgr.UpdateRestoreTask(rt.taskID, taskmgr.UpdateRestoreImportJob(rt.target, jobID, 100))
			return importJobState{completed: true}, nil
		default:
			return importJobState{progress: resp.Data.Progress}, nil
		}
	}

	return waitImportJob(ctx, rt.logger, rt.taskMgr, rt.taskID, rt.target, jobID, state)
}

// toPaths builds the [insertLogDir, deltaLogDir] argument for the restful bulk
// insert API, mapping each directory through the target storage's path
// convention (absolute paths under localPath for a local target).
func (rt *importViaRESTFulTask) toPaths(dir partitionDir) []string {
	paths := make([]string, 0, 2)
	if dir.insertLogDir != "" {
		paths = append(paths, importPath(rt.milvusStorage, rt.milvusLocalPath, dir.insertLogDir))
	}
	if dir.deltaLogDir != "" {
		paths = append(paths, importPath(rt.milvusStorage, rt.milvusLocalPath, dir.deltaLogDir))
	}
	return paths
}

// clearTempFiles removes the staged copy once the import is over, succeeded or
// not: the import has either consumed the data or never will. A copy that
// failed midway is safe too, whatever was copied sits under the same temp dir.
func (rt *importViaRESTFulTask) clearTempFiles(ctx context.Context) {
	if rt.staging == nil {
		return
	}
	if rt.keepTempFiles {
		rt.logger.Info("skip clean temporary files")
		return
	}

	rt.logger.Info("delete temporary file", zap.String("dir", rt.staging.tempDir))
	if err := storage.DeletePrefix(ctx, rt.milvusStorage, destKey(rt.milvusStorage, rt.milvusRootPath, rt.staging.tempDir)); err != nil {
		rt.logger.Warn("clean temporary restore files failed", zap.Error(err))
	}
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
	if cli.Config().Provider != v2.ProviderLocal || p == "" {
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
