package restore

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// restfulImportPlanner plans imports for the v2 restful bulk insert api. The
// api takes a whole set of dirs in one request, so the dirs of a group are
// chunked into tasks of at most maxSegsPerImportJob dirs — the request limit
// is why the chunking exists.
type restfulImportPlanner struct {
	taskID string
	target collref.Name
	ezk    string

	// newStaging builds the staging copy task of one import job; nil when the
	// backup can be imported in place.
	newStaging func() *copyTask

	keepTempFiles bool

	milvusStorage storage.Client
	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath), and milvusLocalPath is what the
	// Milvus process itself resolves (milvus.storage.localPath, falling back
	// to rootPath). Both empty unless the target storage provider is local.
	milvusRootPath  string
	milvusLocalPath string

	// maxSegsPerImportJob caps how many dirs one import task carries, because
	// Milvus limits how many files one restful request may carry.
	maxSegsPerImportJob int
	// multiL0InOneJob reports whether the target takes several L0 segments in
	// one import job; until 2.6.5 each L0 segment needs a job of its own.
	multiL0InOneJob bool

	restfulCli milvus.Restful
	taskMgr    *taskmgr.Mgr
	logger     *zap.Logger
}

func newRestfulImportPlanner(dt *collDMLTask, multiL0InOneJob bool) *restfulImportPlanner {
	return &restfulImportPlanner{
		taskID: dt.taskID,
		target: dt.target,
		ezk:    dt.ezk(),

		newStaging: dt.newCopyTask,

		keepTempFiles: dt.keepTempFiles,

		milvusStorage:   dt.milvusStorage,
		milvusRootPath:  dt.milvusRootPath,
		milvusLocalPath: dt.milvusLocalPath,

		maxSegsPerImportJob: dt.maxSegsPerImportJob,
		multiL0InOneJob:     multiL0InOneJob,

		restfulCli: dt.restfulCli,
		taskMgr:    dt.taskMgr,
		logger:     dt.logger,
	}
}

// planTasks turns the dir groups into one import task per chunk of dirs. An
// L0 dir chunks alone unless the target takes several L0 segments per job.
func (p *restfulImportPlanner) planTasks(partitionName string, groups []dirGroup) []tasklet.Tasklet {
	var tasks []tasklet.Tasklet
	for _, g := range groups {
		chunkSize := p.maxSegsPerImportJob
		if g.isL0 && !p.multiL0InOneJob {
			chunkSize = 1
		}
		for _, dirs := range lo.Chunk(g.dirs, chunkSize) {
			tasks = append(tasks, p.newTask(partitionName, g, dirs))
		}
	}
	return tasks
}

func (p *restfulImportPlanner) newTask(partitionName string, g dirGroup, dirs []partitionDir) *importViaRESTFulTask {
	return &importViaRESTFulTask{
		taskID:        p.taskID,
		target:        p.target,
		partitionName: partitionName,

		dirs:           dirs,
		timestamp:      g.timestamp,
		isL0:           g.isL0,
		storageVersion: g.storageVersion,
		ezk:            p.ezk,

		staging: p.newStaging(),

		keepTempFiles: p.keepTempFiles,

		milvusStorage:   p.milvusStorage,
		milvusRootPath:  p.milvusRootPath,
		milvusLocalPath: p.milvusLocalPath,

		restfulCli: p.restfulCli,
		taskMgr:    p.taskMgr,
		logger:     p.logger,
	}
}

// importViaRESTFulTask imports a set of dirs through the v2 restful bulk
// insert API: the api takes every dir pair in one request. The task is
// self-contained: it stages its data where Milvus can read it, submits the
// import, waits for the job to finish, and removes its own temp files.
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
