package restore

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// grpcImportPlanner plans imports for the v1 grpc bulk insert api. The api
// takes a single partition dir per job, so every dir of every group becomes
// its own task; how the tasks run is up to the caller.
type grpcImportPlanner struct {
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

	grpcCli milvus.Grpc
	store   *jobstate.Store
	logger  *zap.Logger
}

func newGRPCImportPlanner(dt *collDMLTask) *grpcImportPlanner {
	return &grpcImportPlanner{
		taskID: dt.taskID,
		target: dt.target,
		ezk:    dt.ezk(),

		newStaging: dt.newCopyTask,

		keepTempFiles: dt.keepTempFiles,

		milvusStorage:   dt.milvusStorage,
		milvusRootPath:  dt.milvusRootPath,
		milvusLocalPath: dt.milvusLocalPath,

		grpcCli: dt.grpcCli,
		store:   dt.store,
		logger:  dt.logger,
	}
}

// planTasks turns the dir groups into one import task per dir.
func (p *grpcImportPlanner) planTasks(partitionName string, groups []dirGroup) []tasklet.Tasklet {
	var tasks []tasklet.Tasklet
	for _, g := range groups {
		for _, dir := range g.dirs {
			tasks = append(tasks, p.newTask(partitionName, g, dir))
		}
	}
	return tasks
}

func (p *grpcImportPlanner) newTask(partitionName string, g dirGroup, dir partitionDir) *importViaGRPCTask {
	return &importViaGRPCTask{
		taskID:        p.taskID,
		target:        p.target,
		partitionName: partitionName,

		dir:            dir,
		timestamp:      g.timestamp,
		isL0:           g.isL0,
		storageVersion: g.storageVersion,
		ezk:            p.ezk,

		staging: p.newStaging(),

		keepTempFiles: p.keepTempFiles,

		milvusStorage:   p.milvusStorage,
		milvusRootPath:  p.milvusRootPath,
		milvusLocalPath: p.milvusLocalPath,

		grpcCli: p.grpcCli,
		store:   p.store,
		logger:  p.logger,
	}
}

// importViaGRPCTask imports one partition dir through the v1 grpc bulk insert
// API. The task is self-contained: it stages its data where Milvus can read
// it, submits the import, waits for the job to finish, and removes its own
// temp files.
type importViaGRPCTask struct {
	taskID        string
	target        collref.Name
	partitionName string

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
	store   *jobstate.Store
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
	gt.store.UpdateRestoreTask(gt.taskID,
		jobstate.AddRestoreImportJob(gt.target, strconv.FormatInt(jobID, 10), gt.dir.size))
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

	return waitImportJob(ctx, gt.logger, gt.store, gt.taskID, gt.target, strconv.FormatInt(jobID, 10), state)
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
