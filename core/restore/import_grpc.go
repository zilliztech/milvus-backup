package restore

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// grpcImportPlanner plans imports for the v1 grpc bulk insert api. The api
// takes a single partition dir per job, so every dir of every group becomes
// its own task; how the tasks run is up to the caller.
type grpcImportPlanner struct {
	args importTaskArgs
}

func newGRPCImportPlanner(args importTaskArgs) *grpcImportPlanner {
	return &grpcImportPlanner{args: args}
}

// planTasks turns the dir groups into one import task per dir.
func (p *grpcImportPlanner) planTasks(partitionName string, groups []dirGroup) []tasklet.Tasklet {
	var tasks []tasklet.Tasklet
	for _, g := range groups {
		for _, dir := range g.dirs {
			tasks = append(tasks, &importViaGRPCTask{
				args: p.args,

				partitionName:  partitionName,
				dir:            dir,
				timestamp:      g.timestamp,
				isL0:           g.isL0,
				storageVersion: g.storageVersion,
			})
		}
	}
	return tasks
}

// importViaGRPCTask imports one partition dir through the v1 grpc bulk insert
// API. The task is self-contained: it stages its data where Milvus can read
// it, submits the import, waits for the job to finish, and removes its own
// temp files.
type importViaGRPCTask struct {
	args importTaskArgs

	partitionName  string
	dir            partitionDir
	timestamp      uint64
	isL0           bool
	storageVersion int64
}

func (gt *importViaGRPCTask) Execute(ctx context.Context) error {
	staging := newCopyTask(gt.args)
	defer staging.Cleanup(ctx)

	dirs, err := staging.Execute(ctx, []partitionDir{gt.dir})
	if err != nil {
		return err
	}
	gt.dir = dirs[0]

	jobID, err := gt.sendImportReq(ctx)
	if err != nil {
		return err
	}

	return gt.waitImport(ctx, jobID)
}

func (gt *importViaGRPCTask) sendImportReq(ctx context.Context) (int64, error) {
	paths := gt.toGrpcPaths(gt.dir)
	gt.args.logger.Info("start bulk insert via grpc", zap.Strings("paths", paths), zap.String("partition", gt.partitionName))
	in := milvus.GrpcBulkInsertInput{
		DB:             gt.args.target.DBName(),
		CollectionName: gt.args.target.CollName(),
		PartitionName:  gt.partitionName,
		Paths:          paths,
		BackupTS:       gt.timestamp,
		IsL0:           gt.isL0,
		StorageVersion: gt.storageVersion,
		EZK:            gt.args.ezk,
	}

	jobID, err := gt.args.grpcCli.BulkInsert(ctx, in)
	if err != nil {
		return 0, fmt.Errorf("restore: failed to bulk insert via grpc: %w", err)
	}
	gt.args.store.UpdateRestoreTask(gt.args.taskID,
		jobstate.AddRestoreImportJob(gt.args.target, strconv.FormatInt(jobID, 10), gt.dir.size))
	gt.args.logger.Info("create bulk insert via grpc success", zap.Int64("job_id", jobID))

	return jobID, nil
}

func (gt *importViaGRPCTask) waitImport(ctx context.Context, jobID int64) error {
	state := func(ctx context.Context) (importJobState, error) {
		s, err := gt.args.grpcCli.GetBulkInsertState(ctx, jobID)
		if err != nil {
			return importJobState{}, err
		}

		gt.args.logger.Info("bulk insert task state", zap.Int64("jobID", jobID), zap.Any("state", s.State),
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

	return waitImportJob(ctx, gt.args.logger, gt.args.store, gt.args.taskID, gt.args.target, strconv.FormatInt(jobID, 10), state)
}

// toGrpcPaths builds the [insertLogDir, deltaLogDir] argument for the grpc bulk
// insert API, mapping each directory through the target storage's path
// convention (absolute paths under localPath for a local target).
func (gt *importViaGRPCTask) toGrpcPaths(dir partitionDir) []string {
	if len(dir.insertLogDir) == 0 {
		return []string{importPath(gt.args.milvusStorage, gt.args.milvusLocalPath, dir.deltaLogDir)}
	}
	return []string{
		importPath(gt.args.milvusStorage, gt.args.milvusLocalPath, dir.insertLogDir),
		importPath(gt.args.milvusStorage, gt.args.milvusLocalPath, dir.deltaLogDir),
	}
}
