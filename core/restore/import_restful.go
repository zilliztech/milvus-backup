package restore

import (
	"context"
	"fmt"
	"slices"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// restfulImportPlanner plans imports for the v2 restful bulk insert api. The
// api takes a whole set of dirs in one request, so the dirs of a group are
// chunked into tasks of at most maxSegsPerImportJob dirs — the request limit
// is why the chunking exists.
type restfulImportPlanner struct {
	args importTaskArgs

	// multiL0InOneJob reports whether the target takes several L0 segments in
	// one import job; until 2.6.5 each L0 segment needs a job of its own.
	multiL0InOneJob bool

	// peers talk to the peer clusters that mirror the target's import jobs over
	// the replication stream; when non-empty, every task commits two-phase on
	// the target and all peers. Empty for a target without peers.
	peers []importPeer
}

// importPeer pairs a peer cluster's restful client with the address it was
// dialed with, so a failure can name the peer.
type importPeer struct {
	addr string
	cli  milvus.Restful
}

func newRestfulImportPlanner(args importTaskArgs, multiL0InOneJob bool, peers []importPeer) *restfulImportPlanner {
	return &restfulImportPlanner{
		args:            args,
		multiL0InOneJob: multiL0InOneJob,
		peers:           peers,
	}
}

// planTasks turns the dir groups into one import task per chunk of dirs. An
// L0 dir chunks alone unless the target takes several L0 segments per job.
func (p *restfulImportPlanner) planTasks(partitionName string, groups []dirGroup) []tasklet.Tasklet {
	var tasks []tasklet.Tasklet
	for _, g := range groups {
		chunkSize := p.args.maxSegsPerImportJob
		if g.isL0 && !p.multiL0InOneJob {
			chunkSize = 1
		}
		for dirs := range slices.Chunk(g.dirs, chunkSize) {
			tasks = append(tasks, p.newTask(partitionName, g, dirs))
		}
	}
	return tasks
}

func (p *restfulImportPlanner) newTask(partitionName string, g dirGroup, dirs []partitionDir) tasklet.Tasklet {
	importer := &restfulImporter{
		args: p.args,

		partitionName:  partitionName,
		dirs:           dirs,
		timestamp:      g.timestamp,
		isL0:           g.isL0,
		storageVersion: g.storageVersion,
	}
	if len(p.peers) != 0 {
		return &importViaRESTFul2PCTask{importer: importer, peers: p.peers}
	}
	return &importViaRESTFulTask{importer: importer}
}

// importViaRESTFulTask imports a set of dirs through the v2 restful bulk
// insert API and waits for the job to complete: the plain, single-phase shape
// of a restful import. The task is a thin shell — the import mechanism lives
// on the restfulImporter it drives, so a sibling task can drive the same
// device through a different flow.
type importViaRESTFulTask struct {
	importer *restfulImporter
}

func (rt *importViaRESTFulTask) Execute(ctx context.Context) error {
	staging := newCopyTask(rt.importer.args)
	defer staging.Cleanup(ctx)

	if err := rt.importer.stage(ctx, staging); err != nil {
		return err
	}

	jobID, err := rt.importer.submit(ctx)
	if err != nil {
		return err
	}

	return rt.importer.waitCompleted(ctx, jobID)
}

// restfulImporter is the mechanism of a restful bulk insert import: it stages
// a set of dirs where Milvus can read them, submits the import job, polls it
// to a terminal state, and removes the staged copies. It is a device, not a
// flow — each task type holds one and calls the steps in its own order, so
// Milvus protocol details (option keys, terminal states) stay here and out of
// the task shells.
type restfulImporter struct {
	args importTaskArgs

	partitionName  string
	dirs           []partitionDir
	timestamp      uint64
	isL0           bool
	storageVersion int64
}

// stage copies the dirs into the target's storage and rewrites them to point
// at the copies. A nil copy task means the dirs already sit in the shared
// bucket and are handed over as-is.
func (im *restfulImporter) stage(ctx context.Context, staging *copyTask) error {
	copied, err := staging.Execute(ctx, im.dirs)
	if err != nil {
		return err
	}
	im.dirs = copied

	return nil
}

// submit sends the import request and records the job in the job state store.
func (im *restfulImporter) submit(ctx context.Context) (string, error) {
	return im.createJob(ctx, false)
}

// submit2PC sends the import request with auto_commit=false: the job stops at
// Uncommitted and becomes visible only when someone commits it.
func (im *restfulImporter) submit2PC(ctx context.Context) (string, error) {
	return im.createJob(ctx, true)
}

func (im *restfulImporter) createJob(ctx context.Context, manualCommit bool) (string, error) {
	im.args.logger.Info("start bulk insert via restful",
		zap.Int("batch_num", len(im.dirs)), zap.String("partition", im.partitionName),
		zap.Bool("manual_commit", manualCommit))
	paths := lo.Map(im.dirs, func(dir partitionDir, _ int) []string { return im.toPaths(dir) })
	in := milvus.BulkInsertV2Input{
		DB:             im.args.target.DBName(),
		CollectionName: im.args.target.CollName(),
		PartitionName:  im.partitionName,
		Paths:          paths,
		BackupTS:       im.timestamp,
		IsL0:           im.isL0,
		StorageVersion: im.storageVersion,
		EZK:            im.args.ezk,
		ManualCommit:   manualCommit,
	}

	jobID, err := im.args.restfulCli.BulkInsert(ctx, in)
	if err != nil {
		return "", fmt.Errorf("restore: failed to bulk insert via restful: %w", err)
	}
	im.args.logger.Info("create bulk insert via restful success", zap.String("job_id", jobID))

	size := lo.SumBy(im.dirs, func(dir partitionDir) int64 { return dir.size })
	im.args.store.UpdateRestoreTask(im.args.taskID, jobstate.AddRestoreImportJob(im.args.target, jobID, size))

	return jobID, nil
}

// waitCompleted polls the job until it completes.
func (im *restfulImporter) waitCompleted(ctx context.Context, jobID string) error {
	state := func(ctx context.Context) (importJobState, error) {
		resp, err := im.args.restfulCli.GetBulkInsertState(ctx, im.args.target.DBName(), jobID)
		if err != nil {
			return importJobState{}, err
		}

		im.args.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return importJobState{failedReason: resp.Data.Reason}, nil
		case string(milvus.ImportStateCompleted):
			im.args.store.UpdateRestoreTask(im.args.taskID, jobstate.UpdateRestoreImportJob(im.args.target, jobID, 100))
			return importJobState{completed: true}, nil
		default:
			return importJobState{progress: resp.Data.Progress}, nil
		}
	}

	return waitImportJob(ctx, im.args.logger, im.args.store, im.args.taskID, im.args.target, jobID, state)
}

// waitUncommitted polls the job until it holds at Uncommitted — the terminal a
// two-phase job is waited to before anyone commits it. Progress stays short of
// 100: the data is not visible yet.
func (im *restfulImporter) waitUncommitted(ctx context.Context, jobID string) error {
	state := func(ctx context.Context) (importJobState, error) {
		resp, err := im.args.restfulCli.GetBulkInsertState(ctx, im.args.target.DBName(), jobID)
		if err != nil {
			return importJobState{}, err
		}

		im.args.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return importJobState{failedReason: resp.Data.Reason}, nil
		case string(milvus.ImportStateUncommitted):
			return importJobState{completed: true}, nil
		default:
			return importJobState{progress: resp.Data.Progress}, nil
		}
	}

	return waitImportJob(ctx, im.args.logger, im.args.store, im.args.taskID, im.args.target, jobID, state)
}

// commit makes the job's data visible. Milvus commits idempotently, so a
// retried task can call it on a job that is already committing.
func (im *restfulImporter) commit(ctx context.Context, jobID string) error {
	im.args.logger.Info("commit import job", zap.String("job_id", jobID))
	if err := im.args.restfulCli.CommitImport(ctx, im.args.target.DBName(), jobID); err != nil {
		return fmt.Errorf("restore: commit import job %s: %w", jobID, err)
	}

	return nil
}

// abort rolls the job back, dropping whatever it staged. It only works before
// the commit; the two-phase task calls it when the flow fails ahead of one.
func (im *restfulImporter) abort(ctx context.Context, jobID string) {
	im.args.logger.Info("abort import job", zap.String("job_id", jobID))
	if err := im.args.restfulCli.AbortImport(ctx, im.args.target.DBName(), jobID); err != nil {
		im.args.logger.Warn("abort import job failed", zap.String("job_id", jobID), zap.Error(err))
	}
}

// toPaths builds the [insertLogDir, deltaLogDir] argument for the restful bulk
// insert API, mapping each directory through the target storage's path
// convention (absolute paths under localPath for a local target).
func (im *restfulImporter) toPaths(dir partitionDir) []string {
	paths := make([]string, 0, 2)
	if dir.insertLogDir != "" {
		paths = append(paths, importPath(im.args.milvusStorage, im.args.milvusLocalPath, dir.insertLogDir))
	}
	if dir.deltaLogDir != "" {
		paths = append(paths, importPath(im.args.milvusStorage, im.args.milvusLocalPath, dir.deltaLogDir))
	}
	return paths
}
