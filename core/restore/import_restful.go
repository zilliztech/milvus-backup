package restore

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/storage"
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
	// peers talk to the peer clusters that mirror the target's import jobs over
	// the replication stream; when non-empty, every task commits two-phase on
	// the target and all peers. Empty for a target without peers.
	peers []importPeer

	store  *jobstate.Store
	logger *zap.Logger
}

// importPeer pairs a peer cluster's restful client with the address it was
// dialed with, so a failure can name the peer.
type importPeer struct {
	addr string
	cli  milvus.Restful
}

func newRestfulImportPlanner(dt *collDMLTask, multiL0InOneJob bool, peers []importPeer) *restfulImportPlanner {
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
		peers:      peers,
		store:      dt.store,
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

func (p *restfulImportPlanner) newTask(partitionName string, g dirGroup, dirs []partitionDir) tasklet.Tasklet {
	importer := p.newImporter(partitionName, g, dirs)
	if len(p.peers) != 0 {
		return &importViaRESTFul2PCTask{importer: importer, peers: p.peers}
	}
	return &importViaRESTFulTask{importer: importer}
}

func (p *restfulImportPlanner) newImporter(partitionName string, g dirGroup, dirs []partitionDir) *restfulImporter {
	return &restfulImporter{
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
		store:      p.store,
		logger:     p.logger,
	}
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
	defer rt.importer.cleanup(ctx)

	if err := rt.importer.stage(ctx); err != nil {
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
	store      *jobstate.Store
	logger     *zap.Logger
}

// stage copies the dirs into the target's storage and rewrites them to point
// at the copies. Without a staging task the dirs already sit in the shared
// bucket and are handed over as-is.
func (im *restfulImporter) stage(ctx context.Context) error {
	if im.staging == nil {
		return nil
	}

	copied, err := im.staging.Execute(ctx, im.dirs)
	if err != nil {
		return fmt.Errorf("restore: import task copy files: %w", err)
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
	im.logger.Info("start bulk insert via restful",
		zap.Int("batch_num", len(im.dirs)), zap.String("partition", im.partitionName),
		zap.Bool("manual_commit", manualCommit))
	paths := lo.Map(im.dirs, func(dir partitionDir, _ int) []string { return im.toPaths(dir) })
	in := milvus.BulkInsertV2Input{
		DB:             im.target.DBName(),
		CollectionName: im.target.CollName(),
		PartitionName:  im.partitionName,
		Paths:          paths,
		BackupTS:       im.timestamp,
		IsL0:           im.isL0,
		StorageVersion: im.storageVersion,
		EZK:            im.ezk,
		ManualCommit:   manualCommit,
	}

	jobID, err := im.restfulCli.BulkInsert(ctx, in)
	if err != nil {
		return "", fmt.Errorf("restore: failed to bulk insert via restful: %w", err)
	}
	im.logger.Info("create bulk insert via restful success", zap.String("job_id", jobID))

	size := lo.SumBy(im.dirs, func(dir partitionDir) int64 { return dir.size })
	im.store.UpdateRestoreTask(im.taskID, jobstate.AddRestoreImportJob(im.target, jobID, size))

	return jobID, nil
}

// waitCompleted polls the job until it completes.
func (im *restfulImporter) waitCompleted(ctx context.Context, jobID string) error {
	state := func(ctx context.Context) (importJobState, error) {
		resp, err := im.restfulCli.GetBulkInsertState(ctx, im.target.DBName(), jobID)
		if err != nil {
			return importJobState{}, err
		}

		im.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return importJobState{failedReason: resp.Data.Reason}, nil
		case string(milvus.ImportStateCompleted):
			im.store.UpdateRestoreTask(im.taskID, jobstate.UpdateRestoreImportJob(im.target, jobID, 100))
			return importJobState{completed: true}, nil
		default:
			return importJobState{progress: resp.Data.Progress}, nil
		}
	}

	return waitImportJob(ctx, im.logger, im.store, im.taskID, im.target, jobID, state)
}

// waitUncommitted polls the job until it holds at Uncommitted — the terminal a
// two-phase job is waited to before anyone commits it. Progress stays short of
// 100: the data is not visible yet.
func (im *restfulImporter) waitUncommitted(ctx context.Context, jobID string) error {
	state := func(ctx context.Context) (importJobState, error) {
		resp, err := im.restfulCli.GetBulkInsertState(ctx, im.target.DBName(), jobID)
		if err != nil {
			return importJobState{}, err
		}

		im.logger.Info("bulk insert task state", zap.String("job_id", jobID),
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

	return waitImportJob(ctx, im.logger, im.store, im.taskID, im.target, jobID, state)
}

// commit makes the job's data visible. Milvus commits idempotently, so a
// retried task can call it on a job that is already committing.
func (im *restfulImporter) commit(ctx context.Context, jobID string) error {
	im.logger.Info("commit import job", zap.String("job_id", jobID))
	if err := im.restfulCli.CommitImport(ctx, im.target.DBName(), jobID); err != nil {
		return fmt.Errorf("restore: commit import job %s: %w", jobID, err)
	}

	return nil
}

// abort rolls the job back, dropping whatever it staged. It only works before
// the commit; the two-phase task calls it when the flow fails ahead of one.
func (im *restfulImporter) abort(ctx context.Context, jobID string) {
	im.logger.Info("abort import job", zap.String("job_id", jobID))
	if err := im.restfulCli.AbortImport(ctx, im.target.DBName(), jobID); err != nil {
		im.logger.Warn("abort import job failed", zap.String("job_id", jobID), zap.Error(err))
	}
}

// toPaths builds the [insertLogDir, deltaLogDir] argument for the restful bulk
// insert API, mapping each directory through the target storage's path
// convention (absolute paths under localPath for a local target).
func (im *restfulImporter) toPaths(dir partitionDir) []string {
	paths := make([]string, 0, 2)
	if dir.insertLogDir != "" {
		paths = append(paths, importPath(im.milvusStorage, im.milvusLocalPath, dir.insertLogDir))
	}
	if dir.deltaLogDir != "" {
		paths = append(paths, importPath(im.milvusStorage, im.milvusLocalPath, dir.deltaLogDir))
	}
	return paths
}

// cleanup removes the staged copy once the import is over, succeeded or not:
// the import has either consumed the data or never will. A copy that failed
// midway is safe too, whatever was copied sits under the same temp dir.
func (im *restfulImporter) cleanup(ctx context.Context) {
	if im.staging == nil {
		return
	}
	if im.keepTempFiles {
		im.logger.Info("skip clean temporary files")
		return
	}

	im.logger.Info("delete temporary file", zap.String("dir", im.staging.tempDir))
	if err := storage.DeletePrefix(ctx, im.milvusStorage, destKey(im.milvusStorage, im.milvusRootPath, im.staging.tempDir)); err != nil {
		im.logger.Warn("clean temporary restore files failed", zap.Error(err))
	}
}
