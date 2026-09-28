package restore

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

type collTask struct {
	taskID string

	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	option       *Option
	collOverride CollOverride

	taskMgr *taskmgr.Mgr

	target collref.Name

	streaming     bool
	keepTempFiles bool
	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	// maxSegsPerImportJob is how many segments at most are merged into one
	// import job, because Milvus limits how many files one request may carry.
	maxSegsPerImportJob int

	backupDir     string
	backupStorage storage.Client

	milvusStorage storage.Client

	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath), and milvusLocalPath is what the Milvus
	// process itself resolves (milvus.storage.localPath, falling back to rootPath).
	// Both empty unless the target storage provider is local.
	milvusRootPath  string
	milvusLocalPath string

	grpcCli    milvus.Grpc
	restfulCli milvus.Restful

	vchTimestamp struct {
		mu    sync.RWMutex
		vchTS map[string]uint64
	}

	logger *zap.Logger
}

type collTaskArgs struct {
	taskID string

	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	target collref.Name

	option       *Option
	collOverride CollOverride

	taskMgr *taskmgr.Mgr

	backupDir     string
	keepTempFiles bool
	streaming     bool

	maxSegsPerImportJob int

	backupStorage storage.Client
	milvusStorage storage.Client

	milvusRootPath  string
	milvusLocalPath string

	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	grpcCli    milvus.Grpc
	restfulCli milvus.Restful
}

func newCollTask(args collTaskArgs) *collTask {
	src := collref.New(args.collBackup.GetDbName(), args.collBackup.GetCollectionName())

	logger := log.With(
		zap.String("restore_task_id", args.taskID),
		zap.String("backup_coll", src.String()),
		zap.String("target_coll", args.target.String()))

	size := lo.SumBy(args.collBackup.GetPartitionBackups(), func(partition *backuppb.PartitionBackupInfo) int64 {
		return partition.GetSize()
	})
	args.taskMgr.UpdateRestoreTask(args.taskID, taskmgr.AddRestoreCollTask(args.target, size))

	return &collTask{
		taskID: args.taskID,

		dbBackup:   args.dbBackup,
		collBackup: args.collBackup,

		option:       args.option,
		collOverride: args.collOverride,

		target: args.target,

		taskMgr: args.taskMgr,

		copySem:       args.copySem,
		bulkInsertSem: args.bulkInsertSem,

		streaming:     args.streaming,
		keepTempFiles: args.keepTempFiles,
		backupDir:     args.backupDir,

		maxSegsPerImportJob: args.maxSegsPerImportJob,

		backupStorage: args.backupStorage,
		milvusStorage: args.milvusStorage,

		milvusRootPath:  args.milvusRootPath,
		milvusLocalPath: args.milvusLocalPath,

		grpcCli:    args.grpcCli,
		restfulCli: args.restfulCli,

		vchTimestamp: struct {
			mu    sync.RWMutex
			vchTS map[string]uint64
		}{
			vchTS: make(map[string]uint64),
		},

		logger: logger,
	}
}

func (ct *collTask) Target() collref.Name { return ct.target }

func (ct *collTask) Execute(ctx context.Context) error {
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollExecuting(ct.target))

	if err := ct.privateExecute(ctx); err != nil {
		ct.logger.Error("restore collection failed", zap.Error(err))
		ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollFail(ct.target, err))
		return err
	}

	ct.logger.Info("restore collection success")
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollSuccess(ct.target))

	return nil
}

func (ct *collTask) privateExecute(ctx context.Context) error {
	ct.logger.Info("start restore collection")

	ddlt := newCollDDLTask(ct.taskID, ct.option, ct.collOverride, ct.collBackup, ct.target, ct.grpcCli)
	if err := ddlt.Execute(ctx); err != nil {
		return fmt.Errorf("restore_collection: restore collection ddl: %w", err)
	}

	// restore collection data
	if err := ct.restoreData(ctx); err != nil {
		return fmt.Errorf("restore_collection: restore data: %w", err)
	}

	return nil
}

// restoreData builds the import jobs for every partition and the global L0
// segments, then runs them. Each job is self-contained: it stages its data
// where Milvus can read it, imports, and removes its own temp files.
func (ct *collTask) restoreData(ctx context.Context) error {
	if ct.option.MetaOnly {
		ct.logger.Info("skip restore data")
		return nil
	}

	factory := ct.newImportJobFactory()

	ct.logger.Info("start restore partition segment", zap.Int("partition_num", len(ct.collBackup.GetPartitionBackups())))
	g, subCtx := errgroup.WithContext(ctx)
	for _, part := range ct.collBackup.GetPartitionBackups() {
		g.Go(func() error {
			if err := ct.restorePartition(subCtx, part, factory); err != nil {
				return fmt.Errorf("restore_collection: restore partition: %w", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for partition restore: %w", err)
	}

	// restore all partition l0 segment
	ct.logger.Info("start restore all partition L0 segment", zap.Int("l0_segments", len(ct.collBackup.GetL0Segments())))
	l0Batches, err := ct.l0SegmentBatches(ct.collBackup.GetL0Segments())
	if err != nil {
		return fmt.Errorf("restore_collection: get L0 batches: %w", err)
	}
	if err := ct.runBatches(ctx, "", l0Batches, factory); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

func (ct *collTask) restorePartition(ctx context.Context, part *backuppb.PartitionBackupInfo, factory importJobFactory) error {
	ct.logger.Info("start restore not L0 segment", zap.String("partition_name", part.GetPartitionName()))
	notL0Batches, err := ct.notL0SegmentBatches(ctx, part)
	if err != nil {
		return fmt.Errorf("restore_collection: get not L0 groups: %w", err)
	}
	if err := ct.runBatches(ctx, part.GetPartitionName(), notL0Batches, factory); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 groups: %w", err)
	}

	ct.logger.Info("start restore L0 segment", zap.String("partition_name", part.GetPartitionName()))
	l0Segs := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
		return seg.IsL0
	})
	l0Batches, err := ct.l0SegmentBatches(l0Segs)
	if err != nil {
		return fmt.Errorf("restore_collection: get L0 batches: %w", err)
	}
	if err := ct.runBatches(ctx, part.GetPartitionName(), l0Batches, factory); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segment: %w", err)
	}

	return nil
}

// runBatches runs the jobs of every batch. The v1 grpc path runs batches
// serially, one batch's jobs at a time; the v2 restful path runs all batches'
// jobs concurrently.
func (ct *collTask) runBatches(ctx context.Context, partitionName string, batches []batch, factory importJobFactory) error {
	if ct.option.UseV2Restore {
		var jobs []tasklet.Tasklet
		for _, b := range batches {
			jobs = append(jobs, factory(partitionName, b)...)
		}
		return ct.runJobs(ctx, jobs)
	}

	for _, b := range batches {
		if err := ct.runJobs(ctx, factory(partitionName, b)); err != nil {
			return err
		}
	}
	return nil
}

func (ct *collTask) runJobs(ctx context.Context, jobs []tasklet.Tasklet) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, job := range jobs {
		if err := ct.bulkInsertSem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("restore_collection: acquire bulk insert semaphore %w", err)
		}

		g.Go(func() error {
			defer ct.bulkInsertSem.Release(1)

			if err := job.Execute(subCtx); err != nil {
				return fmt.Errorf("restore_collection: execute import job %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for import jobs: %w", err)
	}

	return nil
}

// newImportJobFactory returns the factory that turns a batch into import jobs.
// The v1 grpc API takes one directory pair per call, so a batch becomes one job
// per partition dir; the v2 restful API takes them all at once, so a batch is
// one job.
func (ct *collTask) newImportJobFactory() importJobFactory {
	return func(partitionName string, b batch) []tasklet.Tasklet {
		if ct.option.UseV2Restore {
			job := &restfulImportJob{
				importJobBase: ct.newImportJobBase(partitionName, b, b.partitionDirs),
				restfulCli:    ct.restfulCli,
			}
			return []tasklet.Tasklet{job}
		}

		jobs := make([]tasklet.Tasklet, 0, len(b.partitionDirs))
		for _, dir := range b.partitionDirs {
			job := &grpcImportJob{
				importJobBase: ct.newImportJobBase(partitionName, b, []partitionDir{dir}),
				grpcCli:       ct.grpcCli,
			}
			jobs = append(jobs, job)
		}
		return jobs
	}
}

func (ct *collTask) newImportJobBase(partitionName string, b batch, dirs []partitionDir) importJobBase {
	return importJobBase{
		taskID: ct.taskID,
		target: ct.target,

		partitionName:  partitionName,
		dirs:           dirs,
		timestamp:      b.timestamp,
		isL0:           b.isL0,
		storageVersion: b.storageVersion,
		ezk:            ct.ezk(),

		copy: ct.newCopyTask(),

		keepTempFiles: ct.keepTempFiles,

		milvusStorage:   ct.milvusStorage,
		milvusLocalPath: ct.milvusLocalPath,

		taskMgr: ct.taskMgr,
		logger:  ct.logger,
	}
}

// newCopyTask builds the copy task that stages a job's data into the target's
// storage, with a temp dir unique to the job so the job cleans up after itself.
// It returns nil when backup and target share a bucket and the transfer is not
// streamed, in which case Milvus imports the backup in place.
func (ct *collTask) newCopyTask() *copyTask {
	isSameBucket := ct.milvusStorage.Config().Bucket == ct.backupStorage.Config().Bucket
	isSameStorage := ct.backupStorage.Config().Provider == ct.milvusStorage.Config().Provider
	if isSameBucket && isSameStorage && !ct.streaming {
		return nil
	}

	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s-%s/",
		ct.taskID, ct.target.DBName(), ct.target.CollName(), uuid.NewString())
	return &copyTask{
		src:       ct.backupStorage,
		dest:      ct.milvusStorage,
		backupDir: ct.backupDir,
		tempDir:   tempDir,
		sem:       ct.copySem,

		milvusRootPath: ct.milvusRootPath,

		logger: ct.logger,
	}
}

func (ct *collTask) ezk() string {
	oldEZK := ct.dbBackup.GetEzk()
	if oldEZK == "" {
		return ""
	}

	if len(ct.option.EZKMapping) > 0 {
		if newEZK, ok := ct.option.EZKMapping[oldEZK]; ok {
			return newEZK
		}
	}

	return oldEZK
}

func (ct *collTask) notL0SegBatchesWithoutGroupID(ctx context.Context, part *backuppb.PartitionBackupInfo) ([]batch, error) {
	if ct.option.TruncateBinlogByTs {
		return nil, fmt.Errorf("restore: truncate binlog by ts is not supported if group id is not set in backup")
	}

	opts := []mpath.Option{
		mpath.CollectionID(ct.collBackup.GetCollectionId()),
		mpath.PartitionID(part.GetPartitionId()),
	}
	partDir, err := ct.buildBackupPartitionDir(ctx, part.GetSize(), opts...)
	if err != nil {
		return nil, fmt.Errorf("restore_collection: get partition backup binlog files: %w", err)
	}

	ct.logger.Info("build batches without group id", zap.String("partition", part.GetPartitionName()))
	return []batch{{partitionDirs: []partitionDir{partDir}}}, nil
}

func (ct *collTask) backupTS(vch string) (uint64, error) {
	if !ct.option.TruncateBinlogByTs {
		return 0, nil
	}

	if len(vch) == 0 {
		return 0, fmt.Errorf("restore_collection: empty vch but truncate binlog by ts is set")
	}

	ct.vchTimestamp.mu.RLock()
	// fast path, if the timestamp is already cached
	if ts, ok := ct.vchTimestamp.vchTS[vch]; ok {
		ct.vchTimestamp.mu.RUnlock()
		return ts, nil
	}
	ct.vchTimestamp.mu.RUnlock()

	// slow path, if the timestamp is not cached, get it from backup
	posStr, ok := ct.collBackup.GetChannelCheckpoints()[vch]
	if !ok {
		return 0, fmt.Errorf("restore_collection: failed to get vch %s checkpoint", vch)
	}
	pos, err := pbconv.Base64DecodeMsgPosition(posStr)
	if err != nil {
		return 0, fmt.Errorf("restore_collection: failed to decode checkpoint: %w", err)
	}
	ts := pos.GetTimestamp()

	// cache the timestamp, it is idempotence so no need to check if it is already cached.
	ct.vchTimestamp.mu.Lock()
	defer ct.vchTimestamp.mu.Unlock()
	ct.vchTimestamp.vchTS[vch] = ts
	return ts, nil
}

type batchKey struct {
	vch string
	sv  int64
}

func (ct *collTask) notL0SegBatchesWithGroupID(ctx context.Context, notL0Segs []*backuppb.SegmentBackupInfo) ([]batch, error) {
	// group by vchannel and storage version
	segBatch := lo.GroupBy(notL0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	var batches []batch
	for key, segs := range segBatch {
		ts, err := ct.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get vch %s ts: %w", key.vch, err)
		}

		// because the restful api has a limitation on the number of segments in one request,
		// we need to chunk the segments into multiple batches
		chunkedSegs := lo.Chunk(segs, ct.maxSegsPerImportJob)
		for _, chunk := range chunkedSegs {
			dirs := make([]partitionDir, 0, len(chunk))
			for _, seg := range chunk {
				opts := []mpath.Option{
					mpath.CollectionID(ct.collBackup.GetCollectionId()),
					mpath.PartitionID(seg.GetPartitionId()),
					mpath.GroupID(seg.GetGroupId()),
				}

				dir, err := ct.buildBackupPartitionDir(ctx, seg.GetSize(), opts...)
				if err != nil {
					return nil, fmt.Errorf("restore_collection: get partition backup binlog files: %w", err)
				}
				dirs = append(dirs, dir)
			}

			b := batch{timestamp: ts, partitionDirs: dirs, storageVersion: key.sv}
			batches = append(batches, b)
		}
	}

	ct.logger.Info("build batches with group id done", zap.Int("batch_num", len(batches)))

	return batches, nil
}

func (ct *collTask) notL0SegmentBatches(ctx context.Context, part *backuppb.PartitionBackupInfo) ([]batch, error) {
	var withGroupID bool
	notL0Segs := make([]*backuppb.SegmentBackupInfo, 0, len(part.GetSegmentBackups()))
	for _, seg := range part.GetSegmentBackups() {
		if seg.IsL0 {
			continue
		}
		notL0Segs = append(notL0Segs, seg)
		if seg.GetGroupId() != 0 {
			withGroupID = true
		}
	}
	if len(notL0Segs) == 0 {
		ct.logger.Info("no not L0 segments found")
		return nil, nil
	}

	if withGroupID {
		return ct.notL0SegBatchesWithGroupID(ctx, notL0Segs)
	}
	// backward compatible old backup without group id
	return ct.notL0SegBatchesWithoutGroupID(ctx, part)
}

func (ct *collTask) l0SegmentBatches(l0Segs []*backuppb.SegmentBackupInfo) ([]batch, error) {
	segBatch := lo.GroupBy(l0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	chunkSize := 1
	if ct.grpcCli.HasFeature(milvus.MultiL0InOneJob) {
		chunkSize = ct.maxSegsPerImportJob
	}

	var batches []batch
	for key, segs := range segBatch {
		ts, err := ct.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get vch %s ts: %w", key.vch, err)
		}

		chunkedSegs := lo.Chunk(segs, chunkSize)
		for _, chunk := range chunkedSegs {
			dirs := make([]partitionDir, 0, len(chunk))
			for _, seg := range chunk {
				opts := []mpath.Option{
					mpath.CollectionID(ct.collBackup.GetCollectionId()),
					mpath.PartitionID(seg.GetPartitionId()),
					mpath.SegmentID(seg.GetSegmentId()),
				}

				deltaLogDir := mpath.BackupDeltaLogDir(ct.backupDir, opts...)
				dirs = append(dirs, partitionDir{deltaLogDir: deltaLogDir, size: seg.GetSize()})
			}
			b := batch{isL0: true, timestamp: ts, partitionDirs: dirs, storageVersion: key.sv}
			batches = append(batches, b)
		}
	}

	return batches, nil
}

type partitionDir struct {
	insertLogDir string
	deltaLogDir  string

	size int64
}

type batch struct {
	isL0           bool
	timestamp      uint64
	storageVersion int64

	partitionDirs []partitionDir
}

func (ct *collTask) buildBackupPartitionDir(ctx context.Context, size int64, pathOpt ...mpath.Option) (partitionDir, error) {
	insertLogDir := mpath.BackupInsertLogDir(ct.backupDir, pathOpt...)
	deltaLogDir := mpath.BackupDeltaLogDir(ct.backupDir, pathOpt...)

	exist, err := storage.Exist(ctx, ct.backupStorage, deltaLogDir)
	if err != nil {
		return partitionDir{}, fmt.Errorf("restore_collection: check delta log exist: %w", err)
	}

	if exist {
		return partitionDir{insertLogDir: insertLogDir, deltaLogDir: deltaLogDir, size: size}, nil
	}
	return partitionDir{insertLogDir: insertLogDir, size: size}, nil
}
