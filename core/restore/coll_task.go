package restore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const (
	_bulkInsertTimeout             = 60 * time.Minute
	_bulkInsertCheckInterval       = 3 * time.Second
	_bulkInsertRestfulAPIChunkSize = 256
)

type tearDownFn func(ctx context.Context) error

type collTask struct {
	taskID string

	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	option *Option

	taskMgr *taskmgr.Mgr

	targetNS namespace.NS

	crossStorage  bool
	keepTempFiles bool
	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	backupDir     string
	backupStorage storage.Client

	milvusStorage storage.Client

	grpcCli    milvus.Grpc
	restfulCli milvus.Restful

	tearDownFn struct {
		mu  sync.Mutex
		fns []tearDownFn
	}

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

	targetNS namespace.NS

	option *Option

	taskMgr *taskmgr.Mgr

	backupDir     string
	keepTempFiles bool
	crossStorage  bool

	backupStorage storage.Client
	milvusStorage storage.Client

	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	grpcCli    milvus.Grpc
	restfulCli milvus.Restful
}

func newCollTask(args collTaskArgs) *collTask {
	srcNS := namespace.New(args.collBackup.GetDbName(), args.collBackup.GetCollectionName())

	logger := log.With(
		zap.String("restore_task_id", args.taskID),
		zap.String("backup_ns", srcNS.String()),
		zap.String("target_ns", args.targetNS.String()))

	size := lo.SumBy(args.collBackup.GetPartitionBackups(), func(partition *backuppb.PartitionBackupInfo) int64 {
		return partition.GetSize()
	})
	args.taskMgr.UpdateRestoreTask(args.taskID, taskmgr.AddRestoreCollTask(args.targetNS, size))

	return &collTask{
		taskID: args.taskID,

		dbBackup:   args.dbBackup,
		collBackup: args.collBackup,

		option: args.option,

		targetNS: args.targetNS,

		taskMgr: args.taskMgr,

		copySem:       args.copySem,
		bulkInsertSem: args.bulkInsertSem,

		crossStorage:  args.crossStorage,
		keepTempFiles: args.keepTempFiles,
		backupDir:     args.backupDir,

		backupStorage: args.backupStorage,
		milvusStorage: args.milvusStorage,

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

func (ct *collTask) Execute(ctx context.Context) error {
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollExecuting(ct.targetNS))

	// tear down restore task
	defer func() {
		if err := ct.tearDown(ctx); err != nil {
			ct.logger.Error("restore collection tear down failed", zap.Error(err))
		}
	}()

	if err := ct.privateExecute(ctx); err != nil {
		ct.logger.Error("restore collection failed", zap.Error(err))
		ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollFail(ct.targetNS, err))
		return err
	}

	ct.logger.Info("restore collection success")
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollSuccess(ct.targetNS))

	return nil
}

func (ct *collTask) privateExecute(ctx context.Context) error {
	ct.logger.Info("start restore collection")

	ddlt := newCollDDLTask(ct.taskID, ct.option, ct.collBackup, ct.targetNS, ct.grpcCli)
	if err := ddlt.Execute(ctx); err != nil {
		return fmt.Errorf("restore_collection: restore collection ddl: %w", err)
	}

	// restore collection data
	if err := ct.restoreData(ctx); err != nil {
		return fmt.Errorf("restore_collection: restore data: %w", err)
	}

	return nil
}

func (ct *collTask) restoreData(ctx context.Context) error {
	if ct.option.MetaOnly {
		ct.logger.Info("skip restore data")
		return nil
	}

	// restore collection data
	if ct.option.UseV2Restore {
		if err := ct.restoreDataV2(ctx); err != nil {
			return fmt.Errorf("restore_collection: restore data v2: %w", err)
		}
	} else {
		if err := ct.restoreDataV1(ctx); err != nil {
			return fmt.Errorf("restore_collection: restore data v1: %w", err)
		}
	}

	return nil
}

func (ct *collTask) restoreDataV2(ctx context.Context) error {
	// restore partition segment
	ct.logger.Info("start restore partition segment", zap.Int("partition_num", len(ct.collBackup.GetPartitionBackups())))
	g, subCtx := errgroup.WithContext(ctx)
	for _, part := range ct.collBackup.GetPartitionBackups() {
		g.Go(func() error {
			if err := ct.restorePartitionV2(subCtx, part); err != nil {
				return fmt.Errorf("restore_collection: restore partition v2: %w", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for partition restore: %w", err)
	}

	// restore all partition l0 segment
	ct.logger.Info("start restore all partition L0 segment", zap.Int("l0_segments", len(ct.collBackup.GetL0Segments())))
	if err := ct.restoreL0SegV2(ctx, "", ct.collBackup.GetL0Segments()); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

func (ct *collTask) restoreDataV1(ctx context.Context) error {
	// restore partition segment
	ct.logger.Info("start restore partition segment", zap.Int("partition_num", len(ct.collBackup.GetPartitionBackups())))
	g, subCtx := errgroup.WithContext(ctx)
	for _, part := range ct.collBackup.GetPartitionBackups() {
		g.Go(func() error {
			if err := ct.restorePartitionV1(subCtx, part); err != nil {
				return fmt.Errorf("restore_collection: restore partition data v1: %w", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for partition restore: %w", err)
	}

	// restore all partition l0 segment
	ct.logger.Info("start restore all partition L0 segment", zap.Int("l0_segments", len(ct.collBackup.GetL0Segments())))
	if err := ct.restoreL0SegV1(ctx, "", ct.collBackup.GetL0Segments()); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

func (ct *collTask) tearDown(ctx context.Context) error {
	if ct.keepTempFiles {
		ct.logger.Info("skip clean temporary files")
		return nil
	}

	ct.tearDownFn.mu.Lock()
	defer ct.tearDownFn.mu.Unlock()

	ct.logger.Info("restore task tear down")

	slices.Reverse(ct.tearDownFn.fns)
	for _, fn := range ct.tearDownFn.fns {
		if err := fn(ctx); err != nil {
			ct.logger.Error("tear down restore task failed", zap.Error(err))
			return err
		}
	}

	return nil
}

func (ct *collTask) ezk() string {
	if ct.dbBackup.GetEzk() != "" {
		return ct.dbBackup.GetEzk()
	}

	return ""
}

func (ct *collTask) cleanTempFiles(dir string) tearDownFn {
	return func(ctx context.Context) error {
		if len(dir) == 0 {
			return errors.New("restore_collection: empty temporary file dir")
		}

		ct.logger.Info("delete temporary file", zap.String("dir", dir))
		if err := storage.DeletePrefix(ctx, ct.milvusStorage, dir); err != nil {
			return fmt.Errorf("restore_collection: failed to delete temporary file: %w", err)
		}

		return nil
	}
}

func (ct *collTask) copyToMilvusBucket(ctx context.Context, tempDir, srcPrefix string) (string, error) {
	ct.logger.Info("milvus and backup store in different bucket, copy the data first", zap.String("temp_dir", tempDir))
	dest := path.Join(tempDir, strings.Replace(srcPrefix, ct.backupDir, "", 1)) + "/"
	opt := storage.CopyPrefixOpt{
		Sem:          ct.copySem,
		Src:          ct.backupStorage,
		Dest:         ct.milvusStorage,
		SrcPrefix:    srcPrefix,
		DestPrefix:   dest,
		CopyByServer: true,
	}

	ct.logger.Info("copy temporary restore file", zap.String("src", srcPrefix), zap.String("dest", dest))
	task := storage.NewCopyPrefixTask(opt)
	if err := task.Execute(ctx); err != nil {
		return "", fmt.Errorf("restore_collection: copy temporary restore file: %w", err)
	}
	ct.logger.Info("copy temporary restore file success", zap.String("src", srcPrefix), zap.String("dest", dest))

	return dest, nil
}

func (ct *collTask) copyAndRewriteDir(ctx context.Context, b batch) (batch, error) {
	isSameBucket := ct.milvusStorage.Config().Bucket == ct.backupStorage.Config().Bucket
	isSameStorage := ct.backupStorage.Config().Provider == ct.milvusStorage.Config().Provider
	// if milvus bucket and backup bucket are not the same, should copy the data first
	if isSameBucket && isSameStorage && !ct.crossStorage {
		ct.logger.Info("milvus and backup store in the same bucket, no need to copy the data")
		return b, nil
	}

	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s/", ct.taskID, ct.targetNS.DBName(), ct.targetNS.CollName())
	for i, dir := range b.partitionDirs {
		// insert log
		if len(dir.insertLogDir) != 0 {
			insertLogDir, err := ct.copyToMilvusBucket(ctx, tempDir, dir.insertLogDir)
			if err != nil {
				return batch{}, fmt.Errorf("restore_collection: copy insert log dir: %w", err)
			}
			dir.insertLogDir = insertLogDir
		}

		// delta log
		if len(dir.deltaLogDir) != 0 {
			deltaLogDir, err := ct.copyToMilvusBucket(ctx, tempDir, dir.deltaLogDir)
			if err != nil {
				return batch{}, fmt.Errorf("restore_collection: copy delta log dir: %w", err)
			}
			dir.deltaLogDir = deltaLogDir
		}

		b.partitionDirs[i] = dir
	}

	ct.tearDownFn.mu.Lock()
	defer ct.tearDownFn.mu.Unlock()
	ct.tearDownFn.fns = append(ct.tearDownFn.fns, ct.cleanTempFiles(tempDir))
	return b, nil
}

func (ct *collTask) restoreNotL0SegV1(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	notL0SegBatches, err := ct.notL0SegmentBatches(ctx, part)
	if err != nil {
		return fmt.Errorf("restore_collection: get not L0 groups: %w", err)
	}

	for _, b := range notL0SegBatches {
		bat, err := ct.copyAndRewriteDir(ctx, b)
		if err != nil {
			return fmt.Errorf("restore_collection: restore data v1 copy files: %w", err)
		}
		if err := ct.bulkInsertViaGrpc(ctx, part.GetPartitionName(), bat); err != nil {
			return fmt.Errorf("restore_collection: bulk insert via grpc: %w", err)
		}
	}

	return nil

}

func toPaths(dir partitionDir) []string {
	if len(dir.insertLogDir) == 0 {
		return []string{dir.deltaLogDir}
	}
	return []string{dir.insertLogDir, dir.deltaLogDir}
}

func (ct *collTask) restoreNotL0SegV2(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	batches, err := ct.notL0SegmentBatches(ctx, part)
	if err != nil {
		return fmt.Errorf("restore_collection: get not L0 groups: %w", err)
	}

	g, subCtx := errgroup.WithContext(ctx)
	for _, b := range batches {
		if err := ct.bulkInsertSem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("restore_collection: acquire bulk insert semaphore %w", err)
		}
		g.Go(func() error {
			defer ct.bulkInsertSem.Release(1)

			bat, err := ct.copyAndRewriteDir(subCtx, b)
			if err != nil {
				return fmt.Errorf("restore_collection: restore data v2 copy files: %w", err)
			}
			if err := ct.bulkInsertViaRestful(subCtx, part.GetPartitionName(), bat); err != nil {
				return fmt.Errorf("restore_collection: bulk insert via restful: %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for not L0 segment restore: %w", err)
	}

	return nil
}

func (ct *collTask) restoreL0SegV1(ctx context.Context, partitionName string, l0Segs []*backuppb.SegmentBackupInfo) error {
	batches, err := ct.l0SegmentBatches(l0Segs)
	if err != nil {
		return fmt.Errorf("restore_collection: get L0 batches: %w", err)
	}

	for _, b := range batches {
		bat, err := ct.copyAndRewriteDir(ctx, b)
		if err != nil {
			return fmt.Errorf("restore_collection: restore L0 segment copy files: %w", err)
		}
		if err := ct.bulkInsertViaGrpc(ctx, partitionName, bat); err != nil {
			return fmt.Errorf("restore_collection: restore L0 segment bulk insert via grpc: %w", err)
		}
	}

	return nil
}

func (ct *collTask) restoreL0SegV2(ctx context.Context, partitionName string, l0Segs []*backuppb.SegmentBackupInfo) error {
	batches, err := ct.l0SegmentBatches(l0Segs)
	if err != nil {
		return fmt.Errorf("restore_collection: get L0 batches: %w", err)
	}

	g, subCtx := errgroup.WithContext(ctx)
	for _, b := range batches {
		if err := ct.bulkInsertSem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("restore_collection: acquire bulk insert semaphore %w", err)
		}

		g.Go(func() error {
			defer ct.bulkInsertSem.Release(1)

			bat, err := ct.copyAndRewriteDir(subCtx, b)
			if err != nil {
				return fmt.Errorf("restore_collection: restore L0 segment copy files: %w", err)
			}
			if err := ct.bulkInsertViaRestful(subCtx, partitionName, bat); err != nil {
				return fmt.Errorf("restore_collection: restore L0 segment bulk insert via restful: %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for L0 segment restore: %w", err)
	}

	return nil
}

func (ct *collTask) restorePartitionV1(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	ct.logger.Info("start restore not L0 segment", zap.String("partition_name", part.GetPartitionName()))
	// restore not L0 data groups
	if err := ct.restoreNotL0SegV1(ctx, part); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 groups: %w", err)
	}

	ct.logger.Info("start restore L0 segment", zap.String("partition_name", part.GetPartitionName()))
	// restore partition L0 segment
	l0Segs := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
		return seg.IsL0
	})
	if err := ct.restoreL0SegV1(ctx, part.GetPartitionName(), l0Segs); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segment: %w", err)
	}

	return nil
}

func (ct *collTask) restorePartitionV2(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	ct.logger.Info("start restore partition not L0 segment v2", zap.String("partition_name", part.GetPartitionName()))
	if err := ct.restoreNotL0SegV2(ctx, part); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 groups: %w", err)
	}

	ct.logger.Info("start restore partition L0 segment v2", zap.String("partition_name", part.GetPartitionName()))
	l0Seg := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool { return seg.IsL0 })
	if err := ct.restoreL0SegV2(ctx, part.GetPartitionName(), l0Seg); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segment: %w", err)
	}

	return nil
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
		chunkedSegs := lo.Chunk(segs, _bulkInsertRestfulAPIChunkSize)
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
	} else {
		// backward compatible old backup without group id
		return ct.notL0SegBatchesWithoutGroupID(ctx, part)
	}
}

func (ct *collTask) l0SegmentBatches(l0Segs []*backuppb.SegmentBackupInfo) ([]batch, error) {
	segBatch := lo.GroupBy(l0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	chunkSize := 1
	if ct.grpcCli.HasFeature(milvus.MultiL0InOneJob) {
		chunkSize = _bulkInsertRestfulAPIChunkSize
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

func (ct *collTask) checkBulkInsertViaGrpc(ctx context.Context, jobID int64) error {
	// wait for bulk insert job done
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		state, err := ct.grpcCli.GetBulkInsertState(ctx, jobID)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to get bulk insert state: %w", err)
		}

		ct.logger.Info("bulk insert task state", zap.Int64("jobID", jobID), zap.Any("state", state.State),
			zap.Any("backup", state.Infos))
		switch state.State {
		case commonpb.ImportState_ImportFailed:
			return fmt.Errorf("restore_collection: bulk insert failed: %s", getFailedReason(state.Infos))
		case commonpb.ImportState_ImportCompleted:
			ct.logger.Info("bulk insert task success", zap.Int64("job_id", jobID))
			return nil
		default:
			currentProgress := getProcess(state.Infos)
			ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.UpdateRestoreImportJob(ct.targetNS, strconv.FormatInt(jobID, 10), currentProgress))
			if currentProgress > lastProgress {
				lastUpdateTime = time.Now()
			} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
				ct.logger.Warn("bulk insert task timeout", zap.Int64("job_id", jobID),
					zap.Duration("timeout", _bulkInsertTimeout))
				return errors.New("restore_collection: bulk insert timeout")
			}
			continue
		}
	}

	return errors.New("restore_collection: walk into unreachable code")
}

func (ct *collTask) bulkInsertViaGrpc(ctx context.Context, partitionName string, b batch) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, dir := range b.partitionDirs {
		if err := ct.bulkInsertSem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("restore_collection: acquire bulk insert semaphore %w", err)
		}

		g.Go(func() error {
			defer ct.bulkInsertSem.Release(1)

			paths := toPaths(dir)
			ct.logger.Info("start bulk insert via grpc", zap.Strings("paths", paths), zap.String("partition", partitionName))
			in := milvus.GrpcBulkInsertInput{
				DB:             ct.targetNS.DBName(),
				CollectionName: ct.targetNS.CollName(),
				PartitionName:  partitionName,
				Paths:          toPaths(dir),
				BackupTS:       b.timestamp,
				IsL0:           b.isL0,
				StorageVersion: b.storageVersion,
				EZK:            ct.ezk(),
			}

			jobID, err := ct.grpcCli.BulkInsert(subCtx, in)
			if err != nil {
				return fmt.Errorf("restore_collection: failed to bulk insert via grpc: %w", err)
			}
			ct.taskMgr.UpdateRestoreTask(ct.taskID,
				taskmgr.AddRestoreImportJob(ct.targetNS, strconv.FormatInt(jobID, 10), dir.size))
			ct.logger.Info("create bulk insert via grpc success", zap.Int64("job_id", jobID))
			return ct.checkBulkInsertViaGrpc(subCtx, jobID)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: bulk insert via grpc: %w", err)
	}

	return nil
}

func (ct *collTask) checkBulkInsertViaRestful(ctx context.Context, jobID string) error {
	// wait for bulk insert job done
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		resp, err := ct.restfulCli.GetBulkInsertState(ctx, ct.targetNS.DBName(), jobID)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to get bulk insert state: %w", err)
		}

		ct.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return fmt.Errorf("restore_collection: bulk insert failed: %s", resp.Data.Reason)
		case string(milvus.ImportStateCompleted):
			ct.logger.Info("bulk insert task success", zap.String("job_id", jobID))
			ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.UpdateRestoreImportJob(ct.targetNS, jobID, 100))
			return nil
		default:
			currentProgress := resp.Data.Progress
			ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.UpdateRestoreImportJob(ct.targetNS, jobID, currentProgress))
			if currentProgress > lastProgress {
				lastUpdateTime = time.Now()
			} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
				ct.logger.Warn("bulk insert task timeout", zap.String("job_id", jobID),
					zap.Duration("timeout", _bulkInsertTimeout))
				return errors.New("restore_collection: bulk insert timeout")
			}
			continue
		}
	}

	return errors.New("restore_collection: walk into unreachable code")
}

func (ct *collTask) bulkInsertViaRestful(ctx context.Context, partition string, b batch) error {
	ct.logger.Info("start bulk insert via restful", zap.Int("batch_num", len(b.partitionDirs)), zap.String("partition", partition))
	paths := lo.Map(b.partitionDirs, func(dir partitionDir, _ int) []string { return toPaths(dir) })
	in := milvus.BulkInsertV2Input{
		DB:             ct.targetNS.DBName(),
		CollectionName: ct.targetNS.CollName(),
		PartitionName:  partition,
		Paths:          paths,
		BackupTS:       b.timestamp,
		IsL0:           b.isL0,
		StorageVersion: b.storageVersion,
		EZK:            ct.ezk(),
	}

	jobID, err := ct.restfulCli.BulkInsert(ctx, in)
	if err != nil {
		return fmt.Errorf("restore_collection: failed to bulk insert via restful: %w", err)
	}
	ct.logger.Info("create bulk insert via restful success", zap.String("job_id", jobID))

	size := lo.SumBy(b.partitionDirs, func(dir partitionDir) int64 { return dir.size })
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.AddRestoreImportJob(ct.targetNS, jobID, size))
	if err := ct.checkBulkInsertViaRestful(ctx, jobID); err != nil {
		return fmt.Errorf("restore_collection: check bulk insert via restful: %w", err)
	}

	return nil
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

func (ct *collTask) buildBackupPartitionDir(ctx context.Context, size int64, pathOpt ...mpath.Option) (partitionDir, error) {
	insertLogDir := mpath.BackupInsertLogDir(ct.backupDir, pathOpt...)
	deltaLogDir := mpath.BackupDeltaLogDir(ct.backupDir, pathOpt...)

	exist, err := storage.Exist(ctx, ct.backupStorage, deltaLogDir)
	if err != nil {
		return partitionDir{}, fmt.Errorf("restore_collection: check delta log exist: %w", err)
	}

	if exist {
		return partitionDir{insertLogDir: insertLogDir, deltaLogDir: deltaLogDir, size: size}, nil
	} else {
		return partitionDir{insertLogDir: insertLogDir, size: size}, nil
	}
}
