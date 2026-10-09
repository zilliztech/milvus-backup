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
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// collDMLTask restores one collection's DML from a binlog backup: it builds
// the dir groups of every partition, hands them to the import planner, and
// runs the import tasks. The collection's DDL is not its business; the plan
// that schedules this task runs the DDL first.
type collDMLTask struct {
	taskID string

	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	option *Option

	store *jobstate.Store

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
	// directory (milvus.storage.rootPath), and milvusLocalPath is what the
	// Milvus process itself resolves (milvus.storage.localPath, falling back
	// to rootPath). Both empty unless the target storage provider is local.
	milvusRootPath  string
	milvusLocalPath string

	grpcCli    milvus.Grpc
	restfulCli milvus.Restful

	// importer organizes the dir groups into import tasks. Set by the plan
	// that schedules this task: grpc or restful, per the restore option.
	importer importPlanner

	vchTimestamp struct {
		mu    sync.RWMutex
		vchTS map[string]uint64
	}

	logger *zap.Logger
}

type collDMLTaskArgs struct {
	taskID string

	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	target collref.Name

	option *Option

	store *jobstate.Store

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

func newCollDMLTask(args collDMLTaskArgs) *collDMLTask {
	src := collref.New(args.collBackup.GetDbName(), args.collBackup.GetCollectionName())

	logger := log.With(
		zap.String("restore_task_id", args.taskID),
		zap.String("backup_coll", src.String()),
		zap.String("target_coll", args.target.String()))

	size := lo.SumBy(args.collBackup.GetPartitionBackups(), func(partition *backuppb.PartitionBackupInfo) int64 {
		return partition.GetSize()
	})
	args.store.UpdateRestoreTask(args.taskID, jobstate.AddRestoreCollTask(args.target, size))

	return &collDMLTask{
		taskID: args.taskID,

		dbBackup:   args.dbBackup,
		collBackup: args.collBackup,

		option: args.option,

		target: args.target,

		store: args.store,

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

func (dt *collDMLTask) Execute(ctx context.Context) error {
	if dt.option.MetaOnly {
		dt.logger.Info("skip restore data")
		return nil
	}

	// restore all partition segment
	dt.logger.Info("start restore partition segment", zap.Int("partition_num", len(dt.collBackup.GetPartitionBackups())))
	g, subCtx := errgroup.WithContext(ctx)
	for _, part := range dt.collBackup.GetPartitionBackups() {
		g.Go(func() error {
			if err := dt.restorePartition(subCtx, part); err != nil {
				return fmt.Errorf("restore_collection: restore partition: %w", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for partition restore: %w", err)
	}

	// restore all partition l0 segment
	dt.logger.Info("start restore all partition L0 segment", zap.Int("l0_segments", len(dt.collBackup.GetL0Segments())))
	l0Groups, err := dt.l0DirGroups(dt.collBackup.GetL0Segments())
	if err != nil {
		return fmt.Errorf("restore_collection: build L0 dir groups: %w", err)
	}
	l0Tasks := dt.importer.planTasks("", l0Groups)
	if err := runTasks(ctx, dt.bulkInsertSem, l0Tasks); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

// restorePartition imports the partition's leveled segments, then its L0
// segments: L0 data must land after the leveled data it supersedes, so the two
// runs are ordered barriers rather than one concurrent pool.
func (dt *collDMLTask) restorePartition(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	dt.logger.Info("start restore not L0 segment", zap.String("partition_name", part.GetPartitionName()))
	notL0Groups, err := dt.notL0DirGroups(ctx, part)
	if err != nil {
		return fmt.Errorf("restore_collection: build not L0 dir groups: %w", err)
	}
	notL0Tasks := dt.importer.planTasks(part.GetPartitionName(), notL0Groups)
	if err := runTasks(ctx, dt.bulkInsertSem, notL0Tasks); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 segments: %w", err)
	}

	dt.logger.Info("start restore L0 segment", zap.String("partition_name", part.GetPartitionName()))
	l0Segs := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
		return seg.IsL0
	})
	l0Groups, err := dt.l0DirGroups(l0Segs)
	if err != nil {
		return fmt.Errorf("restore_collection: build L0 dir groups: %w", err)
	}
	l0Tasks := dt.importer.planTasks(part.GetPartitionName(), l0Groups)
	if err := runTasks(ctx, dt.bulkInsertSem, l0Tasks); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segments: %w", err)
	}

	return nil
}

// newCopyTask builds the copy task that stages an import task's data into the
// target's storage, with a temp dir unique to the import task so the task
// cleans up after itself. It returns nil when backup and target share a bucket
// and the transfer is not streamed, in which case Milvus imports the backup in
// place.
func (dt *collDMLTask) newCopyTask() *copyTask {
	isSameBucket := dt.milvusStorage.Config().Bucket == dt.backupStorage.Config().Bucket
	isSameStorage := dt.backupStorage.Config().Provider == dt.milvusStorage.Config().Provider
	if isSameBucket && isSameStorage && !dt.streaming {
		return nil
	}

	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s-%s/",
		dt.taskID, dt.target.DBName(), dt.target.CollName(), uuid.NewString())
	return &copyTask{
		src:       dt.backupStorage,
		dest:      dt.milvusStorage,
		backupDir: dt.backupDir,
		tempDir:   tempDir,
		sem:       dt.copySem,

		milvusRootPath: dt.milvusRootPath,

		logger: dt.logger,
	}
}

func (dt *collDMLTask) ezk() string {
	oldEZK := dt.dbBackup.GetEzk()
	if oldEZK == "" {
		return ""
	}

	if len(dt.option.EZKMapping) > 0 {
		if newEZK, ok := dt.option.EZKMapping[oldEZK]; ok {
			return newEZK
		}
	}

	return oldEZK
}

func (dt *collDMLTask) backupTS(vch string) (uint64, error) {
	if !dt.option.TruncateBinlogByTs {
		return 0, nil
	}

	if len(vch) == 0 {
		return 0, fmt.Errorf("restore_collection: empty vch but truncate binlog by ts is set")
	}

	dt.vchTimestamp.mu.RLock()
	// fast path, if the timestamp is already cached
	if ts, ok := dt.vchTimestamp.vchTS[vch]; ok {
		dt.vchTimestamp.mu.RUnlock()
		return ts, nil
	}
	dt.vchTimestamp.mu.RUnlock()

	// slow path, if the timestamp is not cached, get it from backup
	posStr, ok := dt.collBackup.GetChannelCheckpoints()[vch]
	if !ok {
		return 0, fmt.Errorf("restore_collection: failed to get vch %s checkpoint", vch)
	}
	pos, err := pbconv.Base64DecodeMsgPosition(posStr)
	if err != nil {
		return 0, fmt.Errorf("restore_collection: failed to decode checkpoint: %w", err)
	}
	ts := pos.GetTimestamp()

	// cache the timestamp, it is idempotence so no need to check if it is already cached.
	dt.vchTimestamp.mu.Lock()
	defer dt.vchTimestamp.mu.Unlock()
	dt.vchTimestamp.vchTS[vch] = ts
	return ts, nil
}

func (dt *collDMLTask) notL0DirGroupsWithoutGroupID(ctx context.Context, part *backuppb.PartitionBackupInfo) ([]dirGroup, error) {
	if dt.option.TruncateBinlogByTs {
		return nil, fmt.Errorf("restore: truncate binlog by ts is not supported if group id is not set in backup")
	}

	opts := []mpath.Option{
		mpath.CollectionID(dt.collBackup.GetCollectionId()),
		mpath.PartitionID(part.GetPartitionId()),
	}
	partDir, err := dt.buildBackupPartitionDir(ctx, part.GetSize(), opts...)
	if err != nil {
		return nil, fmt.Errorf("restore_collection: get partition backup binlog files: %w", err)
	}

	dt.logger.Info("build dir groups without group id", zap.String("partition", part.GetPartitionName()))
	return []dirGroup{{dirs: []partitionDir{partDir}}}, nil
}

type batchKey struct {
	vch string
	sv  int64
}

func (dt *collDMLTask) notL0DirGroupsWithGroupID(ctx context.Context, notL0Segs []*backuppb.SegmentBackupInfo) ([]dirGroup, error) {
	// group by vchannel and storage version
	segGroups := lo.GroupBy(notL0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	var groups []dirGroup
	for key, segs := range segGroups {
		ts, err := dt.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get vch %s ts: %w", key.vch, err)
		}

		dirs := make([]partitionDir, 0, len(segs))
		for _, seg := range segs {
			opts := []mpath.Option{
				mpath.CollectionID(dt.collBackup.GetCollectionId()),
				mpath.PartitionID(seg.GetPartitionId()),
				mpath.GroupID(seg.GetGroupId()),
			}

			dir, err := dt.buildBackupPartitionDir(ctx, seg.GetSize(), opts...)
			if err != nil {
				return nil, fmt.Errorf("restore_collection: get partition backup binlog files: %w", err)
			}
			dirs = append(dirs, dir)
		}

		groups = append(groups, dirGroup{timestamp: ts, storageVersion: key.sv, dirs: dirs})
	}

	dt.logger.Info("build dir groups with group id done", zap.Int("group_num", len(groups)))

	return groups, nil
}

func (dt *collDMLTask) notL0DirGroups(ctx context.Context, part *backuppb.PartitionBackupInfo) ([]dirGroup, error) {
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
		dt.logger.Info("no not L0 segments found")
		return nil, nil
	}

	if withGroupID {
		return dt.notL0DirGroupsWithGroupID(ctx, notL0Segs)
	}
	// backward compatible old backup without group id
	return dt.notL0DirGroupsWithoutGroupID(ctx, part)
}

func (dt *collDMLTask) l0DirGroups(l0Segs []*backuppb.SegmentBackupInfo) ([]dirGroup, error) {
	segGroups := lo.GroupBy(l0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	var groups []dirGroup
	for key, segs := range segGroups {
		ts, err := dt.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get vch %s ts: %w", key.vch, err)
		}

		dirs := make([]partitionDir, 0, len(segs))
		for _, seg := range segs {
			opts := []mpath.Option{
				mpath.CollectionID(dt.collBackup.GetCollectionId()),
				mpath.PartitionID(seg.GetPartitionId()),
				mpath.SegmentID(seg.GetSegmentId()),
			}

			deltaLogDir := mpath.BackupDeltaLogDir(dt.backupDir, opts...)
			dirs = append(dirs, partitionDir{deltaLogDir: deltaLogDir, size: seg.GetSize()})
		}
		groups = append(groups, dirGroup{isL0: true, timestamp: ts, storageVersion: key.sv, dirs: dirs})
	}

	return groups, nil
}

type partitionDir struct {
	insertLogDir string
	deltaLogDir  string

	size int64
}

// dirGroup is a set of partition dirs that share one import request's
// parameters: same vchannel (hence same backup timestamp) and same storage
// version, or the L0 segments of one vchannel. How the dirs of a group split
// into import jobs is decided by the import planner that consumes the group.
type dirGroup struct {
	isL0           bool
	timestamp      uint64
	storageVersion int64

	dirs []partitionDir
}

func (dt *collDMLTask) buildBackupPartitionDir(ctx context.Context, size int64, pathOpt ...mpath.Option) (partitionDir, error) {
	insertLogDir := mpath.BackupInsertLogDir(dt.backupDir, pathOpt...)
	deltaLogDir := mpath.BackupDeltaLogDir(dt.backupDir, pathOpt...)

	exist, err := storage.Exist(ctx, dt.backupStorage, deltaLogDir)
	if err != nil {
		return partitionDir{}, fmt.Errorf("restore_collection: check delta log exist: %w", err)
	}

	if exist {
		return partitionDir{insertLogDir: insertLogDir, deltaLogDir: deltaLogDir, size: size}, nil
	}
	return partitionDir{insertLogDir: insertLogDir, size: size}, nil
}
