package secondary

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/collref"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

const _bulkInsertRestfulAPIChunkSize = 256

// Overridden in tests so the waits do not take their production time to
// exercise.
var (
	// How long to let the target apply the DDL before concluding that a
	// collection the restore created is not there.
	_restoreVerifyTimeout  = 30 * time.Second
	_restoreVerifyInterval = 3 * time.Second
	// How long to wait, per collection, for a create the restore has just sent
	// to become visible on the target before starting to import into it.
	_collCreateTimeout  = 30 * time.Second
	_collCreateInterval = 1 * time.Second
)

type partitionDir struct {
	insertLogDir string
	deltaLogDir  string

	size int64
}

func (dir *partitionDir) toPaths() []string {
	paths := make([]string, 0, 2)
	if dir.insertLogDir != "" {
		paths = append(paths, dir.insertLogDir)
	}
	if dir.deltaLogDir != "" {
		paths = append(paths, dir.deltaLogDir)
	}
	return paths
}

type batch struct {
	isL0           bool
	timestamp      uint64
	storageVersion int64

	partitionDirs []partitionDir
}

func (b *batch) options() map[string]string {
	opts := map[string]string{
		"skip_disk_quota_check": "true",
		"auto_commit":           "false",
		"end_ts":                strconv.FormatUint(b.timestamp, 10),
		"storage_version":       strconv.FormatInt(b.storageVersion, 10),
	}

	if b.isL0 {
		opts["l0_import"] = "true"
	} else {
		opts["backup"] = "true"
	}

	return opts
}

type batchKey struct {
	vch string
	sv  int64
}

type dmlTaskArgs struct {
	TaskID string

	PchTS map[string]uint64

	BackupStorage storage.Client
	BackupDir     string

	MilvusStorage  storage.Client
	MilvusRootPath string
	Streaming      bool
	CopySem        *semaphore.Weighted

	StreamCli  milvus.Stream
	RestfulCli milvus.Restful
}

type collDMLTask struct {
	taskID string

	backupStorage storage.Client
	backupDir     string

	milvusStorage  storage.Client
	milvusRootPath string
	streaming      bool
	copySem        *semaphore.Weighted

	// tempDirs are the staging prefixes created in milvusStorage for this
	// collection; they are removed when Execute returns.
	tempDirs []string

	pchTS      map[string]uint64
	collBackup *backuppb.CollectionBackupInfo

	streamCli  milvus.Stream
	restfulCli milvus.Restful

	logger *zap.Logger
}

func newCollDMLTask(args dmlTaskArgs, collBackup *backuppb.CollectionBackupInfo) *collDMLTask {
	collRef := collref.New(collBackup.GetDbName(), collBackup.GetCollectionName())

	return &collDMLTask{
		taskID: args.TaskID,

		pchTS:      args.PchTS,
		collBackup: collBackup,

		backupStorage: args.BackupStorage,
		backupDir:     args.BackupDir,

		milvusStorage:  args.MilvusStorage,
		milvusRootPath: args.MilvusRootPath,
		streaming:      args.Streaming,
		copySem:        args.CopySem,

		streamCli:  args.StreamCli,
		restfulCli: args.RestfulCli,

		logger: log.With(zap.String("task_id", args.TaskID), zap.String("coll", collRef.String())),
	}
}

func (dmlt *collDMLTask) Execute(ctx context.Context) (err error) {
	dmlt.logger.Info("start restore collection dml")
	defer func() {
		if cerr := dmlt.cleanTempDirs(context.WithoutCancel(ctx)); cerr != nil {
			dmlt.logger.Warn("clean staged binlogs failed", zap.Error(cerr))
			if err == nil {
				err = cerr
			}
		}
	}()

	// Send and wait for non-L0 imports for all partitions.
	if err := dmlt.restorePartitionNonL0(ctx); err != nil {
		return fmt.Errorf("secondary: restore partition non-L0: %w", err)
	}

	// Send and wait for per-partition L0 imports. L0 must come after non-L0.
	if err := dmlt.restorePartitionL0(ctx); err != nil {
		return fmt.Errorf("secondary: restore partition L0: %w", err)
	}

	// Send and wait for all-partition L0 imports.
	if err := dmlt.restoreAllPartitionL0(ctx); err != nil {
		return fmt.Errorf("secondary: restore all partition l0: %w", err)
	}

	return nil
}

// restorePartitionNonL0 builds and sends non-L0 import messages for all partitions
// sequentially to ensure messages arrive at each physical channel in ts order,
// waiting on each import job from the moment it is submitted.
func (dmlt *collDMLTask) restorePartitionNonL0(ctx context.Context) error {
	g, subCtx := errgroup.WithContext(ctx)
	var count int
	for _, partition := range dmlt.collBackup.GetPartitionBackups() {
		nonL0Segs := lo.Filter(partition.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
			return !seg.IsL0
		})

		batches, err := dmlt.nonL0SegBatches(subCtx, nonL0Segs)
		if err != nil {
			return fmt.Errorf("secondary: build non-L0 batches for partition %s: %w", partition.GetPartitionName(), err)
		}

		n, err := dmlt.sendBatches(subCtx, g, partition.GetPartitionId(), batches)
		count += n
		if err != nil {
			return fmt.Errorf("secondary: send non-L0 for partition %s: %w", partition.GetPartitionName(), err)
		}
	}

	dmlt.logger.Info("check non-l0 bulk insert jobs", zap.Int("job_count", count))
	if err := g.Wait(); err != nil {
		return fmt.Errorf("secondary: check bulk insert jobs: %w", err)
	}
	return nil
}

// restorePartitionL0 builds and sends per-partition L0 import messages sequentially,
// waiting on each import job from the moment it is submitted.
func (dmlt *collDMLTask) restorePartitionL0(ctx context.Context) error {
	g, subCtx := errgroup.WithContext(ctx)
	var count int
	for _, partition := range dmlt.collBackup.GetPartitionBackups() {
		l0Segs := lo.Filter(partition.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
			return seg.IsL0
		})

		batches, err := dmlt.l0SegBatches(l0Segs)
		if err != nil {
			return fmt.Errorf("secondary: build L0 batches for partition %s: %w", partition.GetPartitionName(), err)
		}

		n, err := dmlt.sendBatches(subCtx, g, partition.GetPartitionId(), batches)
		count += n
		if err != nil {
			return fmt.Errorf("secondary: send L0 for partition %s: %w", partition.GetPartitionName(), err)
		}
	}

	dmlt.logger.Info("check l0 bulk insert jobs", zap.Int("job_count", count))
	if err := g.Wait(); err != nil {
		return fmt.Errorf("secondary: check bulk insert jobs: %w", err)
	}
	return nil
}

func (dmlt *collDMLTask) backupTS(vch string) (uint64, error) {
	pch := funcutil.ToPhysicalChannel(vch)

	ts, ok := dmlt.pchTS[pch]
	if !ok {
		return 0, fmt.Errorf("restore: no flush all ts for pch %s", pch)
	}

	return ts, nil
}

func (dmlt *collDMLTask) restoreAllPartitionL0(ctx context.Context) error {
	dmlt.logger.Info("restore all partition l0")

	batches, err := dmlt.l0SegBatches(dmlt.collBackup.GetL0Segments())
	if err != nil {
		return fmt.Errorf("secondary: build all partition l0 batches: %w", err)
	}

	g, subCtx := errgroup.WithContext(ctx)
	if _, err := dmlt.sendBatches(subCtx, g, common.AllPartitionsID, batches); err != nil {
		return fmt.Errorf("secondary: send all partition l0: %w", err)
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("secondary: check all partition l0 jobs: %w", err)
	}

	dmlt.logger.Info("restore all partition l0 done")

	return nil
}

// sendBatches submits every batch in order and starts waiting on each import job
// as it is submitted, returning the number submitted.
//
// The waiting cannot be deferred until every batch has been sent. Submitting a
// batch stages its binlogs first, so a collection of any size spends a long time
// in this loop -- a 173 GB collection in 209 batches took thirty-seven minutes --
// while the target keeps a finished job's record only for
// dataCoord.import.taskRetention past its completion. A job submitted early
// therefore finishes and is reclaimed long before a deferred wait would first
// ask about it, and the restore then cannot tell a job that succeeded and was
// forgotten from one that never ran.
func (dmlt *collDMLTask) sendBatches(ctx context.Context, g *errgroup.Group, partitionID int64, batches []batch) (int, error) {
	for i, b := range batches {
		jobID, err := dmlt.sendImportMsg(ctx, partitionID, b)
		if err != nil {
			return i, fmt.Errorf("secondary: send import msg: %w", err)
		}
		g.Go(func() error {
			if err := dmlt.importJob().check(ctx, jobID); err != nil {
				return fmt.Errorf("secondary: check bulk insert job %d: %w", jobID, err)
			}
			return nil
		})
	}
	return len(batches), nil
}

// importJob is the lifecycle view of the import jobs this collection submits,
// read off the task's fields at call time so a client swapped in later is
// honored.
func (dmlt *collDMLTask) importJob() bulkInsertJob {
	return bulkInsertJob{
		dbName:       dmlt.collBackup.GetDbName(),
		collectionID: dmlt.collBackup.GetCollectionId(),
		vchannels:    dmlt.collBackup.GetVirtualChannelNames(),

		streamCli:  dmlt.streamCli,
		restfulCli: dmlt.restfulCli,

		logger: dmlt.logger,
	}
}

func (dmlt *collDMLTask) nonL0SegBatches(ctx context.Context, segs []*backuppb.SegmentBackupInfo) ([]batch, error) {
	// group by vchannel and storage version
	segBatch := lo.GroupBy(segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	var batches []batch
	for key, segs := range segBatch {
		ts, err := dmlt.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("secondary: get vch %s ts: %w", key.vch, err)
		}

		// because the restful api has a limitation on the number of segments in one request,
		// we need to chunk the segments into multiple batches
		for chunk := range slices.Chunk(segs, _bulkInsertRestfulAPIChunkSize) {
			dirs := make([]partitionDir, 0, len(chunk))
			for _, seg := range chunk {
				opts := []mpath.Option{
					mpath.CollectionID(dmlt.collBackup.GetCollectionId()),
					mpath.PartitionID(seg.GetPartitionId()),
					mpath.GroupID(seg.GetGroupId()),
				}

				dir, err := dmlt.buildBackupPartitionDir(ctx, seg.GetSize(), opts...)
				if err != nil {
					return nil, fmt.Errorf("secondary: get partition backup binlog files: %w", err)
				}
				dirs = append(dirs, dir)
			}

			b := batch{timestamp: ts, partitionDirs: dirs, storageVersion: key.sv}
			batches = append(batches, b)
		}
	}

	dmlt.logger.Info("build non-l0 batches done", zap.Int("batch_num", len(batches)))

	return batches, nil
}

func (dmlt *collDMLTask) l0SegBatches(l0Segs []*backuppb.SegmentBackupInfo) ([]batch, error) {
	segBatch := lo.GroupBy(l0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	var batches []batch
	for key, segs := range segBatch {
		ts, err := dmlt.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get vch %s ts: %w", key.vch, err)
		}

		for chunk := range slices.Chunk(segs, _bulkInsertRestfulAPIChunkSize) {
			dirs := make([]partitionDir, 0, len(chunk))
			for _, seg := range chunk {
				opts := []mpath.Option{
					mpath.CollectionID(dmlt.collBackup.GetCollectionId()),
					mpath.PartitionID(seg.GetPartitionId()),
					mpath.SegmentID(seg.GetSegmentId()),
				}

				deltaLogDir := mpath.BackupDeltaLogDir(dmlt.backupDir, opts...)
				dirs = append(dirs, partitionDir{deltaLogDir: deltaLogDir, size: seg.GetSize()})
			}
			b := batch{isL0: true, timestamp: ts, partitionDirs: dirs, storageVersion: key.sv}
			batches = append(batches, b)
		}
	}

	dmlt.logger.Info("build l0 batches done", zap.Int("batch_num", len(batches)))

	return batches, nil
}

func (dmlt *collDMLTask) buildImportFiles(b batch) []*msgpb.ImportFile {
	files := make([]*msgpb.ImportFile, 0, len(b.partitionDirs))

	for i, dir := range b.partitionDirs {
		importFile := &msgpb.ImportFile{
			Id:    int64(i + 1),
			Paths: dir.toPaths(),
		}

		files = append(files, importFile)
	}

	return files
}

func (dmlt *collDMLTask) sendImportMsg(ctx context.Context, partitionID int64, b batch) (int64, error) {
	jobID := rand.Int64()
	b, err := dmlt.stageBatch(ctx, b)
	if err != nil {
		return 0, fmt.Errorf("secondary: stage binlogs into milvus storage: %w", err)
	}
	schema, err := conv.Schema(dmlt.collBackup.GetSchema())
	if err != nil {
		return 0, fmt.Errorf("secondary: convert schema: %w", err)
	}
	if err := checkDynamicField(schema); err != nil {
		return 0, err
	}
	appendSysFields(schema)

	err = dmlt.streamCli.Send(ctx, func(ts uint64) []message.MutableMessage {
		header := &message.ImportMessageHeader{}
		body := &message.ImportMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Import,
				Timestamp: ts,
			},
			Options:        b.options(),
			DbName:         dmlt.collBackup.GetDbName(),
			CollectionName: dmlt.collBackup.GetCollectionName(),
			CollectionID:   dmlt.collBackup.GetCollectionId(),
			PartitionIDs:   []int64{partitionID},
			Files:          dmlt.buildImportFiles(b),
			Schema:         schema,
			JobID:          jobID,
		}

		builder := message.NewImportMessageBuilderV1().
			WithHeader(header).
			WithBody(body).
			WithBroadcast(dmlt.collBackup.GetVirtualChannelNames())

		broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
		return broadcast.SplitIntoMutableMessage()
	})
	if err != nil {
		return 0, fmt.Errorf("secondary: broadcast import: %w", err)
	}

	return jobID, nil
}

func (dmlt *collDMLTask) buildBackupPartitionDir(ctx context.Context, size int64, pathOpt ...mpath.Option) (partitionDir, error) {
	insertLogDir := mpath.BackupInsertLogDir(dmlt.backupDir, pathOpt...)
	deltaLogDir := mpath.BackupDeltaLogDir(dmlt.backupDir, pathOpt...)

	exist, err := storage.Exist(ctx, dmlt.backupStorage, deltaLogDir)
	if err != nil {
		return partitionDir{}, fmt.Errorf("secondary: check delta log exist: %w", err)
	}

	if exist {
		return partitionDir{insertLogDir: insertLogDir, deltaLogDir: deltaLogDir, size: size}, nil
	}

	return partitionDir{insertLogDir: insertLogDir, size: size}, nil
}

// needStaging reports whether binlogs must be copied into the target's own
// storage before import. DataCoord resolves import paths only against its own
// bucket, so a backup kept in a different bucket or backend is unreachable to
// it. Mirrors the rule used by the plain restore path (copyAndRewriteDir).
func (dmlt *collDMLTask) needStaging() bool {
	if dmlt.milvusStorage == nil {
		return false
	}
	isSameBucket := dmlt.milvusStorage.Config().Bucket == dmlt.backupStorage.Config().Bucket
	isSameStorage := dmlt.milvusStorage.Config().Provider == dmlt.backupStorage.Config().Provider
	return !isSameBucket || !isSameStorage || dmlt.streaming
}

func (dmlt *collDMLTask) isLocal() bool {
	return dmlt.milvusStorage.Config().Provider == cfg.ProviderLocal
}

// destKey maps a bucket-relative key onto the key the milvus storage client
// reads and writes; only the local provider needs the root path prefixed.
func (dmlt *collDMLTask) destKey(key string) string {
	if !dmlt.isLocal() || key == "" {
		return key
	}
	return strings.TrimSuffix(dmlt.milvusRootPath, "/") + "/" + key
}

// stageBatch copies every partition dir of b from the backup storage into a
// temporary prefix in the milvus storage and rewrites the batch to point at the
// copies. It is a no-op when the two storages are the same bucket.
func (dmlt *collDMLTask) stageBatch(ctx context.Context, b batch) (batch, error) {
	if !dmlt.needStaging() {
		return b, nil
	}

	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s/", dmlt.taskID,
		dmlt.collBackup.GetDbName(), dmlt.collBackup.GetCollectionName())
	for i, dir := range b.partitionDirs {
		if dir.insertLogDir != "" {
			staged, err := dmlt.copyToMilvusStorage(ctx, tempDir, dir.insertLogDir)
			if err != nil {
				return batch{}, fmt.Errorf("secondary: stage insert log dir: %w", err)
			}
			dir.insertLogDir = staged
		}
		if dir.deltaLogDir != "" {
			staged, err := dmlt.copyToMilvusStorage(ctx, tempDir, dir.deltaLogDir)
			if err != nil {
				return batch{}, fmt.Errorf("secondary: stage delta log dir: %w", err)
			}
			dir.deltaLogDir = staged
		}
		b.partitionDirs[i] = dir
	}
	dmlt.tempDirs = append(dmlt.tempDirs, tempDir)
	return b, nil
}

func (dmlt *collDMLTask) copyToMilvusStorage(ctx context.Context, tempDir, srcPrefix string) (string, error) {
	dest := path.Join(tempDir, strings.Replace(srcPrefix, dmlt.backupDir, "", 1)) + "/"
	destKey := dmlt.destKey(dest)
	dmlt.logger.Info("milvus and backup store in different bucket, stage binlogs first",
		zap.String("src", srcPrefix), zap.String("dest", destKey))

	task := storage.NewCopyPrefixTask(storage.CopyPrefixOpt{
		Sem:        dmlt.copySem,
		Src:        dmlt.backupStorage,
		Dest:       dmlt.milvusStorage,
		SrcPrefix:  srcPrefix,
		DestPrefix: destKey,
		Streaming:  true,
	})
	if err := task.Execute(ctx); err != nil {
		return "", fmt.Errorf("secondary: copy binlogs: %w", err)
	}

	expected, err := storage.ExpectedDestObjects(ctx, dmlt.backupStorage, srcPrefix, destKey)
	if err != nil {
		return "", fmt.Errorf("secondary: build expected for copy verify: %w", err)
	}
	verify := storage.NewVerifyPrefixTask(storage.VerifyPrefixOpt{Cli: dmlt.milvusStorage, Prefix: destKey, Expected: expected})
	if err := verify.Execute(ctx); err != nil {
		return "", fmt.Errorf("secondary: verify staged binlogs: %w", err)
	}
	return dest, nil
}

func (dmlt *collDMLTask) cleanTempDirs(ctx context.Context) error {
	for _, dir := range dmlt.tempDirs {
		dmlt.logger.Info("delete staged binlogs", zap.String("dir", dir))
		if err := storage.DeletePrefix(ctx, dmlt.milvusStorage, dmlt.destKey(dir)); err != nil {
			return fmt.Errorf("secondary: delete staged binlogs %s: %w", dir, err)
		}
	}
	dmlt.tempDirs = nil
	return nil
}
