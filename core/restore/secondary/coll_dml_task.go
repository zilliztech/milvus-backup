package secondary

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/internal/namespace"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

const (
	_bulkInsertTimeout             = 60 * time.Minute
	_bulkInsertRestfulAPIChunkSize = 256
	_bulkInsertCheckInterval       = 3 * time.Second
)

type partitionDir struct {
	insertLogDir string
	deltaLogDir  string

	size int64
}

func (dir *partitionDir) toPaths() []string {
	if len(dir.insertLogDir) == 0 {
		return []string{dir.deltaLogDir}
	}
	return []string{dir.insertLogDir, dir.deltaLogDir}
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

	TSAlloc *tsAlloc

	PchTS map[string]uint64

	BackupStorage storage.Client
	BackupDir     string

	StreamCli  milvus.Stream
	RestfulCli milvus.Restful
}

type collDMLTask struct {
	taskID string

	tsAlloc *tsAlloc

	backupStorage storage.Client
	backupDir     string

	pchTS      map[string]uint64
	collBackup *backuppb.CollectionBackupInfo

	streamCli  milvus.Stream
	restfulCli milvus.Restful

	logger *zap.Logger
}

func newCollDMLTask(args dmlTaskArgs, collBackup *backuppb.CollectionBackupInfo) *collDMLTask {
	ns := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())

	return &collDMLTask{
		taskID: args.TaskID,

		tsAlloc: args.TSAlloc,

		pchTS:      args.PchTS,
		collBackup: collBackup,

		backupStorage: args.BackupStorage,
		backupDir:     args.BackupDir,

		streamCli:  args.StreamCli,
		restfulCli: args.RestfulCli,

		logger: log.With(zap.String("task_id", args.TaskID), zap.String("ns", ns.String())),
	}
}

func (dmlt *collDMLTask) Execute(ctx context.Context) error {
	dmlt.logger.Info("start restore collection dml")

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
// then waits for all import jobs concurrently.
func (dmlt *collDMLTask) restorePartitionNonL0(ctx context.Context) error {
	var jobIDs []int64
	for _, partition := range dmlt.collBackup.GetPartitionBackups() {
		var nonL0Segs []*backuppb.SegmentBackupInfo
		for _, seg := range partition.GetSegmentBackups() {
			if !seg.IsL0 {
				nonL0Segs = append(nonL0Segs, seg)
			}
		}

		batches, err := dmlt.nonL0SegBatches(ctx, nonL0Segs)
		if err != nil {
			return fmt.Errorf("secondary: build non-L0 batches for partition %s: %w", partition.GetPartitionName(), err)
		}

		ids, err := dmlt.sendBatches(ctx, partition.GetPartitionId(), batches)
		if err != nil {
			return fmt.Errorf("secondary: send non-L0 for partition %s: %w", partition.GetPartitionName(), err)
		}
		jobIDs = append(jobIDs, ids...)
	}

	dmlt.logger.Info("check non-l0 bulk insert jobs", zap.Int("job_count", len(jobIDs)))
	return dmlt.checkBulkInsertJobs(ctx, jobIDs)
}

// restorePartitionL0 builds and sends per-partition L0 import messages sequentially,
// then waits for all import jobs concurrently.
func (dmlt *collDMLTask) restorePartitionL0(ctx context.Context) error {
	var jobIDs []int64
	for _, partition := range dmlt.collBackup.GetPartitionBackups() {
		var l0Segs []*backuppb.SegmentBackupInfo
		for _, seg := range partition.GetSegmentBackups() {
			if seg.IsL0 {
				l0Segs = append(l0Segs, seg)
			}
		}

		batches, err := dmlt.l0SegBatches(l0Segs)
		if err != nil {
			return fmt.Errorf("secondary: build L0 batches for partition %s: %w", partition.GetPartitionName(), err)
		}

		ids, err := dmlt.sendBatches(ctx, partition.GetPartitionId(), batches)
		if err != nil {
			return fmt.Errorf("secondary: send L0 for partition %s: %w", partition.GetPartitionName(), err)
		}
		jobIDs = append(jobIDs, ids...)
	}

	dmlt.logger.Info("check l0 bulk insert jobs", zap.Int("job_count", len(jobIDs)))
	return dmlt.checkBulkInsertJobs(ctx, jobIDs)
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

	jobIDs, err := dmlt.sendBatches(ctx, common.AllPartitionsID, batches)
	if err != nil {
		return fmt.Errorf("secondary: send all partition l0: %w", err)
	}

	if err := dmlt.checkBulkInsertJobs(ctx, jobIDs); err != nil {
		return fmt.Errorf("secondary: check all partition l0 jobs: %w", err)
	}

	dmlt.logger.Info("restore all partition l0 done")

	return nil
}

func (dmlt *collDMLTask) sendBatches(ctx context.Context, partitionID int64, batches []batch) ([]int64, error) {
	jobIDs := make([]int64, 0, len(batches))
	for _, b := range batches {
		jobID, err := dmlt.sendImportMsg(ctx, partitionID, b)
		if err != nil {
			return nil, fmt.Errorf("secondary: send import msg: %w", err)
		}
		jobIDs = append(jobIDs, jobID)
	}
	return jobIDs, nil
}

func (dmlt *collDMLTask) checkBulkInsertJobs(ctx context.Context, jobIDs []int64) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, jobID := range jobIDs {
		g.Go(func() error {
			if err := dmlt.checkBulkInsertJob(subCtx, strconv.Itoa(int(jobID))); err != nil {
				return fmt.Errorf("secondary: check bulk insert job %d: %w", jobID, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("secondary: check bulk insert jobs: %w", err)
	}

	return nil
}

func (dmlt *collDMLTask) checkBulkInsertJob(ctx context.Context, jobID string) error {
	// wait for bulk insert job done
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		resp, err := dmlt.restfulCli.GetBulkInsertState(ctx, dmlt.collBackup.GetDbName(), jobID)
		if err != nil {
			return fmt.Errorf("secondary: get bulk insert state: %w", err)
		}

		dmlt.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return fmt.Errorf("secondary: bulk insert failed: %s", resp.Data.Reason)
		case string(milvus.ImportStateCompleted):
			dmlt.logger.Info("bulk insert task success", zap.String("job_id", jobID))
			return nil
		default:
			currentProgress := resp.Data.Progress
			if currentProgress > lastProgress {
				lastUpdateTime = time.Now()
			} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
				dmlt.logger.Warn("bulk insert task timeout", zap.String("job_id", jobID),
					zap.Duration("timeout", _bulkInsertTimeout))
				return errors.New("secondary: bulk insert timeout")
			}
			continue
		}
	}

	return errors.New("secondary: walk into unreachable code")
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
		chunkedSegs := lo.Chunk(segs, _bulkInsertRestfulAPIChunkSize)
		for _, chunk := range chunkedSegs {
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

		chunkedSegs := lo.Chunk(segs, _bulkInsertRestfulAPIChunkSize)
		for _, chunk := range chunkedSegs {
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
	schema, err := conv.Schema(dmlt.collBackup.GetSchema())
	if err != nil {
		return 0, fmt.Errorf("secondary: convert schema: %w", err)
	}
	appendSysFields(schema)
	appendDynamicField(schema)

	ts := dmlt.tsAlloc.Alloc()
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
	msgs := broadcast.SplitIntoMutableMessage()

	for _, msg := range msgs {
		immutableMessage := msg.WithTimeTick(ts).
			WithLastConfirmed(newFakeMessageID(ts)).
			IntoImmutableMessage(newFakeMessageID(ts)).
			IntoImmutableMessageProto()

		if err := dmlt.streamCli.Send(ctx, immutableMessage); err != nil {
			return 0, fmt.Errorf("secondary: broadcast import: %w", err)
		}
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
