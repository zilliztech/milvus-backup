package secondary

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/zilliztech/milvus-backup/internal/namespace"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

const (
	_bulkInsertTimeout             = 60 * time.Minute
	_bulkInsertRestfulAPIChunkSize = 256
	_bulkInsertCheckInterval       = 3 * time.Second
	_commitImportMessageType       = 45
	_commitImportMessageVersion    = 2
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
	ns := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())

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

		logger: log.With(zap.String("task_id", args.TaskID), zap.String("ns", ns.String())),
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
// then waits for all import jobs concurrently.
func (dmlt *collDMLTask) restorePartitionNonL0(ctx context.Context) error {
	var jobIDs []int64
	for _, partition := range dmlt.collBackup.GetPartitionBackups() {
		nonL0Segs := lo.Filter(partition.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
			return !seg.IsL0
		})

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
		l0Segs := lo.Filter(partition.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
			return seg.IsL0
		})

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
			if err := dmlt.checkBulkInsertJob(subCtx, jobID); err != nil {
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

func (dmlt *collDMLTask) checkBulkInsertJob(ctx context.Context, jobID int64) error {
	state, err := dmlt.waitBulkInsertReadyToCommit(ctx, jobID)
	if err != nil {
		return err
	}

	switch state {
	case milvus.ImportStateCompleted:
		return nil
	case milvus.ImportStateUncommitted:
		if err := dmlt.sendCommitImportMsg(ctx, jobID); err != nil {
			return fmt.Errorf("secondary: send commit import msg: %w", err)
		}
	case milvus.ImportStateCommitting:
		// Another coordinator already initiated the commit; only wait for completion.
	default:
		return fmt.Errorf("secondary: unexpected bulk insert state %s", state)
	}

	return dmlt.waitBulkInsertCompleted(ctx, jobID)
}

func (dmlt *collDMLTask) waitBulkInsertReadyToCommit(ctx context.Context, jobID int64) (milvus.ImportState, error) {
	return dmlt.waitBulkInsertState(ctx, jobID, func(state milvus.ImportState) bool {
		return state == milvus.ImportStateCompleted ||
			state == milvus.ImportStateUncommitted ||
			state == milvus.ImportStateCommitting
	})
}

func (dmlt *collDMLTask) waitBulkInsertCompleted(ctx context.Context, jobID int64) error {
	_, err := dmlt.waitBulkInsertState(ctx, jobID, func(state milvus.ImportState) bool {
		return state == milvus.ImportStateCompleted
	})
	return err
}

func (dmlt *collDMLTask) waitBulkInsertState(
	ctx context.Context,
	jobID int64,
	done func(milvus.ImportState) bool,
) (milvus.ImportState, error) {
	jobIDStr := strconv.FormatInt(jobID, 10)

	var lastProgress int
	lastUpdateTime := time.Now()
	ticker := time.NewTicker(_bulkInsertCheckInterval)
	defer ticker.Stop()

	for {
		resp, err := dmlt.restfulCli.GetBulkInsertState(ctx, dmlt.collBackup.GetDbName(), jobIDStr)
		if err != nil {
			return "", fmt.Errorf("secondary: get bulk insert state: %w", err)
		}

		state := milvus.ImportState(resp.Data.State)
		dmlt.logger.Info("bulk insert task state", zap.Int64("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		if state == milvus.ImportStateFailed {
			return "", fmt.Errorf("secondary: bulk insert failed: %s", resp.Data.Reason)
		}
		if done(state) {
			if state == milvus.ImportStateCompleted {
				dmlt.logger.Info("bulk insert task success", zap.Int64("job_id", jobID))
			}
			return state, nil
		}

		currentProgress := resp.Data.Progress
		if currentProgress > lastProgress {
			lastProgress = currentProgress
			lastUpdateTime = time.Now()
		} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
			dmlt.logger.Warn("bulk insert task no progress for too long, may milvus is not healthy",
				zap.Int64("job_id", jobID),
				zap.Duration("timeout", _bulkInsertTimeout))
			lastUpdateTime = time.Now()
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func (dmlt *collDMLTask) sendCommitImportMsg(ctx context.Context, jobID int64) error {
	header := protowire.AppendTag(nil, 1, protowire.VarintType)
	header = protowire.AppendVarint(header, uint64(dmlt.collBackup.GetCollectionId()))
	header = protowire.AppendTag(header, 2, protowire.VarintType)
	header = protowire.AppendVarint(header, uint64(jobID))

	broadcastHeader, err := message.EncodeProto(&messagespb.BroadcastHeader{
		Vchannels: dmlt.collBackup.GetVirtualChannelNames(),
	})
	if err != nil {
		return fmt.Errorf("secondary: encode commit import broadcast header: %w", err)
	}

	properties := map[string]string{
		"_t":  strconv.Itoa(_commitImportMessageType),
		"_v":  strconv.Itoa(_commitImportMessageVersion),
		"_h":  base64.StdEncoding.EncodeToString(header),
		"_bh": broadcastHeader,
	}

	if err := dmlt.streamCli.Send(ctx, func(uint64) []message.MutableMessage {
		broadcast := message.NewBroadcastMutableMessageBeforeAppend(nil, properties).
			WithBroadcastID(rand.Uint64())
		return broadcast.SplitIntoMutableMessage()
	}); err != nil {
		return fmt.Errorf("secondary: broadcast commit import: %w", err)
	}

	return nil
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
	return dmlt.milvusStorage.Config().Provider == v2.ProviderLocal
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
