package secondary

import (
	"context"
	"errors"
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
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
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

type binlogDMLTaskArgs struct {
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

type collBinlogDMLTask struct {
	saved            *restoreplan.SecondaryCollection
	progress         *jobstate.SecondaryCollectionProgress
	streamCheckpoint *milvus.StreamCheckpoint
	taskID           string
	enableResume     bool

	backupStorage storage.Client
	backupDir     string

	milvusStorage  storage.Client
	milvusRootPath string
	streaming      bool
	copySem        *semaphore.Weighted

	pchTS      map[string]uint64
	collBackup *backuppb.CollectionBackupInfo

	streamCli  milvus.Stream
	restfulCli milvus.Restful

	logger *zap.Logger
}

func newCollBinlogDMLTask(args binlogDMLTaskArgs, collBackup *backuppb.CollectionBackupInfo) *collBinlogDMLTask {
	collRef := collref.New(collBackup.GetDbName(), collBackup.GetCollectionName())

	return &collBinlogDMLTask{
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

func (dmlt *collBinlogDMLTask) Execute(ctx context.Context) error {
	dmlt.logger.Info("start restore collection dml")
	if dmlt.saved == nil || dmlt.progress == nil {
		return fmt.Errorf("secondary: missing binlog plan or progress for collection %d", dmlt.collBackup.GetCollectionId())
	}
	for phase := range dmlt.saved.Phases {
		if err := dmlt.runBatches(ctx, phase); err != nil {
			if !dmlt.enableResume {
				if cleanupErr := dmlt.cleanTempDirs(context.WithoutCancel(ctx)); cleanupErr != nil {
					dmlt.logger.Warn("clean staged binlogs failed", zap.Error(cleanupErr))
				}
			}
			return err
		}
	}
	return dmlt.cleanTempDirs(context.WithoutCancel(ctx))
}

func (dmlt *collBinlogDMLTask) plan(ctx context.Context) (*restoreplan.SecondaryCollection, error) {
	plan := &restoreplan.SecondaryCollection{StageBinlogs: dmlt.needStaging()}
	for _, part := range dmlt.collBackup.GetPartitionBackups() {
		nonL0 := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool { return !seg.GetIsL0() })
		batches, err := dmlt.nonL0SegBatches(ctx, nonL0)
		if err != nil {
			return nil, err
		}
		plan.Phases[0] = append(plan.Phases[0], planImports(part.GetPartitionId(), batches)...)
		l0 := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool { return seg.GetIsL0() })
		batches, err = dmlt.l0SegBatches(l0)
		if err != nil {
			return nil, err
		}
		plan.Phases[1] = append(plan.Phases[1], planImports(part.GetPartitionId(), batches)...)
	}
	batches, err := dmlt.l0SegBatches(dmlt.collBackup.GetL0Segments())
	if err != nil {
		return nil, err
	}
	plan.Phases[2] = planImports(common.AllPartitionsID, batches)
	return plan, nil
}

func planImports(partitionID int64, batches []batch) []restoreplan.SecondaryImport {
	out := make([]restoreplan.SecondaryImport, 0, len(batches))
	for _, b := range batches {
		input := restoreplan.BinlogImportBatch{L0: b.isL0, Timestamp: b.timestamp, StorageVersion: b.storageVersion}
		for _, dir := range b.partitionDirs {
			input.Dirs = append(input.Dirs, restoreplan.Directory{Insert: dir.insertLogDir, Delta: dir.deltaLogDir, Size: dir.size})
		}
		out = append(out, restoreplan.SecondaryImport{PartitionID: partitionID, Batch: input})
	}
	return out
}

func (dmlt *collBinlogDMLTask) runBatches(ctx context.Context, phase int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, subCtx := errgroup.WithContext(ctx)
	_, err := dmlt.sendBatches(subCtx, g, phase)
	if err != nil {
		cancel()
	}
	waitErr := g.Wait()
	return errors.Join(err, waitErr)
}

func (dmlt *collBinlogDMLTask) backupTS(vch string) (uint64, error) {
	pch := funcutil.ToPhysicalChannel(vch)

	ts, ok := dmlt.pchTS[pch]
	if !ok {
		return 0, fmt.Errorf("restore: no flush all ts for pch %s", pch)
	}

	return ts, nil
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
func (dmlt *collBinlogDMLTask) sendBatches(ctx context.Context, g *errgroup.Group, phase int) (int, error) {
	imports := dmlt.saved.Phases[phase]
	for i := range imports {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		op := &dmlt.progress.BinlogImports[phase][i]
		job := dmlt.importJob()
		job.submit = func(ctx context.Context, id int64) error {
			return dmlt.submitBatch(ctx, id, phase, i)
		}
		state, err := job.start(ctx, op)
		if err != nil {
			return i, err
		}
		g.Go(func() error { return job.wait(ctx, op, state) })
	}
	return len(imports), nil
}

func (dmlt *collBinlogDMLTask) submitBatch(ctx context.Context, id int64, phase, i int) error {
	input := dmlt.saved.Phases[phase][i]
	b := batch{timestamp: input.Batch.Timestamp, isL0: input.Batch.L0, storageVersion: input.Batch.StorageVersion}
	for _, d := range input.Batch.Dirs {
		b.partitionDirs = append(b.partitionDirs, partitionDir{insertLogDir: d.Insert, deltaLogDir: d.Delta, size: d.Size})
	}
	if dmlt.saved.StageBinlogs {
		if dmlt.progress.StagingDir == "" {
			// Register before copying so Cleanup can remove partial files.
			dmlt.progress.StagingDir = jobstate.SecondaryStagingDir(dmlt.taskID, dmlt.collBackup.GetDbName(), dmlt.collBackup.GetCollectionName())
		}
		staged, err := dmlt.stageBatch(ctx, b, dmlt.progress.StagingDir)
		if err != nil {
			return fmt.Errorf("secondary: stage binlogs into milvus storage: %w", err)
		}
		b = staged
	}
	return dmlt.sendImportMsg(ctx, id, input.PartitionID, b)
}

// importJob is the lifecycle view of the import jobs this collection submits,
// read off the task's fields at call time so a client swapped in later is
// honored.
func (dmlt *collBinlogDMLTask) importJob() bulkInsertJob {
	return bulkInsertJob{
		streamCheckpoint: dmlt.streamCheckpoint,
		dbName:           dmlt.collBackup.GetDbName(),
		collectionID:     dmlt.collBackup.GetCollectionId(),
		vchannels:        dmlt.collBackup.GetVirtualChannelNames(),

		streamCli:  dmlt.streamCli,
		restfulCli: dmlt.restfulCli,

		logger: dmlt.logger,
	}
}

func (dmlt *collBinlogDMLTask) nonL0SegBatches(ctx context.Context, segs []*backuppb.SegmentBackupInfo) ([]batch, error) {
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

func (dmlt *collBinlogDMLTask) l0SegBatches(l0Segs []*backuppb.SegmentBackupInfo) ([]batch, error) {
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

func (dmlt *collBinlogDMLTask) buildImportFiles(b batch) []*msgpb.ImportFile {
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

func (dmlt *collBinlogDMLTask) sendImportMsg(ctx context.Context, jobID, partitionID int64, b batch) error {
	schema, err := conv.Schema(dmlt.collBackup.GetSchema())
	if err != nil {
		return fmt.Errorf("secondary: convert schema: %w", err)
	}
	if err := checkDynamicField(schema); err != nil {
		return err
	}

	err = dmlt.streamCli.Send(ctx, importKey(jobID), func(ts uint64) []message.MutableMessage {
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
		return fmt.Errorf("secondary: broadcast import: %w", err)
	}

	return nil
}

func (dmlt *collBinlogDMLTask) buildBackupPartitionDir(ctx context.Context, size int64, pathOpt ...mpath.Option) (partitionDir, error) {
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
func (dmlt *collBinlogDMLTask) needStaging() bool {
	if dmlt.milvusStorage == nil {
		return false
	}
	isSameBucket := dmlt.milvusStorage.Config().Bucket == dmlt.backupStorage.Config().Bucket
	isSameStorage := dmlt.milvusStorage.Config().Provider == dmlt.backupStorage.Config().Provider
	return !isSameBucket || !isSameStorage || dmlt.streaming
}

func (dmlt *collBinlogDMLTask) isLocal() bool {
	return dmlt.milvusStorage.Config().Provider == cfg.ProviderLocal
}

// destKey maps a bucket-relative key onto the key the milvus storage client
// reads and writes; only the local provider needs the root path prefixed.
func (dmlt *collBinlogDMLTask) destKey(key string) string {
	if !dmlt.isLocal() || key == "" {
		return key
	}
	return strings.TrimSuffix(dmlt.milvusRootPath, "/") + "/" + key
}

// stageBatch copies every partition dir of b from the backup storage into a
// temporary prefix in the milvus storage and rewrites the batch to point at the
// copies under the collection staging directory.
func (dmlt *collBinlogDMLTask) stageBatch(ctx context.Context, b batch, tempDir string) (batch, error) {
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

	return b, nil
}

func (dmlt *collBinlogDMLTask) copyToMilvusStorage(ctx context.Context, tempDir, srcPrefix string) (string, error) {
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

func (dmlt *collBinlogDMLTask) cleanTempDirs(ctx context.Context) error {
	if dmlt.progress.StagingDir == "" {
		return nil
	}
	if err := storage.DeletePrefix(ctx, dmlt.milvusStorage, dmlt.destKey(dmlt.progress.StagingDir)); err != nil {
		return fmt.Errorf("secondary: delete staged binlogs %s: %w", dmlt.progress.StagingDir, err)
	}
	dmlt.progress.StagingDir = ""
	return nil
}
