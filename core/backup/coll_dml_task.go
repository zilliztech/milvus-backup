package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const _allPartitionID = -1

type collDMLTask struct {
	taskID string

	ns namespace.NS

	milvusStorage  storage.Client
	milvusRootPath string

	crossStorage bool

	backupStorage storage.Client
	backupDir     string

	throttling concurrencyThrottling

	grpc    milvus.Grpc
	restful milvus.Restful

	gcCtrl gcCtrl

	taskMgr     *taskmgr.Mgr
	metaBuilder *metaBuilder

	logger *zap.Logger
}

func newCollDMLTask(ns namespace.NS, args collTaskArgs) *collDMLTask {
	logger := log.L().With(zap.String("task_id", args.TaskID), zap.String("ns", ns.String()))
	return &collDMLTask{
		taskID: args.TaskID,

		ns: ns,

		milvusStorage:  args.MilvusStorage,
		milvusRootPath: args.MilvusRootPath,

		crossStorage: args.CrossStorage,

		backupStorage: args.BackupStorage,
		backupDir:     args.BackupDir,

		throttling: args.Throttling,

		grpc:    args.Grpc,
		restful: args.Restful,

		taskMgr:     args.TaskMgr,
		metaBuilder: args.MetaBuilder,

		gcCtrl: args.gcCtrl,

		logger: logger,
	}
}

func (dmlt *collDMLTask) listDeltaLogByAPI(ctx context.Context, binlogDir string, fieldsBinlog []milvus.BinlogInfo) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, dmlt.milvusStorage, binlogDir, true)
	if err != nil {
		return nil, 0, fmt.Errorf("backup: list insert logs %w", err)
	}
	keySize := make(map[string]int64, len(keys))
	for idx, key := range keys {
		keySize[key] = sizes[idx]
	}

	bakFieldsBinlog := make([]*backuppb.FieldBinlog, 0, len(fieldsBinlog))
	for _, fieldBinlog := range fieldsBinlog {
		logIDs := fieldBinlog.LogIDs
		binlogs := make([]*backuppb.Binlog, 0, len(logIDs))
		for _, logID := range logIDs {
			// delta log path has no field id
			key := mpath.Join(binlogDir, mpath.LogID(logID))
			size, ok := keySize[key]
			if !ok {
				return nil, 0, fmt.Errorf("backup: log %s not exist", key)
			}
			binlog := &backuppb.Binlog{LogPath: key, LogId: logID, LogSize: size}
			binlogs = append(binlogs, binlog)
		}
		bakFieldBinlog := &backuppb.FieldBinlog{FieldID: fieldBinlog.FieldID, Binlogs: binlogs}
		bakFieldsBinlog = append(bakFieldsBinlog, bakFieldBinlog)
	}

	return bakFieldsBinlog, lo.Sum(sizes), nil
}

func (dmlt *collDMLTask) listInsertLogByAPI(ctx context.Context, binlogDir string, fieldsBinlog []milvus.BinlogInfo) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, dmlt.milvusStorage, binlogDir, true)
	if err != nil {
		return nil, 0, fmt.Errorf("backup: list insert logs %w", err)
	}
	keySize := make(map[string]int64, len(keys))
	for idx, key := range keys {
		keySize[key] = sizes[idx]
	}

	bakFieldsBinlog := make([]*backuppb.FieldBinlog, 0, len(fieldsBinlog))
	for _, fieldBinlog := range fieldsBinlog {
		logIDs := fieldBinlog.LogIDs
		binlogs := make([]*backuppb.Binlog, 0, len(logIDs))
		for _, logID := range logIDs {
			key := mpath.Join(binlogDir, mpath.FieldID(fieldBinlog.FieldID), mpath.LogID(logID))
			size, ok := keySize[key]
			if !ok {
				return nil, 0, fmt.Errorf("backup: log %s not exist", key)
			}
			binlog := &backuppb.Binlog{LogPath: key, LogId: logID, LogSize: size}
			binlogs = append(binlogs, binlog)
		}
		bakFieldBinlog := &backuppb.FieldBinlog{FieldID: fieldBinlog.FieldID, Binlogs: binlogs}
		bakFieldsBinlog = append(bakFieldsBinlog, bakFieldBinlog)
	}

	return bakFieldsBinlog, lo.Sum(sizes), nil
}

func (dmlt *collDMLTask) getSegments(ctx context.Context) ([]*backuppb.SegmentBackupInfo, error) {
	dmlt.logger.Info("start get segments of collection")
	segments, err := dmlt.grpc.GetPersistentSegmentInfo(ctx, dmlt.ns.DBName(), dmlt.ns.CollName())
	if err != nil {
		return nil, fmt.Errorf("backup: get persistent segment info %w", err)
	}

	// most of the segments are not l0, so we don't need to set capacity for l0SegIDs
	var l0SegIDs []int64
	notL0SegIDs := make([]int64, 0, len(segments))
	for _, seg := range segments {
		if seg.GetLevel() == commonpb.SegmentLevel_L0 {
			l0SegIDs = append(l0SegIDs, seg.GetSegmentID())
		} else {
			notL0SegIDs = append(notL0SegIDs, seg.GetSegmentID())
		}
	}
	dmlt.logger.Info("segments of collection", zap.Int64s("l0_segments", l0SegIDs), zap.Int64s("not_l0_segments", notL0SegIDs))

	bakSegs := make([]*backuppb.SegmentBackupInfo, 0, len(segments))
	for _, seg := range segments {
		bakSeg, err := dmlt.getSegment(ctx, seg)
		if err != nil {
			return nil, fmt.Errorf("backup: get segment %w", err)
		}
		bakSegs = append(bakSegs, bakSeg)
	}

	return bakSegs, nil
}

func (dmlt *collDMLTask) getSegment(ctx context.Context, seg *milvuspb.PersistentSegmentInfo) (*backuppb.SegmentBackupInfo, error) {
	dmlt.logger.Info("get segment info", zap.Int64("segment_id", seg.SegmentID))

	var bakSeg *backuppb.SegmentBackupInfo
	if dmlt.grpc.HasFeature(milvus.GetSegmentInfo) {
		// will try to get segment info via proxy node first
		var err error
		bakSeg, err = dmlt.getSegmentInfoByAPI(ctx, seg)
		if err == nil {
			return bakSeg, nil
		} else {
			dmlt.logger.Warn("get segment info via proxy node failed, pls check whether milvus restful api is enabled", zap.Error(err))
		}
	}

	var err error
	// if failed, try to get segment info via list file
	bakSeg, err = dmlt.getSegmentInfoByListFile(ctx, seg)
	if err != nil {
		return nil, fmt.Errorf("backup: get segment info %w", err)
	}

	return bakSeg, nil
}

func (dmlt *collDMLTask) listInsertLogByListFile(ctx context.Context, binlogDir string) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, dmlt.milvusStorage, binlogDir, true)
	if err != nil {
		return nil, 0, fmt.Errorf("backup: list insert logs %w", err)
	}

	// group binlogs by field id
	fieldBinlogs := make(map[int64][]*backuppb.Binlog)
	for idx, key := range keys {
		binlog, err := mpath.ParseInsertLogPath(key)
		if err != nil {
			return nil, 0, fmt.Errorf("backup: parse log path %w", err)
		}
		bakBinlog := &backuppb.Binlog{LogId: binlog.LogID, LogSize: sizes[idx], LogPath: key}
		fieldBinlogs[binlog.FieldID] = append(fieldBinlogs[binlog.FieldID], bakBinlog)
	}

	fields := make([]*backuppb.FieldBinlog, 0, len(fieldBinlogs))
	var fileNum int
	for fieldID, binlogs := range fieldBinlogs {
		dmlt.logger.Info("get insert logs done", zap.Int64("field_id", fieldID), zap.Int("count", len(binlogs)))
		if fileNum == 0 {
			fileNum = len(binlogs)
		} else if fileNum != len(binlogs) {
			return nil, 0, fmt.Errorf("backup: field %d has different file num to other fields", fieldID)
		}
		fields = append(fields, &backuppb.FieldBinlog{FieldID: fieldID, Binlogs: binlogs})
	}

	return fields, lo.Sum(sizes), nil
}

func (dmlt *collDMLTask) listDeltaLogByListFile(ctx context.Context, binlogDir string) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, dmlt.milvusStorage, binlogDir, true)
	if err != nil {
		return nil, 0, fmt.Errorf("backup: list delta log %w", err)
	}

	bakBinlogs := make([]*backuppb.Binlog, 0, len(keys))
	for idx, key := range keys {
		binlog, err := mpath.ParseDeltaLogPath(key)
		if err != nil {
			return nil, 0, fmt.Errorf("backup: parse log path %w", err)
		}
		bakBinlog := &backuppb.Binlog{LogId: binlog.LogID, LogSize: sizes[idx], LogPath: key}
		bakBinlogs = append(bakBinlogs, bakBinlog)
	}
	dmlt.logger.Info("get delta logs done", zap.Int("count", len(bakBinlogs)))

	return []*backuppb.FieldBinlog{{Binlogs: bakBinlogs}}, lo.Sum(sizes), nil
}

// getSegmentInfoByListFile if milvus version < 2.5.8, we can only get segment info via list file.
func (dmlt *collDMLTask) getSegmentInfoByListFile(ctx context.Context, seg *milvuspb.PersistentSegmentInfo) (*backuppb.SegmentBackupInfo, error) {
	dmlt.logger.Info("get segment info via list file", zap.Int64("segment_id", seg.SegmentID))

	pathOpts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionID()),
		mpath.PartitionID(seg.GetPartitionID()),
		mpath.SegmentID(seg.GetSegmentID()),
	}

	insertLogDir := mpath.MilvusInsertLogDir(dmlt.milvusRootPath, pathOpts...)
	dmlt.logger.Debug("insert log dir", zap.String("dir", insertLogDir))
	insertLogs, iSize, err := dmlt.listInsertLogByListFile(ctx, insertLogDir)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}
	dmlt.logger.Info("get insert logs done", zap.Int("count", len(insertLogs)))

	deltaLogDir := mpath.MilvusDeltaLogDir(dmlt.milvusRootPath, pathOpts...)
	dmlt.logger.Debug("delta log dir", zap.String("dir", deltaLogDir))
	deltaLogs, dSize, err := dmlt.listDeltaLogByListFile(ctx, deltaLogDir)
	if err != nil {
		return nil, fmt.Errorf("backup: list delta logs %w", err)
	}
	dmlt.logger.Info("get delta logs done", zap.Int("count", len(deltaLogs)))

	return &backuppb.SegmentBackupInfo{
		SegmentId:      seg.GetSegmentID(),
		CollectionId:   seg.GetCollectionID(),
		PartitionId:    seg.GetPartitionID(),
		NumOfRows:      seg.GetNumRows(),
		StorageVersion: seg.GetStorageVersion(),
		GroupId:        dmlt.groupID(seg),
		IsL0:           seg.GetLevel() == commonpb.SegmentLevel_L0,
		Binlogs:        insertLogs,
		Deltalogs:      deltaLogs,
		Size:           iSize + dSize,
	}, nil
}

// groupID generates a virtual partition ID for batch importing multiple segments.
// When the partition ID is -1, it means that the segment applies to all partitions.
// Therefore, it requires special handling and cannot participate in batch import, so groupID returns 0.
func (dmlt *collDMLTask) groupID(seg *milvuspb.PersistentSegmentInfo) int64 {
	if seg.GetPartitionID() == _allPartitionID {
		return 0
	}

	return seg.GetSegmentID()
}

// getSegmentDetailByAPI if milvus version > 2.5.8, we have a new api to get the segment info via proxy node.
// see: https://github.com/milvus-io/milvus/pull/40464
func (dmlt *collDMLTask) getSegmentInfoByAPI(ctx context.Context, seg *milvuspb.PersistentSegmentInfo) (*backuppb.SegmentBackupInfo, error) {
	dmlt.logger.Info("try get segment info via proxy node", zap.Int64("segment_id", seg.SegmentID))
	segInfo, err := dmlt.restful.GetSegmentInfo(ctx, dmlt.ns.DBName(), seg.CollectionID, seg.SegmentID)
	if err != nil {
		return nil, fmt.Errorf("backup: get segment info %w", err)
	}
	pathOpts := []mpath.Option{
		mpath.CollectionID(seg.CollectionID),
		mpath.PartitionID(seg.PartitionID),
		mpath.SegmentID(seg.SegmentID),
	}

	insertLogDir := mpath.MilvusInsertLogDir(dmlt.milvusRootPath, pathOpts...)
	insertLogs, iSize, err := dmlt.listInsertLogByAPI(ctx, insertLogDir, segInfo.InsertLogs)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}

	deltaLogDir := mpath.MilvusDeltaLogDir(dmlt.milvusRootPath, pathOpts...)
	deltaLogs, dSize, err := dmlt.listDeltaLogByAPI(ctx, deltaLogDir, segInfo.DeltaLogs)
	if err != nil {
		return nil, fmt.Errorf("backup: list delta logs %w", err)
	}

	bakSeg := &backuppb.SegmentBackupInfo{
		SegmentId:      seg.GetSegmentID(),
		CollectionId:   seg.GetCollectionID(),
		PartitionId:    seg.GetPartitionID(),
		NumOfRows:      seg.GetNumRows(),
		StorageVersion: seg.GetStorageVersion(),
		Binlogs:        insertLogs,
		Deltalogs:      deltaLogs,
		GroupId:        dmlt.groupID(seg),
		IsL0:           seg.GetLevel() == commonpb.SegmentLevel_L0,
		Size:           iSize + dSize,
		VChannel:       segInfo.VChannel,
	}

	return bakSeg, nil
}

func (dmlt *collDMLTask) Execute(ctx context.Context) error {
	dmlt.logger.Info("start to backup dml of collection")

	dmlt.taskMgr.UpdateBackupTask(dmlt.taskID, taskmgr.SetBackupCollDMLPrepare(dmlt.ns))

	describe, err := dmlt.grpc.DescribeCollection(ctx, dmlt.ns.DBName(), dmlt.ns.CollName())
	if err != nil {
		return fmt.Errorf("backup: describe collection %w", err)
	}
	dmlt.gcCtrl.PauseCollectionGC(ctx, describe.CollectionID)
	defer dmlt.gcCtrl.ResumeCollectionGC(ctx, describe.CollectionID)

	segments, err := dmlt.getSegments(ctx)
	if err != nil {
		return fmt.Errorf("backup: get segments %w", err)
	}

	dmlt.metaBuilder.addSegments(segments)

	size := lo.SumBy(segments, func(seg *backuppb.SegmentBackupInfo) int64 { return seg.GetSize() })
	dmlt.taskMgr.UpdateBackupTask(dmlt.taskID, taskmgr.SetBackupCollDMLExecuting(dmlt.ns, size))

	if err := dmlt.backupSegmentsData(ctx, segments); err != nil {
		return fmt.Errorf("backup: backup segments %w", err)
	}

	dmlt.taskMgr.UpdateBackupTask(dmlt.taskID, taskmgr.SetBackupCollDMLDone(dmlt.ns))

	return nil
}

func (dmlt *collDMLTask) backupSegmentsData(ctx context.Context, segments []*backuppb.SegmentBackupInfo) error {
	dmlt.logger.Info("start to backup segments", zap.Int("segment_num", len(segments)))
	g, subCtx := errgroup.WithContext(ctx)
	for _, seg := range segments {
		if err := dmlt.throttling.SegSem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("backup: acquire segment semaphore %w", err)
		}

		g.Go(func() error {
			defer dmlt.throttling.SegSem.Release(1)

			if err := dmlt.backupSegmentData(subCtx, seg); err != nil {
				return fmt.Errorf("backup: copy segment %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("backup: wait backup segments %w", err)
	}

	dmlt.logger.Info("backup segments done")

	return nil
}

func (dmlt *collDMLTask) insertLogsAttrs(seg *backuppb.SegmentBackupInfo) ([]storage.CopyAttr, error) {
	opts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionId()),
		mpath.PartitionID(seg.GetPartitionId()),
		mpath.SegmentID(seg.GetSegmentId()),
		mpath.GroupID(seg.GetGroupId()),
	}
	destDir := mpath.BackupInsertLogDir(dmlt.backupDir, opts...)

	var attrs []storage.CopyAttr
	for _, field := range seg.GetBinlogs() {
		fieldAttrs := make([]storage.CopyAttr, 0, len(field.GetBinlogs()))
		for _, binlog := range field.GetBinlogs() {
			destKey := mpath.Join(destDir, mpath.FieldID(field.FieldID), mpath.LogID(binlog.LogId))
			if destKey == binlog.LogPath {
				return nil, fmt.Errorf("backup: dest key %s is same as src key %s", destKey, binlog.LogPath)
			}

			srcAttr := storage.ObjectAttr{Key: binlog.LogPath, Length: binlog.LogSize}
			fieldAttrs = append(fieldAttrs, storage.CopyAttr{Src: srcAttr, DestKey: destKey})
		}
		attrs = append(attrs, fieldAttrs...)
	}

	return attrs, nil
}

func (dmlt *collDMLTask) deltaLogAttrs(seg *backuppb.SegmentBackupInfo) ([]storage.CopyAttr, error) {
	opts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionId()),
		mpath.PartitionID(seg.GetPartitionId()),
		mpath.SegmentID(seg.GetSegmentId()),
	}
	if seg.GetPartitionId() != _allPartitionID {
		opts = append(opts, mpath.GroupID(seg.GetGroupId()))
	}
	destDir := mpath.BackupDeltaLogDir(dmlt.backupDir, opts...)

	var attrs []storage.CopyAttr
	for _, field := range seg.GetDeltalogs() {
		fieldAttrs := make([]storage.CopyAttr, 0, len(field.GetBinlogs()))
		for _, binlog := range field.GetBinlogs() {
			destKey := mpath.Join(destDir, mpath.LogID(binlog.LogId))
			if destKey == binlog.LogPath {
				return nil, fmt.Errorf("backup: dest key %s is same as src key %s", destKey, binlog.LogPath)
			}

			srcAttr := storage.ObjectAttr{Key: binlog.LogPath, Length: binlog.LogSize}
			fieldAttrs = append(fieldAttrs, storage.CopyAttr{Src: srcAttr, DestKey: destKey})
		}
		attrs = append(attrs, fieldAttrs...)
	}

	return attrs, nil
}

func (dmlt *collDMLTask) backupSegmentData(ctx context.Context, seg *backuppb.SegmentBackupInfo) error {
	dmlt.logger.Info("backup binlogs of segment", zap.Int64("segment_id", seg.GetSegmentId()))
	insertAttrs, err := dmlt.insertLogsAttrs(seg)
	if err != nil {
		return fmt.Errorf("backup: backup insert logs %w", err)
	}
	if len(insertAttrs) == 0 && !seg.GetIsL0() {
		return fmt.Errorf("backup: segment %d has no insert logs", seg.GetSegmentId())
	}
	deltaAttrs, err := dmlt.deltaLogAttrs(seg)
	if err != nil {
		return fmt.Errorf("backup: backup delta logs %w", err)
	}

	attrs := append(insertAttrs, deltaAttrs...)
	opt := storage.CopyObjectsOpt{
		Src:          dmlt.milvusStorage,
		Dest:         dmlt.backupStorage,
		Attrs:        attrs,
		CopyByServer: dmlt.crossStorage,
		Sem:          dmlt.throttling.CopySem,
		TraceFn: func(size int64, cost time.Duration) {
			dmlt.taskMgr.UpdateBackupTask(dmlt.taskID, taskmgr.IncBackupCollCopiedSize(dmlt.ns, size, cost))
		},
	}
	cpTask := storage.NewCopyObjectsTask(opt)
	if err := cpTask.Execute(ctx); err != nil {
		return fmt.Errorf("backup: copy bin logs %w", err)
	}

	dmlt.logger.Info("backup binlogs of segment done", zap.Int64("segment_id", seg.GetSegmentId()))

	return nil
}
