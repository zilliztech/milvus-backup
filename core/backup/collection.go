package backup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/pbconv"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

const _allPartitionID = -1

type CollectionOpt struct {
	BackupID string

	MetaOnly  bool
	SkipFlush bool

	MilvusStorage  storage.ChunkManager
	MilvusRootPath string
	MilvusBucket   string

	BackupStorage storage.ChunkManager
	BackupPath    string
	BackupBucket  string

	Meta *meta.MetaManager

	Grpc client.Grpc
}

type CollectionTask struct {
	backupID string

	dbName   string
	collName string

	metaOnly  bool
	skipFlush bool

	milvusStorage  storage.ChunkManager
	milvusRootPath string
	milvusBucket   string

	backupStorage storage.ChunkManager
	backupPath    string
	backupBucket  string

	meta *meta.MetaManager
	grpc client.Grpc

	logger *zap.Logger
}

func NewCollectionTask(dbName, collName string, opt CollectionOpt) *CollectionTask {
	return &CollectionTask{
		backupID: opt.BackupID,

		dbName:   dbName,
		collName: collName,

		metaOnly:  opt.MetaOnly,
		skipFlush: opt.SkipFlush,

		milvusStorage:  opt.MilvusStorage,
		milvusRootPath: opt.MilvusRootPath,
		milvusBucket:   opt.MilvusBucket,

		backupStorage: opt.BackupStorage,
		backupPath:    opt.BackupPath,
		backupBucket:  opt.BackupBucket,

		meta: opt.Meta,
		grpc: opt.Grpc,

		logger: log.L().With(zap.String("db_name", dbName), zap.String("collection_name", collName)),
	}

}

func (ct *CollectionTask) Execute(ctx context.Context) error {
	if err := ct.backupDDL(ctx); err != nil {
		return fmt.Errorf("backup: backup ddl %w", err)
	}

	if ct.metaOnly {
		ct.logger.Info("skip backup dml")
		return nil
	}

	if err := ct.backupDML(ctx); err != nil {
		return fmt.Errorf("backup: backup dml %w", err)
	}

	return nil
}

func (ct *CollectionTask) convFields(fields []*schemapb.FieldSchema) ([]*backuppb.FieldSchema, error) {
	bakFields := make([]*backuppb.FieldSchema, 0, len(fields))
	for _, field := range fields {
		var defaultValue string
		if field.GetDefaultValue() != nil {
			bytes, err := proto.Marshal(field.GetDefaultValue())
			if err != nil {
				return nil, fmt.Errorf("backup: marshal default value")
			}
			defaultValue = string(bytes)
		}

		f := &backuppb.FieldSchema{
			FieldID:           field.GetFieldID(),
			Name:              field.GetName(),
			IsPrimaryKey:      field.GetIsPrimaryKey(),
			Description:       field.GetDescription(),
			AutoID:            field.GetAutoID(),
			DataType:          backuppb.DataType(field.GetDataType()),
			TypeParams:        pbconv.MilvusKVToBakKV(field.GetTypeParams()),
			IndexParams:       pbconv.MilvusKVToBakKV(field.GetIndexParams()),
			IsDynamic:         field.GetIsDynamic(),
			IsPartitionKey:    field.GetIsPartitionKey(),
			Nullable:          field.GetNullable(),
			ElementType:       backuppb.DataType(field.GetElementType()),
			IsFunctionOutput:  field.GetIsFunctionOutput(),
			DefaultValueProto: defaultValue,
		}
		bakFields = append(bakFields, f)
	}

	return bakFields, nil
}

func (ct *CollectionTask) convFunctions(funs []*schemapb.FunctionSchema) []*backuppb.FunctionSchema {
	bakFuns := make([]*backuppb.FunctionSchema, 0, len(funs))
	for _, function := range funs {
		functionBak := &backuppb.FunctionSchema{
			Name:             function.GetName(),
			Id:               function.GetId(),
			Description:      function.GetDescription(),
			Type:             backuppb.FunctionType(function.GetType()),
			InputFieldNames:  function.GetInputFieldNames(),
			InputFieldIds:    function.GetInputFieldIds(),
			OutputFieldNames: function.GetOutputFieldNames(),
			OutputFieldIds:   function.GetOutputFieldIds(),
			Params:           pbconv.MilvusKVToBakKV(function.GetParams()),
		}
		bakFuns = append(bakFuns, functionBak)
	}

	return bakFuns
}

func (ct *CollectionTask) convSchema(schema *schemapb.CollectionSchema) (*backuppb.CollectionSchema, error) {
	fields, err := ct.convFields(schema.Fields)
	if err != nil {
		return nil, fmt.Errorf("backup: convert fields %w", err)
	}
	functions := ct.convFunctions(schema.Functions)
	bakSchema := &backuppb.CollectionSchema{
		Name:               schema.GetName(),
		Description:        schema.GetDescription(),
		AutoID:             schema.GetAutoID(),
		Fields:             fields,
		EnableDynamicField: schema.GetEnableDynamicField(),
		Functions:          functions,
	}

	return bakSchema, nil
}

func (ct *CollectionTask) getPartLoadState(ctx context.Context, collLoadState string, partitionNames []string) (map[string]string, error) {
	partLoadState := make(map[string]string, len(partitionNames))
	// if the collection is loaded or not loaded, means all partitions are loaded or not loaded
	if collLoadState == meta.LoadState_Loaded || collLoadState == meta.LoadState_NotLoad {
		for _, partitionName := range partitionNames {
			partLoadState[partitionName] = collLoadState
		}

		return partLoadState, nil
	}

	for _, partName := range partitionNames {
		progress, err := ct.grpc.GetLoadingProgress(ctx, ct.dbName, partName)
		if err != nil {
			return nil, fmt.Errorf("backup: get loading progress %w", err)
		}

		if progress == 0 {
			partLoadState[partName] = meta.LoadState_NotLoad
		} else if progress == 100 {
			partLoadState[partName] = meta.LoadState_Loaded
		} else {
			partLoadState[partName] = meta.LoadState_Loading
		}
	}

	return partLoadState, nil
}

func (ct *CollectionTask) getCollLoadState(ctx context.Context) (string, error) {
	progress, err := ct.grpc.GetLoadingProgress(ctx, ct.dbName, ct.collName)
	if err != nil {
		return "", fmt.Errorf("backup: get loading progress %w", err)
	}

	if progress == 0 {
		return meta.LoadState_NotLoad, nil
	}
	if progress == 100 {
		return meta.LoadState_Loaded, nil
	}

	return meta.LoadState_Loading, nil
}

func (ct *CollectionTask) backupPartitionDDL(ctx context.Context, collID int64, collLoadState string) ([]*backuppb.PartitionBackupInfo, error) {
	ct.logger.Info("start backup partition ddl of collection")

	resp, err := ct.grpc.ShowPartitions(context.Background(), ct.dbName, ct.collName)
	if err != nil {
		return nil, fmt.Errorf("backup: show partitions %w", err)
	}
	ct.logger.Info("partitions of collection", zap.Strings("partitions", resp.PartitionNames))

	nameLen := len(resp.PartitionNames)
	idLen := len(resp.PartitionIDs)
	if nameLen != idLen {
		return nil, fmt.Errorf("backup: partition ids len = %d and names len = %d len not match", idLen, nameLen)
	}

	loadState, err := ct.getPartLoadState(ctx, collLoadState, resp.PartitionNames)
	if err != nil {
		return nil, fmt.Errorf("backup: get partition load state %w", err)
	}
	bakPartitions := make([]*backuppb.PartitionBackupInfo, 0, nameLen)
	for idx, id := range resp.PartitionIDs {
		bakPartition := &backuppb.PartitionBackupInfo{
			PartitionId:   id,
			PartitionName: resp.PartitionNames[idx],
			CollectionId:  collID,
			LoadState:     loadState[resp.PartitionNames[idx]],
		}
		bakPartitions = append(bakPartitions, bakPartition)
	}

	return bakPartitions, nil
}

func (ct *CollectionTask) backupIndexes(ctx context.Context) ([]*backuppb.IndexInfo, error) {
	ct.logger.Info("start backup indexes of collection")
	indexes, err := ct.grpc.ListIndex(ctx, ct.dbName, ct.collName)
	if err != nil && !strings.Contains(err.Error(), "index not found") {
		return nil, fmt.Errorf("backup: list index %w", err)
	}
	ct.logger.Info("indexes of collection", zap.Any("indexes", indexes))

	bakIndexes := make([]*backuppb.IndexInfo, 0, len(indexes))
	for _, index := range indexes {
		params := pbconv.MilvusKVToMap(index.GetParams())
		bakIndex := &backuppb.IndexInfo{
			FieldName: index.GetFieldName(),
			IndexName: index.GetIndexName(),
			IndexType: params["index_type"],
			Params:    params,
		}
		bakIndexes = append(bakIndexes, bakIndex)
	}

	return bakIndexes, nil
}

// backupDDL collects the collection DDL info, including schema, index and partition info.
// The segment info is not collected here, will be collect later in collectDMLInfo
func (ct *CollectionTask) backupDDL(ctx context.Context) error {
	ct.logger.Info("start to backup ddl of collection")

	descResp, err := ct.grpc.DescribeCollection(ctx, ct.dbName, ct.collName)
	if err != nil {
		return fmt.Errorf("backup: describe collection %w", err)
	}
	schema, err := ct.convSchema(descResp.GetSchema())
	if err != nil {
		return fmt.Errorf("backup: convert schema %w", err)
	}
	indexes, err := ct.backupIndexes(ctx)
	if err != nil {
		return fmt.Errorf("backup: backup indexes %w", err)
	}
	collLoadState, err := ct.getCollLoadState(ctx)
	if err != nil {
		return fmt.Errorf("backup: get collection load state %w", err)
	}
	partitions, err := ct.backupPartitionDDL(ctx, descResp.CollectionID, collLoadState)
	if err != nil {
		return fmt.Errorf("backup: backup partition ddl %w", err)
	}

	backupInfo := &backuppb.CollectionBackupInfo{
		Id:               ct.backupID,
		StateCode:        backuppb.BackupTaskStateCode_BACKUP_INITIAL,
		StartTime:        time.Now().Unix(),
		CollectionId:     descResp.GetCollectionID(),
		DbName:           descResp.GetDbName(),
		CollectionName:   descResp.GetCollectionName(),
		Schema:           schema,
		ShardsNum:        descResp.GetShardsNum(),
		ConsistencyLevel: backuppb.ConsistencyLevel(descResp.ConsistencyLevel),
		HasIndex:         len(indexes) > 0,
		IndexInfos:       indexes,
		LoadState:        collLoadState,
		PartitionBackups: partitions,
	}
	ct.meta.AddCollection(backupInfo)

	return nil
}

func (ct *CollectionTask) flushCollection(ctx context.Context) error {
	if ct.skipFlush {
		ct.logger.Info("skip flush collection")
		return nil
	}

	ct.logger.Info("start to flush collection")
	start := time.Now()
	err := retry.Do(ctx, func() error {
		resp, err := ct.grpc.Flush(ctx, ct.dbName, ct.collName)
		if err != nil {
			return fmt.Errorf("backup: flush collection %w", err)
		}
		ct.logger.Info("flush collection done", zap.Any("resp", resp), zap.Duration("cost", time.Since(start)))

		return nil
	})
	if err != nil {
		return fmt.Errorf("backup: flush collection after retry %w", err)
	}

	return nil
}

func (ct *CollectionTask) getSegment(ctx context.Context, seg *milvuspb.PersistentSegmentInfo) (*backuppb.SegmentBackupInfo, error) {
	ct.logger.Info("get segment info", zap.Int64("segment_id", seg.SegmentID))

	// if milvus version >= 2.5.8, so try to get segment info via proxy node first
	bakSeg, err := ct.getSegmentInfoByAPI(ctx, seg)
	if err == nil {
		return bakSeg, nil
	} else {
		ct.logger.Info("get segment info via proxy node failed, will use list file", zap.Error(err))
	}

	// if failed, try to get segment info via list file
	bakSeg, err = ct.getSegmentInfoByListFile(ctx, seg)
	if err != nil {
		return nil, fmt.Errorf("backup: get segment info %w", err)
	}

	return bakSeg, nil
}

func (ct *CollectionTask) listBinlogByAPI(ctx context.Context, binlogDir string, fieldsBinlog []*internalpb.FieldBinlog) ([]*backuppb.FieldBinlog, error) {
	keys, sizes, err := ct.milvusStorage.ListWithPrefix(ctx, ct.milvusBucket, binlogDir, true)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}
	keySize := make(map[string]int64, len(keys))
	for idx, key := range keys {
		keySize[key] = sizes[idx]
	}

	bakFieldsBinlog := make([]*backuppb.FieldBinlog, 0, len(fieldsBinlog))
	for _, fieldBinlog := range fieldsBinlog {
		logIDs := fieldBinlog.GetLogIDs()
		binlogs := make([]*backuppb.Binlog, 0, len(logIDs))
		for _, logID := range logIDs {
			key := mpath.Join(binlogDir, mpath.FieldID(fieldBinlog.GetFieldID()), mpath.LogID(logID))
			size, ok := keySize[key]
			if !ok {
				return nil, fmt.Errorf("backup: log %s not exist", key)
			}
			binlog := &backuppb.Binlog{LogPath: key, LogId: logID, LogSize: size}
			binlogs = append(binlogs, binlog)
		}
		bakFieldBinlog := &backuppb.FieldBinlog{FieldID: fieldBinlog.GetFieldID(), Binlogs: binlogs}
		bakFieldsBinlog = append(bakFieldsBinlog, bakFieldBinlog)
	}

	return bakFieldsBinlog, nil
}

func (ct *CollectionTask) listInsertLogByListFile(ctx context.Context, binlogDir string) ([]*backuppb.FieldBinlog, error) {
	keys, sizes, err := ct.milvusStorage.ListWithPrefix(ctx, ct.milvusBucket, binlogDir, true)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}

	// group binlogs by field id
	fieldBinlogs := make(map[int64][]*backuppb.Binlog)
	for idx, key := range keys {
		binlog, err := mpath.ParseInsertLogPath(key)
		if err != nil {
			return nil, fmt.Errorf("backup: parse log path %w", err)
		}
		bakBinlog := &backuppb.Binlog{LogId: binlog.LogID, LogSize: sizes[idx], LogPath: key}
		if fieldBinlog, ok := fieldBinlogs[binlog.FieldID]; ok {
			fieldBinlog = append(fieldBinlog, bakBinlog)
		} else {
			fieldBinlogs[binlog.FieldID] = []*backuppb.Binlog{bakBinlog}
		}
	}

	fields := make([]*backuppb.FieldBinlog, 0, len(fieldBinlogs))
	for fieldID, binlogs := range fieldBinlogs {
		fields = append(fields, &backuppb.FieldBinlog{FieldID: fieldID, Binlogs: binlogs})
	}

	return fields, nil
}

func (ct *CollectionTask) listDeltaLogByListFile(ctx context.Context, binlogDir string) ([]*backuppb.FieldBinlog, error) {
	keys, sizes, err := ct.milvusStorage.ListWithPrefix(ctx, ct.milvusBucket, binlogDir, true)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}

	bakBinlogs := make([]*backuppb.Binlog, 0, len(keys))
	for idx, key := range keys {
		binlog, err := mpath.ParseDeltaLogPath(key)
		if err != nil {
			return nil, fmt.Errorf("backup: parse log path %w", err)
		}
		bakBinlog := &backuppb.Binlog{LogId: binlog.LogID, LogSize: sizes[idx], LogPath: key}
		bakBinlogs = append(bakBinlogs, bakBinlog)
	}

	return []*backuppb.FieldBinlog{{Binlogs: bakBinlogs}}, nil
}

// getSegmentInfoByListFile if milvus version < 2.5.8, we can only get segment info via list file.
func (ct *CollectionTask) getSegmentInfoByListFile(ctx context.Context, seg *milvuspb.PersistentSegmentInfo) (*backuppb.SegmentBackupInfo, error) {
	ct.logger.Info("get segment info via list file", zap.Int64("segment_id", seg.SegmentID))

	pathOpts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionID()),
		mpath.PartitionID(seg.GetPartitionID()),
		mpath.SegmentID(seg.GetSegmentID()),
	}

	insertLogDir := mpath.MilvusInsertLogDir(ct.milvusRootPath, pathOpts...)
	insertLogs, err := ct.listInsertLogByListFile(ctx, insertLogDir)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}

	deltaLogDir := mpath.MilvusDeltaLogDir(ct.milvusRootPath, pathOpts...)
	deltaLogs, err := ct.listDeltaLogByListFile(ctx, deltaLogDir)
	if err != nil {
		return nil, fmt.Errorf("backup: list delta logs %w", err)
	}

	return &backuppb.SegmentBackupInfo{
		SegmentId:    seg.GetSegmentID(),
		CollectionId: seg.GetCollectionID(),
		PartitionId:  seg.GetPartitionID(),
		NumOfRows:    seg.GetNumRows(),
		GroupId:      ct.groupID(seg),
		IsL0:         seg.GetLevel() == commonpb.SegmentLevel_L0,
		Binlogs:      insertLogs,
		Deltalogs:    deltaLogs,
	}, nil
}

// groupID generates a virtual partition ID for batch importing multiple segments.
// When the partition ID is -1, it means that the segment applies to all partitions.
// Therefore, it requires special handling and cannot participate in batch import, so groupID returns 0.
func (ct *CollectionTask) groupID(seg *milvuspb.PersistentSegmentInfo) int64 {
	if seg.GetPartitionID() == _allPartitionID {
		return 0
	}

	return seg.GetSegmentID()
}

// getSegmentDetailByAPI if milvus version > 2.5.8, we have a new api to get the segment info via proxy node.
// see: https://github.com/milvus-io/milvus/pull/40464
func (ct *CollectionTask) getSegmentInfoByAPI(ctx context.Context, seg *milvuspb.PersistentSegmentInfo) (*backuppb.SegmentBackupInfo, error) {
	ct.logger.Info("try get segment info via proxy node", zap.Int64("segment_id", seg.SegmentID))
	resp, err := ct.grpc.GetSegmentInfo(ctx, ct.dbName, seg.CollectionID, []int64{seg.SegmentID})
	if err != nil {
		return nil, fmt.Errorf("backup: get segment info %w", err)
	}
	// currently, we get segment info one by one, so the resp.SegmentInfos only has one element
	segInfo := resp[0]
	pathOpts := []mpath.Option{
		mpath.CollectionID(seg.CollectionID),
		mpath.PartitionID(seg.PartitionID),
		mpath.SegmentID(seg.SegmentID),
	}

	insertLogDir := mpath.MilvusInsertLogDir(ct.milvusRootPath, pathOpts...)
	insertLogs, err := ct.listBinlogByAPI(ctx, insertLogDir, segInfo.GetInsertLogs())
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}

	deltaLogDir := mpath.MilvusDeltaLogDir(ct.milvusRootPath, pathOpts...)
	deltaLogs, err := ct.listBinlogByAPI(ctx, deltaLogDir, segInfo.GetDeltaLogs())
	if err != nil {
		return nil, fmt.Errorf("backup: list delta logs %w", err)
	}

	bakSeg := &backuppb.SegmentBackupInfo{
		SegmentId:    seg.GetSegmentID(),
		CollectionId: seg.GetCollectionID(),
		PartitionId:  seg.GetPartitionID(),
		NumOfRows:    seg.GetNumRows(),
		Binlogs:      insertLogs,
		Deltalogs:    deltaLogs,
		GroupId:      ct.groupID(seg),
		IsL0:         seg.GetLevel() == commonpb.SegmentLevel_L0,
		VChannel:     segInfo.GetVChannel(),
	}

	return bakSeg, nil
}

func (ct *CollectionTask) getSegments(ctx context.Context) ([]*backuppb.SegmentBackupInfo, error) {
	ct.logger.Info("start get segments of collection")
	segments, err := ct.grpc.GetPersistentSegmentInfo(ctx, ct.dbName, ct.collName)
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
	ct.logger.Info("segments of collection", zap.Int64s("l0_segments", l0SegIDs), zap.Int64s("not_l0_segments", notL0SegIDs))

	bakSegs := make([]*backuppb.SegmentBackupInfo, 0, len(segments))
	for _, seg := range segments {
		bakSeg, err := ct.getSegment(ctx, seg)
		if err != nil {
			return nil, fmt.Errorf("backup: get segment %w", err)
		}
		bakSegs = append(bakSegs, bakSeg)
	}

	return bakSegs, nil
}

func (ct *CollectionTask) backupDML(ctx context.Context) error {
	ct.logger.Info("start to backup dml of collection")
	if err := ct.flushCollection(ctx); err != nil {
		return fmt.Errorf("backup: backup dml %w", err)
	}

	segments, err := ct.getSegments(ctx)
	if err != nil {
		return fmt.Errorf("backup: get segments %w", err)
	}

	if err := ct.backupSegments(ctx, segments); err != nil {
		return fmt.Errorf("backup: backup segments %w", err)
	}

	return nil
}

func (ct *CollectionTask) backupSegments(ctx context.Context, segments []*backuppb.SegmentBackupInfo) error {
	ct.logger.Info("start to backup segments")
	for _, seg := range segments {
		if err := ct.backupSegment(ctx, seg); err != nil {
			return fmt.Errorf("backup: copy segment %w", err)
		}
	}

	return nil
}

func (ct *CollectionTask) backupInsertLogs(ctx context.Context, seg *backuppb.SegmentBackupInfo) error {
	opts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionId()),
		mpath.PartitionID(seg.GetPartitionId()),
		mpath.SegmentID(seg.GetSegmentId()),
		mpath.GroupID(seg.GetGroupId()),
	}
	destDir := mpath.BackupInsertLogDir(ct.backupPath, opts...)

	var attrs []storage.CopyAttr
	for _, field := range seg.GetBinlogs() {
		fieldAttrs := make([]storage.CopyAttr, 0, len(field.GetBinlogs()))
		for _, binlog := range field.GetBinlogs() {
			destKey := mpath.Join(destDir, mpath.FieldID(field.FieldID), mpath.LogID(binlog.LogId))
			if destKey == binlog.LogPath {
				return fmt.Errorf("backup: dest key %s is same as src key %s", destKey, binlog.LogPath)
			}

			srcAttr := storage.ObjectAttr{Key: binlog.LogPath, Length: binlog.LogSize}
			fieldAttrs = append(fieldAttrs, storage.CopyAttr{Src: srcAttr, DestKey: destKey})
		}
		attrs = append(attrs, fieldAttrs...)
	}

	copier := storage.NewCopier(ct.backupStorage, ct.milvusStorage, storage.CopyOption{})
	if err := copier.CopyObjects(ctx, ct.milvusBucket, ct.backupBucket, attrs); err != nil {
		return fmt.Errorf("backup: copy insert logs %w", err)
	}

	return nil
}

func (ct *CollectionTask) backupDeltaLogs(ctx context.Context, seg *backuppb.SegmentBackupInfo) error {
	opts := []mpath.Option{
		mpath.CollectionID(seg.GetCollectionId()),
		mpath.PartitionID(seg.GetPartitionId()),
		mpath.SegmentID(seg.GetSegmentId()),
	}
	if seg.GetPartitionId() != _allPartitionID {
		opts = append(opts, mpath.GroupID(seg.GetGroupId()))
	}
	destDir := mpath.BackupDeltaLogDir(ct.backupPath, opts...)

	var attrs []storage.CopyAttr
	for _, field := range seg.GetDeltalogs() {
		fieldAttrs := make([]storage.CopyAttr, 0, len(field.GetBinlogs()))
		for _, binlog := range field.GetBinlogs() {
			destKey := mpath.Join(destDir, mpath.LogID(binlog.LogId))
			if destKey == binlog.LogPath {
				return fmt.Errorf("backup: dest key %s is same as src key %s", destKey, binlog.LogPath)
			}

			srcAttr := storage.ObjectAttr{Key: binlog.LogPath, Length: binlog.LogSize}
			fieldAttrs = append(fieldAttrs, storage.CopyAttr{Src: srcAttr, DestKey: destKey})
		}
		attrs = append(attrs, fieldAttrs...)
	}

	copier := storage.NewCopier(ct.backupStorage, ct.milvusStorage, storage.CopyOption{})
	if err := copier.CopyObjects(ctx, ct.milvusBucket, ct.backupBucket, attrs); err != nil {
		return fmt.Errorf("backup: copy delta logs %w", err)
	}

	return nil
}

func (ct *CollectionTask) backupSegment(ctx context.Context, seg *backuppb.SegmentBackupInfo) error {
	ct.logger.Info("backup segment", zap.Int64("segment_id", seg.GetSegmentId()))
	if err := ct.backupInsertLogs(ctx, seg); err != nil {
		return fmt.Errorf("backup: backup insert logs %w", err)
	}
	if err := ct.backupDeltaLogs(ctx, seg); err != nil {
		return fmt.Errorf("backup: backup delta logs %w", err)
	}

	return nil
}
