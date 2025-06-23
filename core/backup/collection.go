package backup

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/pbconv"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
)

const _allPartitionID = -1

type CollectionOpt struct {
	BackupID string

	MetaOnly  bool
	SkipFlush bool

	MilvusStorage  storage.Client
	MilvusRootPath string

	BackupStorage storage.Client
	BackupDir     string

	CopySem      *semaphore.Weighted
	CrossStorage bool

	Meta *meta.MetaManager

	Grpc    milvus.Grpc
	Restful milvus.Restful
}

type CollectionTask struct {
	backupID string

	dbName   string
	collName string

	collID int64

	metaOnly  bool
	skipFlush bool

	milvusStorage  storage.Client
	milvusRootPath string

	backupStorage storage.Client
	backupDir     string

	crossStorage bool
	copySem      *semaphore.Weighted

	meta *meta.MetaManager

	grpc    milvus.Grpc
	restful milvus.Restful

	logger *zap.Logger
}

func NewCollectionTask(dbName, collName string, opt CollectionOpt) *CollectionTask {
	logger := log.L().With(
		zap.String("db_name", dbName),
		zap.String("collection_name", collName),
		zap.String("backup_id", opt.BackupID))
	return &CollectionTask{
		backupID: opt.BackupID,

		dbName:   dbName,
		collName: collName,

		metaOnly:  opt.MetaOnly,
		skipFlush: opt.SkipFlush,

		milvusStorage:  opt.MilvusStorage,
		milvusRootPath: opt.MilvusRootPath,

		crossStorage: opt.CrossStorage,
		copySem:      opt.CopySem,

		backupStorage: opt.BackupStorage,
		backupDir:     opt.BackupDir,

		meta: opt.Meta,

		grpc:    opt.Grpc,
		restful: opt.Restful,

		logger: logger,
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
		// We should use Base64 to serialize the proto array.
		// To maintain compatibility with the previous implementation error,
		// we will continue to support direct string encoding.
		// TODO: remove defaultValueProto in the future.
		var defaultValueProto string
		var defaultValueBase64 string
		if field.GetDefaultValue() != nil {
			bytes, err := proto.Marshal(field.GetDefaultValue())
			if err != nil {
				return nil, fmt.Errorf("backup: marshal default value")
			}
			defaultValueProto = string(bytes)
			defaultValueBase64 = base64.StdEncoding.EncodeToString(bytes)
		}

		f := &backuppb.FieldSchema{
			FieldID:            field.GetFieldID(),
			Name:               field.GetName(),
			IsPrimaryKey:       field.GetIsPrimaryKey(),
			Description:        field.GetDescription(),
			AutoID:             field.GetAutoID(),
			DataType:           backuppb.DataType(field.GetDataType()),
			TypeParams:         pbconv.MilvusKVToBakKV(field.GetTypeParams()),
			IndexParams:        pbconv.MilvusKVToBakKV(field.GetIndexParams()),
			IsDynamic:          field.GetIsDynamic(),
			IsPartitionKey:     field.GetIsPartitionKey(),
			Nullable:           field.GetNullable(),
			ElementType:        backuppb.DataType(field.GetElementType()),
			IsFunctionOutput:   field.GetIsFunctionOutput(),
			DefaultValueProto:  defaultValueProto,
			DefaultValueBase64: defaultValueBase64,
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
	ct.logger.Info("collection fields", zap.Any("fields", fields))

	functions := ct.convFunctions(schema.Functions)
	ct.logger.Info("collection functions", zap.Any("functions", functions))

	bakSchema := &backuppb.CollectionSchema{
		Name:               schema.GetName(),
		Description:        schema.GetDescription(),
		AutoID:             schema.GetAutoID(),
		Fields:             fields,
		Properties:         pbconv.MilvusKVToBakKV(schema.GetProperties()),
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

	nameLen := len(resp.GetPartitionNames())
	idLen := len(resp.GetPartitionIDs())
	if nameLen != idLen {
		return nil, fmt.Errorf("backup: partition ids len = %d and names len = %d len not match", idLen, nameLen)
	}

	loadState, err := ct.getPartLoadState(ctx, collLoadState, resp.GetPartitionNames())
	if err != nil {
		return nil, fmt.Errorf("backup: get partition load state %w", err)
	}
	bakPartitions := make([]*backuppb.PartitionBackupInfo, 0, nameLen)
	for idx, id := range resp.GetPartitionIDs() {
		bakPartition := &backuppb.PartitionBackupInfo{
			PartitionId:   id,
			PartitionName: resp.GetPartitionNames()[idx],
			CollectionId:  collID,
			LoadState:     loadState[resp.GetPartitionNames()[idx]],
		}
		bakPartitions = append(bakPartitions, bakPartition)
		ct.meta.AddPartition(bakPartition)
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
		Properties:       pbconv.MilvusKVToBakKV(descResp.GetProperties()),
	}
	ct.meta.AddCollection(backupInfo)

	ct.collID = descResp.GetCollectionID()

	return nil
}

func (ct *CollectionTask) flushCollection(ctx context.Context) (*milvuspb.FlushResponse, error) {
	if ct.skipFlush {
		ct.logger.Info("skip flush collection")
		return nil, nil
	}

	ct.logger.Info("start to flush collection")
	start := time.Now()
	resp, err := ct.grpc.Flush(ctx, ct.dbName, ct.collName)
	if err != nil {
		return nil, fmt.Errorf("backup: flush collection %w", err)
	}

	ct.logger.Info("flush collection done", zap.Any("resp", resp), zap.Duration("cost", time.Since(start)))

	return resp, nil
}

func (ct *CollectionTask) addDMLPositionToMeta(flushResp *milvuspb.FlushResponse) {
	channelCheckpoints := make(map[string]string, len(flushResp.GetChannelCps()))
	var maxChannelBackupTimeStamp uint64
	for vch, checkpoint := range flushResp.GetChannelCps() {
		cp, err := utils.Base64MsgPosition(checkpoint)
		if err != nil {
			ct.logger.Error("backup: encode msg position", zap.Error(err))
			continue
		}
		channelCheckpoints[vch] = cp
		if maxChannelBackupTimeStamp == 0 {
			maxChannelBackupTimeStamp = checkpoint.GetTimestamp()
		} else if maxChannelBackupTimeStamp < checkpoint.GetTimestamp() {
			maxChannelBackupTimeStamp = checkpoint.GetTimestamp()
		}
	}
	ct.meta.UpdateCollection(ct.backupID, ct.collID,
		meta.SetCollectionChannelCheckpoints(channelCheckpoints),
		meta.SetCollectionBackupTimestamp(maxChannelBackupTimeStamp),
		meta.SetCollectionBackupPhysicalTimestamp(uint64(flushResp.GetCollSealTimes()[ct.collName])))
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

func (ct *CollectionTask) listInsertLogByAPI(ctx context.Context, binlogDir string, fieldsBinlog []milvus.BinlogInfo) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, ct.milvusStorage, binlogDir, true)
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

func (ct *CollectionTask) listDeltaLogByAPI(ctx context.Context, binlogDir string, fieldsBinlog []milvus.BinlogInfo) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, ct.milvusStorage, binlogDir, true)
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

func (ct *CollectionTask) listInsertLogByListFile(ctx context.Context, binlogDir string) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, ct.milvusStorage, binlogDir, true)
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
		ct.logger.Info("get insert logs done", zap.Int64("field_id", fieldID), zap.Int("count", len(binlogs)))
		if fileNum == 0 {
			fileNum = len(binlogs)
		} else if fileNum != len(binlogs) {
			return nil, 0, fmt.Errorf("backup: field %d has different file num to other fields", fieldID)
		}
		fields = append(fields, &backuppb.FieldBinlog{FieldID: fieldID, Binlogs: binlogs})
	}

	return fields, lo.Sum(sizes), nil
}

func (ct *CollectionTask) listDeltaLogByListFile(ctx context.Context, binlogDir string) ([]*backuppb.FieldBinlog, int64, error) {
	keys, sizes, err := storage.ListPrefixFlat(ctx, ct.milvusStorage, binlogDir, true)
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
	ct.logger.Info("get delta logs done", zap.Int("count", len(bakBinlogs)))

	return []*backuppb.FieldBinlog{{Binlogs: bakBinlogs}}, lo.Sum(sizes), nil
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
	ct.logger.Debug("insert log dir", zap.String("dir", insertLogDir))
	insertLogs, iSize, err := ct.listInsertLogByListFile(ctx, insertLogDir)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}
	ct.logger.Info("get insert logs done", zap.Int("count", len(insertLogs)))

	deltaLogDir := mpath.MilvusDeltaLogDir(ct.milvusRootPath, pathOpts...)
	ct.logger.Debug("delta log dir", zap.String("dir", deltaLogDir))
	deltaLogs, dSize, err := ct.listDeltaLogByListFile(ctx, deltaLogDir)
	if err != nil {
		return nil, fmt.Errorf("backup: list delta logs %w", err)
	}
	ct.logger.Info("get delta logs done", zap.Int("count", len(deltaLogs)))

	return &backuppb.SegmentBackupInfo{
		SegmentId:    seg.GetSegmentID(),
		CollectionId: seg.GetCollectionID(),
		PartitionId:  seg.GetPartitionID(),
		NumOfRows:    seg.GetNumRows(),
		GroupId:      ct.groupID(seg),
		IsL0:         seg.GetLevel() == commonpb.SegmentLevel_L0,
		Binlogs:      insertLogs,
		Deltalogs:    deltaLogs,
		Size:         iSize + dSize,
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
	segInfo, err := ct.restful.GetSegmentInfo(ctx, ct.dbName, seg.CollectionID, seg.SegmentID)
	if err != nil {
		return nil, fmt.Errorf("backup: get segment info %w", err)
	}
	pathOpts := []mpath.Option{
		mpath.CollectionID(seg.CollectionID),
		mpath.PartitionID(seg.PartitionID),
		mpath.SegmentID(seg.SegmentID),
	}

	insertLogDir := mpath.MilvusInsertLogDir(ct.milvusRootPath, pathOpts...)
	insertLogs, iSize, err := ct.listInsertLogByAPI(ctx, insertLogDir, segInfo.InsertLogs)
	if err != nil {
		return nil, fmt.Errorf("backup: list insert logs %w", err)
	}

	deltaLogDir := mpath.MilvusDeltaLogDir(ct.milvusRootPath, pathOpts...)
	deltaLogs, dSize, err := ct.listDeltaLogByAPI(ctx, deltaLogDir, segInfo.DeltaLogs)
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
		Size:         iSize + dSize,
		VChannel:     segInfo.VChannel,
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

	ct.addSegmentToMeta(bakSegs)

	return bakSegs, nil
}

func (ct *CollectionTask) addSegmentToMeta(segments []*backuppb.SegmentBackupInfo) {
	for _, seg := range segments {
		if seg.GetIsL0() && seg.GetPartitionId() == _allPartitionID {
			ct.meta.UpdateCollection(ct.backupID, seg.GetCollectionId(), meta.AddL0Segment(seg))
		} else {
			ct.meta.AddSegment(seg)
		}
	}
}

func (ct *CollectionTask) backupDML(ctx context.Context) error {
	ct.logger.Info("start to backup dml of collection")
	flushResp, err := ct.flushCollection(ctx)
	if err != nil {
		return fmt.Errorf("backup: backup dml %w", err)
	}
	ct.addDMLPositionToMeta(flushResp)

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
	destDir := mpath.BackupInsertLogDir(ct.backupDir, opts...)

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

	opt := storage.CopyObjectsOpt{
		Src:          ct.milvusStorage,
		Dest:         ct.backupStorage,
		Attrs:        attrs,
		CopyByServer: ct.crossStorage,
		Sem:          ct.copySem,
	}
	cpTask := storage.NewCopyObjectsTask(opt)
	if err := cpTask.Execute(ctx); err != nil {
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
	destDir := mpath.BackupDeltaLogDir(ct.backupDir, opts...)

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

	opt := storage.CopyObjectsOpt{
		Src:          ct.milvusStorage,
		Dest:         ct.backupStorage,
		Attrs:        attrs,
		CopyByServer: ct.crossStorage,
		Sem:          ct.copySem,
	}
	cpTask := storage.NewCopyObjectsTask(opt)
	if err := cpTask.Execute(ctx); err != nil {
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

	// TODO: now l0 segment not under partition, update l0 segment backuped to true will cause nil pointer error
	if seg.GetPartitionId() == _allPartitionID {
		return nil
	}
	ct.meta.UpdateSegment(seg.PartitionId, seg.SegmentId, meta.SetSegmentBackuped(true))

	return nil
}
