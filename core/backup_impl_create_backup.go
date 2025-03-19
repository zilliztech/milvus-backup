package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zilliztech/milvus-backup/core/meta"

	"github.com/golang/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

func conv2BakKV(kv []*commonpb.KeyValuePair) []*backuppb.KeyValuePair {
	return lo.Map(kv, func(item *commonpb.KeyValuePair, _ int) *backuppb.KeyValuePair {
		return &backuppb.KeyValuePair{
			Key:   item.Key,
			Value: item.Value,
		}
	})
}

func (b *BackupContext) CreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest) *backuppb.BackupInfoResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive CreateBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.String("databaseCollections", utils.GetCreateDBCollections(request)),
		zap.Bool("async", request.GetAsync()),
		zap.Bool("force", request.GetForce()),
		zap.Bool("metaOnly", request.GetMetaOnly()))

	resp := &backuppb.BackupInfoResponse{
		RequestId: request.GetRequestId(),
	}

	if !b.started {
		err := b.Start()
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
			return resp
		}
	}

	// backup name validate
	if request.GetBackupName() == "" {
		request.BackupName = "backup_" + fmt.Sprint(time.Now().UTC().Format("2006_01_02_15_04_05_")) + fmt.Sprint(time.Now().Nanosecond())
	}
	if request.GetBackupName() != "" {
		exist, err := b.getBackupStorageClient().Exist(b.ctx, b.backupBucketName, b.backupRootPath+meta.SEPERATOR+request.GetBackupName())
		if err != nil {
			errMsg := fmt.Sprintf("fail to check whether exist backup with name: %s", request.GetBackupName())
			log.Error(errMsg, zap.Error(err))
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errMsg + "/n" + err.Error()
			return resp
		}
		if exist {
			errMsg := fmt.Sprintf("backup already exist with the name: %s", request.GetBackupName())
			log.Error(errMsg)
			resp.Code = backuppb.ResponseCode_Parameter_Error
			resp.Msg = errMsg
			return resp
		}
	}
	err := utils.ValidateType(request.GetBackupName(), BackupName)
	if err != nil {
		log.Error("illegal backup name", zap.Error(err))
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = err.Error()
		return resp
	}

	milvusVersion, err := b.getMilvusClient().GetVersion(b.ctx)
	if err != nil {
		log.Error("fail to get milvus version", zap.Error(err))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	backup := &backuppb.BackupInfo{
		Id:            request.GetRequestId(),
		StateCode:     backuppb.BackupTaskStateCode_BACKUP_INITIAL,
		StartTime:     time.Now().UnixNano() / int64(time.Millisecond),
		Name:          request.BackupName,
		MilvusVersion: milvusVersion,
	}
	b.meta.AddBackup(backup)
	//levelBackupInfo := NewLeveledBackupInfo(backup)
	//b.backupTasksCache.Store(request.GetRequestId(), levelBackupInfo)
	//b.backupNameIdDict.Store(name, request.GetRequestId())

	if request.Async {
		go b.executeCreateBackup(ctx, request, backup)
		asyncResp := &backuppb.BackupInfoResponse{
			RequestId: request.GetRequestId(),
			Code:      backuppb.ResponseCode_Success,
			Msg:       "create backup is executing asynchronously",
			Data:      backup,
		}
		return asyncResp
	} else {
		err := b.executeCreateBackup(ctx, request, backup)
		resp.Data = b.meta.GetBackup(backup.GetId())
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
		} else {
			resp.Code = backuppb.ResponseCode_Success
			resp.Msg = "success"
		}
		return resp
	}
}

type collectionStruct struct {
	db             string
	collectionName string
}

// parse collections to backup
// For backward compatibility：
//
//	1，parse dbCollections first,
//	2，if dbCollections not set, use collectionNames
func (b *BackupContext) parseBackupCollections(request *backuppb.CreateBackupRequest) ([]collectionStruct, error) {
	log.Debug("Request collection names",
		zap.Strings("request_collection_names", request.GetCollectionNames()),
		zap.String("request_db_collections", utils.GetCreateDBCollections(request)),
		zap.Int("length", len(request.GetCollectionNames())))
	var toBackupCollections []collectionStruct

	dbCollectionsStr := utils.GetCreateDBCollections(request)
	// first priority: dbCollections
	if dbCollectionsStr != "" {
		var dbCollections meta.DbCollections
		err := jsoniter.UnmarshalFromString(dbCollectionsStr, &dbCollections)
		if err != nil {
			log.Error("fail in unmarshal dbCollections in CreateBackupRequest", zap.String("dbCollections", dbCollectionsStr), zap.Error(err))
			return nil, err
		}
		for db, collections := range dbCollections {
			if len(collections) == 0 {
				resp, err := b.getMilvusClient().ListCollections(b.ctx, db)
				if err != nil {
					log.Error("fail in ListCollections", zap.Error(err))
					return nil, err
				}
				for _, coll := range resp.CollectionNames {
					log.Debug("Add collection to toBackupCollections", zap.String("db", db), zap.String("collection", coll))
					toBackupCollections = append(toBackupCollections, collectionStruct{db, coll})
				}
			} else {
				for _, coll := range collections {
					toBackupCollections = append(toBackupCollections, collectionStruct{db, coll})
				}
			}
		}
		log.Debug("Parsed backup collections from request.db_collections", zap.Int("length", len(toBackupCollections)))
		return toBackupCollections, nil
	}

	if request.GetCollectionNames() == nil || len(request.GetCollectionNames()) == 0 {
		dbs, err := b.getMilvusClient().ListDatabases(b.ctx)
		if err != nil {
			// compatible to milvus under v2.2.8 without database support
			if !b.getMilvusClient().SupportMultiDatabase() {
				// default database only
				resp, err := b.getMilvusClient().ListCollections(b.ctx, "default")
				if err != nil {
					log.Error("fail in ListCollections", zap.Error(err))
					return nil, err
				}
				for _, coll := range resp.CollectionNames {
					toBackupCollections = append(toBackupCollections, collectionStruct{"default", coll})
				}
			} else {
				log.Error("fail in ListDatabases", zap.Error(err))
				return nil, err
			}
		} else {
			for _, db := range dbs {
				resp, err := b.getMilvusClient().ListCollections(b.ctx, db)
				if err != nil {
					log.Error("fail in ListCollections", zap.Error(err))
					return nil, err
				}
				for _, coll := range resp.CollectionNames {
					toBackupCollections = append(toBackupCollections, collectionStruct{db, coll})
				}
			}
		}
		log.Debug(fmt.Sprintf("List %v collections", len(toBackupCollections)))
	} else {
		for _, collectionName := range request.GetCollectionNames() {
			var dbName = "default"
			if strings.Contains(collectionName, ".") {
				splits := strings.Split(collectionName, ".")
				dbName = splits[0]
				collectionName = splits[1]
			}

			exist, err := b.getMilvusClient().HasCollection(b.ctx, dbName, collectionName)
			if err != nil {
				log.Error("fail in HasCollection", zap.Error(err))
				return nil, err
			}
			if !exist {
				errMsg := fmt.Sprintf("request backup collection does not exist: %s.%s", dbName, collectionName)
				log.Error(errMsg)
				return nil, errors.New(errMsg)
			}
			toBackupCollections = append(toBackupCollections, collectionStruct{dbName, collectionName})
		}
	}

	return toBackupCollections, nil
}

func (b *BackupContext) backupCollectionPrepare(ctx context.Context, backupInfo *backuppb.BackupInfo, collection collectionStruct, force bool) error {
	log.Info("start backup collection", zap.String("db", collection.db), zap.String("collection", collection.collectionName))
	// list collection result is not complete
	descResp, err := b.getMilvusClient().DescribeCollection(b.ctx, collection.db, collection.collectionName)
	if err != nil {
		log.Error("fail in DescribeCollection", zap.Error(err))
		return err
	}
	fields := make([]*backuppb.FieldSchema, 0, len(descResp.Schema.Fields))
	for _, field := range descResp.Schema.Fields {
		fieldBak := &backuppb.FieldSchema{
			FieldID:          field.FieldID,
			Name:             field.Name,
			IsPrimaryKey:     field.IsPrimaryKey,
			Description:      field.Description,
			AutoID:           field.AutoID,
			DataType:         backuppb.DataType(field.DataType),
			TypeParams:       conv2BakKV(field.TypeParams),
			IndexParams:      conv2BakKV(field.IndexParams),
			IsDynamic:        field.IsDynamic,
			IsPartitionKey:   field.IsPartitionKey,
			Nullable:         field.Nullable,
			ElementType:      backuppb.DataType(field.ElementType),
			IsFunctionOutput: field.IsFunctionOutput,
		}
		defaultValue := field.DefaultValue
		if defaultValue != nil {
			bytes, err := proto.Marshal(field.DefaultValue)
			if err != nil {
				return err
			}
			fieldBak.DefaultValueProto = string(bytes)
		}
		fields = append(fields, fieldBak)
	}
	functions := make([]*backuppb.FunctionSchema, 0, len(descResp.Schema.Functions))
	for _, function := range descResp.Schema.Functions {
		functionBak := &backuppb.FunctionSchema{
			Name:             function.Name,
			Id:               function.Id,
			Description:      function.Description,
			Type:             backuppb.FunctionType(function.Type),
			InputFieldNames:  function.InputFieldNames,
			InputFieldIds:    function.InputFieldIds,
			OutputFieldNames: function.OutputFieldNames,
			OutputFieldIds:   function.OutputFieldIds,
			Params:           conv2BakKV(function.Params),
		}
		functions = append(functions, functionBak)
	}
	schema := &backuppb.CollectionSchema{
		Name:               descResp.Schema.Name,
		Description:        descResp.Schema.Description,
		AutoID:             descResp.Schema.AutoID,
		Fields:             fields,
		EnableDynamicField: descResp.Schema.EnableDynamicField,
		Functions:          functions,
	}

	indexInfos := make([]*backuppb.IndexInfo, 0)
	indexDict := make(map[string]*backuppb.IndexInfo)
	log.Info("try to get index", zap.String("collection_name", descResp.CollectionName))
	indexes, err := b.getMilvusClient().ListIndex(b.ctx, collection.db, descResp.CollectionName)
	if err != nil && !strings.Contains(err.Error(), "index not found") {
		log.Error("fail in DescribeIndex", zap.Error(err))
		return err
	}

	log.Info("List index", zap.String("collection_name", descResp.CollectionName), zap.Any("indexes", indexes))
	for _, index := range indexes {
		if _, ok := indexDict[index.IndexName]; ok {
			continue
		} else {
			params := lo.SliceToMap(index.Params, func(item *commonpb.KeyValuePair) (string, string) {
				return item.Key, item.Value
			})
			indexInfo := &backuppb.IndexInfo{
				FieldName: index.FieldName,
				IndexName: index.IndexName,
				IndexType: params["index_type"],
				Params:    params,
			}
			indexInfos = append(indexInfos, indexInfo)
			indexDict[index.IndexName] = indexInfo
		}
	}

	collectionBackup := &backuppb.CollectionBackupInfo{
		Id:               backupInfo.Id,
		StateCode:        backuppb.BackupTaskStateCode_BACKUP_INITIAL,
		StartTime:        time.Now().Unix(),
		CollectionId:     descResp.CollectionID,
		DbName:           collection.db,
		CollectionName:   descResp.CollectionName,
		Schema:           schema,
		ShardsNum:        descResp.ShardsNum,
		ConsistencyLevel: backuppb.ConsistencyLevel(descResp.ConsistencyLevel),
		HasIndex:         len(indexInfos) > 0,
		IndexInfos:       indexInfos,
	}
	b.meta.AddCollection(collectionBackup)

	showPartResp, err := b.getMilvusClient().ShowPartitions(b.ctx, collectionBackup.GetDbName(), collectionBackup.GetCollectionName())
	if err != nil {
		log.Error("fail to ShowPartitions", zap.Error(err))
		return err
	}

	// use GetLoadingProgress currently, GetLoadState is a new interface @20230104  milvus pr#21515
	collectionLoadProgress, err := b.getMilvusClient().GetLoadingProgress(ctx, collectionBackup.GetDbName(), collectionBackup.GetCollectionName(), []string{})
	if err != nil {
		log.Error("fail to GetLoadingProgress of collection", zap.Error(err))
		return err
	}

	var collectionLoadState string
	partitionLoadStates := make(map[string]string, len(showPartResp.PartitionNames))
	if collectionLoadProgress == 0 {
		collectionLoadState = meta.LoadState_NotLoad
		for _, partitionName := range showPartResp.PartitionNames {
			partitionLoadStates[partitionName] = meta.LoadState_NotLoad
		}
	} else if collectionLoadProgress == 100 {
		collectionLoadState = meta.LoadState_Loaded
		for _, partitionName := range showPartResp.PartitionNames {
			partitionLoadStates[partitionName] = meta.LoadState_Loaded
		}
	} else {
		collectionLoadState = meta.LoadState_Loading
		for _, partitionName := range showPartResp.PartitionNames {
			loadProgress, err := b.getMilvusClient().GetLoadingProgress(ctx, collectionBackup.GetDbName(), collectionBackup.GetCollectionName(), []string{partitionName})
			if err != nil {
				log.Error("fail to GetLoadingProgress of partition", zap.Error(err))
				return err
			}
			if loadProgress == 0 {
				partitionLoadStates[partitionName] = meta.LoadState_NotLoad
			} else if loadProgress == 100 {
				partitionLoadStates[partitionName] = meta.LoadState_Loaded
			} else {
				partitionLoadStates[partitionName] = meta.LoadState_Loading
			}
		}
	}

	// fill segments
	unfilledSegments := make([]*milvuspb.PersistentSegmentInfo, 0)
	if !force {
		// Flush
		segmentEntitiesBeforeFlush, err := b.getMilvusClient().GetPersistentSegmentInfo(ctx, collectionBackup.GetDbName(), collectionBackup.GetCollectionName())
		if err != nil {
			return err
		}
		log.Info("GetPersistentSegmentInfo before flush from milvus",
			zap.String("databaseName", collectionBackup.GetDbName()),
			zap.String("collectionName", collectionBackup.GetCollectionName()),
			zap.Int("segmentNumBeforeFlush", len(segmentEntitiesBeforeFlush)))
		flushResp, err := b.getMilvusClient().Flush(ctx, collectionBackup.GetDbName(), collectionBackup.GetCollectionName())
		if err != nil {
			log.Warn("fail to flush the collection",
				zap.String("databaseName", collectionBackup.GetDbName()),
				zap.String("collectionName", collectionBackup.GetCollectionName()),
				zap.Error(err))
			return err
		}

		//collectionBackup.BackupTimestamp = utils.ComposeTS(timeOfSeal, 0)
		channelCheckpoints := make(map[string]string, 0)
		var maxChannelBackupTimeStamp uint64 = 0
		for vch, checkpoint := range flushResp.GetChannelCps() {
			channelCheckpoints[vch] = utils.Base64MsgPosition(checkpoint)
			if maxChannelBackupTimeStamp == 0 {
				maxChannelBackupTimeStamp = checkpoint.GetTimestamp()
			} else if maxChannelBackupTimeStamp < checkpoint.GetTimestamp() {
				maxChannelBackupTimeStamp = checkpoint.GetTimestamp()
			}
		}
		b.meta.UpdateCollection(collectionBackup.Id, collectionBackup.CollectionId,
			meta.SetCollectionChannelCheckpoints(channelCheckpoints),
			meta.SetCollectionBackupTimestamp(maxChannelBackupTimeStamp),
			meta.SetCollectionBackupPhysicalTimestamp(uint64(flushResp.GetCollSealTimes()[collectionBackup.GetCollectionName()])))
		log.Info("flush segments",
			zap.String("databaseName", collectionBackup.GetDbName()),
			zap.String("collectionName", collectionBackup.GetCollectionName()),
			zap.Int64s("newSealedSegmentIDs", flushResp.GetCollSegIDs()[collectionBackup.GetCollectionName()].GetData()),
			zap.Int64s("flushedSegmentIDs", flushResp.GetFlushCollSegIDs()[collectionBackup.GetCollectionName()].GetData()),
			zap.Uint64("timeOfSeal", uint64(flushResp.GetCollSealTimes()[collectionBackup.GetCollectionName()])),
			zap.Uint64("BackupTimestamp", collectionBackup.BackupTimestamp),
			zap.Any("channelCPs", flushResp.GetChannelCps()))

		segmentEntitiesAfterFlush, err := b.getMilvusClient().GetPersistentSegmentInfo(ctx, collectionBackup.GetDbName(), collectionBackup.GetCollectionName())
		if err != nil {
			return err
		}

		segmentIDsBeforeFlush := lo.Map(segmentEntitiesBeforeFlush, func(segment *milvuspb.PersistentSegmentInfo, _ int) int64 { return segment.SegmentID })
		segmentIDsAfterFlush := lo.Map(segmentEntitiesAfterFlush, func(segment *milvuspb.PersistentSegmentInfo, _ int) int64 { return segment.SegmentID })
		newL0Segments := lo.Filter(segmentEntitiesAfterFlush, func(segment *milvuspb.PersistentSegmentInfo, _ int) bool {
			return !lo.Contains(segmentIDsBeforeFlush, segment.SegmentID) && segment.Level == commonpb.SegmentLevel_L0
		})
		newL0SegmentsIDs := lo.Map(newL0Segments, func(segment *milvuspb.PersistentSegmentInfo, _ int) int64 { return segment.SegmentID })

		log.Info("GetPersistentSegmentInfo after flush from milvus",
			zap.String("databaseName", collectionBackup.GetDbName()),
			zap.String("collectionName", collectionBackup.GetCollectionName()),
			zap.Int64s("segmentIDsBeforeFlush", segmentIDsBeforeFlush),
			zap.Int64s("segmentIDsAfterFlush", segmentIDsAfterFlush),
			zap.Int64s("newL0SegmentsIDs", newL0SegmentsIDs))

		for _, seg := range segmentEntitiesAfterFlush {
			unfilledSegments = append(unfilledSegments, seg)
		}
	} else {
		// Flush
		segmentEntitiesBeforeFlush, err := b.getMilvusClient().GetPersistentSegmentInfo(ctx, collectionBackup.GetDbName(), collectionBackup.GetCollectionName())
		if err != nil {
			log.Warn(fmt.Sprintf("fail to flush the collection: %s", collectionBackup.GetCollectionName()), zap.Error(err))
			return err
		}
		log.Info("GetPersistentSegmentInfo from milvus",
			zap.String("databaseName", collectionBackup.GetDbName()),
			zap.String("collectionName", collectionBackup.GetCollectionName()),
			zap.Int("segmentNum", len(segmentEntitiesBeforeFlush)))
		for _, seg := range segmentEntitiesBeforeFlush {
			unfilledSegments = append(unfilledSegments, seg)
		}
	}

	newSegIDs := lo.Map(unfilledSegments, func(segment *milvuspb.PersistentSegmentInfo, _ int) int64 { return segment.SegmentID })
	log.Debug("Finished fill segment",
		zap.String("databaseName", collectionBackup.GetDbName()),
		zap.String("collectionName", collectionBackup.GetCollectionName()),
		zap.Int64s("segments", newSegIDs))

	partSegInfoMap := make(map[int64][]*backuppb.SegmentBackupInfo)
	for _, v := range unfilledSegments {
		segment := v
		segmentInfo := &backuppb.SegmentBackupInfo{
			SegmentId:    segment.SegmentID,
			CollectionId: segment.CollectionID,
			PartitionId:  segment.PartitionID,
			NumOfRows:    segment.NumRows,
		}
		b.meta.AddSegment(segmentInfo)
		partSegInfoMap[segment.PartitionID] = append(partSegInfoMap[segment.PartitionID], segmentInfo)
	}

	for i, partitionID := range showPartResp.PartitionIDs {
		partitionSegments := partSegInfoMap[partitionID]
		var size int64 = 0
		for _, seg := range partitionSegments {
			size += seg.GetSize()
		}
		partitionBackupInfo := &backuppb.PartitionBackupInfo{
			PartitionId:    partitionID,
			PartitionName:  showPartResp.PartitionNames[i],
			CollectionId:   collectionBackup.GetCollectionId(),
			SegmentBackups: partSegInfoMap[partitionID],
			Size:           size,
			LoadState:      partitionLoadStates[showPartResp.PartitionNames[i]],
		}
		b.meta.AddPartition(partitionBackupInfo)
	}

	l0segments, exist := partSegInfoMap[-1]
	if exist {
		b.meta.UpdateCollection(collectionBackup.Id, collectionBackup.CollectionId, meta.SetL0Segments(l0segments))
	}

	partitionBackupInfos := b.meta.GetPartitions(collectionBackup.CollectionId)
	log.Info("finish build partition info",
		zap.String("collectionName", collectionBackup.GetCollectionName()),
		zap.Int("partitionNum", len(partitionBackupInfos)),
		zap.Int("l0SegmentsNum", len(l0segments)))

	var collectionBackupSize int64 = 0
	for _, part := range partitionBackupInfos {
		collectionBackupSize += part.GetSize()
	}

	b.meta.UpdateCollection(collectionBackup.Id, collectionBackup.CollectionId, meta.SetCollectionLoadState(collectionLoadState), meta.SetCollectionSize(collectionBackupSize))
	return nil
}

func (b *BackupContext) backupCollectionExecute(ctx context.Context, collectionBackup *backuppb.CollectionBackupInfo) error {
	log.Info("backupCollectionExecute", zap.Any("collectionMeta", collectionBackup.String()))
	backupInfo := b.meta.GetBackupByCollectionID(collectionBackup.GetCollectionId())
	backupBinlogPath := meta.BackupBinlogDirPath(b.backupRootPath, backupInfo.GetName())
	for _, partition := range b.meta.GetPartitions(collectionBackup.CollectionId) {
		segmentBackupInfos := make([]*backuppb.SegmentBackupInfo, 0)
		var currentSize int64 = 0
		var groupID int64 = 1
		// currently not group l0 segments
		//var currentL0Size int64 = 0
		//var l0GroupID int64 = 1
		segments := b.meta.GetSegments(partition.GetPartitionId())
		for _, v := range segments {
			segment := v
			err := b.fillSegmentBackupInfo(ctx, segment)
			if err != nil {
				log.Error("Fail to fill segment backup info", zap.Error(err))
				return err
			}
			if !segment.IsL0 {
				if currentSize > BackupSegmentGroupMaxSizeInMB*1024*1024 { // 256MB
					groupID++
					currentSize = 0
				}
				currentSize = currentSize + segment.GetSize()
				b.meta.UpdateSegment(segment.GetPartitionId(), segment.GetSegmentId(), meta.SetGroupID(groupID))
			} else {
				//if currentSize > BackupSegmentGroupMaxSizeInMB*1024*1024 { // 256MB
				//	l0GroupID++
				//	currentL0Size = 0
				//}
				//currentL0Size = currentL0Size + segment.GetSize()
				b.meta.UpdateSegment(segment.GetPartitionId(), segment.GetSegmentId(), meta.SetGroupID(segment.GetSegmentId()))
			}
			segmentBackupInfos = append(segmentBackupInfos, segment)
		}
		log.Info("Begin copy data",
			zap.String("dbName", collectionBackup.GetDbName()),
			zap.String("collectionName", collectionBackup.GetCollectionName()),
			zap.Int64("collectionID", partition.GetCollectionId()),
			zap.Int64("partitionID", partition.GetPartitionId()))

		sort.SliceStable(segmentBackupInfos, func(i, j int) bool {
			return segmentBackupInfos[i].Size < segmentBackupInfos[j].Size
		})

		segmentIDs := lo.Map(segmentBackupInfos, func(segment *backuppb.SegmentBackupInfo, _ int) int64 {
			return segment.GetSegmentId()
		})
		err := b.copySegments(ctx, backupBinlogPath, segmentIDs)
		if err != nil {
			return err
		}
	}

	l0Segments := collectionBackup.GetL0Segments()
	segmentBackupInfos := make([]*backuppb.SegmentBackupInfo, 0)
	segmentIDs := make([]int64, 0)
	for _, v := range l0Segments {
		segment := v
		err := b.fillSegmentBackupInfo(ctx, segment)
		if err != nil {
			log.Error("Fail to fill segment backup info", zap.Error(err))
			return err
		}
		segmentIDs = append(segmentIDs, segment.GetSegmentId())
		segmentBackupInfos = append(segmentBackupInfos, b.meta.GetSegment(segment.GetSegmentId()))
	}
	err := b.copySegments(ctx, backupBinlogPath, segmentIDs)
	if err != nil {
		log.Error("Fail to fill segment backup info", zap.Error(err))
		return err
	}
	b.meta.UpdateCollection(collectionBackup.Id, collectionBackup.CollectionId, meta.SetL0Segments(segmentBackupInfos))

	b.meta.UpdateCollection(collectionBackup.Id, collectionBackup.CollectionId, meta.SetCollectionEndTime(time.Now().Unix()))
	log.Info("Finish copy data",
		zap.String("dbName", collectionBackup.GetDbName()),
		zap.String("collectionName", collectionBackup.GetCollectionName()))
	return nil
}

func (b *BackupContext) pauseMilvusGC(ctx context.Context, gcAddress string, pauseSeconds int) {
	pauseAPI := "/management/datacoord/garbage_collection/pause"
	params := url.Values{}
	params.Add("pause_seconds", strconv.Itoa(pauseSeconds))
	fullURL := fmt.Sprintf("%s?%s", gcAddress+pauseAPI, params.Encode())
	response, err := http.Get(fullURL)

	if err != nil {
		log.Warn("Pause Milvus GC Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	defer response.Body.Close()
	// Read the response body
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Warn("Read response Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	log.Info("Pause Milvus GC response", zap.String("response", string(body)), zap.String("address", gcAddress), zap.Int("pauseSeconds", pauseSeconds))
}

func (b *BackupContext) resumeMilvusGC(ctx context.Context, gcAddress string) {
	pauseAPI := "/management/datacoord/garbage_collection/resume"
	fullURL := gcAddress + pauseAPI
	response, err := http.Get(fullURL)
	if err != nil {
		log.Warn("Resume Milvus GC Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	// Read the response body
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Warn("Read response Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	log.Info("Resume Milvus GC response", zap.String("response", string(body)), zap.String("address", gcAddress))
}

func (b *BackupContext) executeCreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest, backupInfo *backuppb.BackupInfo) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// set backup state
	b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_EXECUTING))

	// pause GC
	if request.GetGcPauseEnable() || b.params.BackupCfg.GcPauseEnable {
		var pause = 0
		if request.GetGcPauseSeconds() == 0 {
			pause = b.params.BackupCfg.GcPauseSeconds
		} else {
			pause = int(request.GetGcPauseSeconds())
		}
		var gcAddress string = ""
		if request.GetGcPauseAddress() == "" {
			gcAddress = b.params.BackupCfg.GcPauseAddress
		} else {
			gcAddress = request.GetGcPauseAddress()
		}
		b.pauseMilvusGC(ctx, gcAddress, pause)
		defer b.resumeMilvusGC(ctx, gcAddress)
	}

	// 1, get collection level meta
	toBackupCollections, err := b.parseBackupCollections(request)
	if err != nil {
		log.Error("parse backup collections from request failed", zap.Error(err))
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
		return err
	}
	collectionNames := make([]string, len(toBackupCollections))
	for i, coll := range toBackupCollections {
		collectionNames[i] = coll.db + "." + coll.collectionName
	}
	log.Info("collections to backup", zap.Strings("collections", collectionNames))

	jobIds := make([]int64, 0)
	for _, collection := range toBackupCollections {
		collectionClone := collection
		job := func(ctx context.Context) error {
			retryForSpecificError := func(retries int, delay time.Duration) error {
				for i := 0; i < retries; i++ {
					err := b.backupCollectionPrepare(ctx, backupInfo, collectionClone, request.GetForce())
					// If no error, return successfully
					if err == nil {
						return nil
					}
					// Retry only for the specific error
					if strings.Contains(err.Error(), "rate limit exceeded") {
						fmt.Printf("Attempt %d: Temporary error occurred, retrying...\n", i+1)
						time.Sleep(delay)
						continue
					}
					// Return immediately for any other error
					return err
				}
				return fmt.Errorf("operation failed after %d retries", retries)
			}
			return retryForSpecificError(10, 10*time.Second)
		}
		jobId := b.getBackupCollectionWorkerPool().SubmitWithId(job)
		jobIds = append(jobIds, jobId)
	}
	err = b.getBackupCollectionWorkerPool().WaitJobs(jobIds)
	if err != nil {
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
		return err
	}
	log.Info("Finish prepare all collections meta")

	if !request.GetMetaOnly() {
		for collectionID, collection := range b.meta.GetCollections(backupInfo.GetId()) {
			collectionClone := collection
			log.Info("before backupCollectionExecute", zap.Int64("collectionID", collectionID), zap.String("collection", collection.CollectionName))
			job := func(ctx context.Context) error {
				err := b.backupCollectionExecute(ctx, collectionClone)
				return err
			}
			jobId := b.getBackupCollectionWorkerPool().SubmitWithId(job)
			jobIds = append(jobIds, jobId)
		}

		err = b.getBackupCollectionWorkerPool().WaitJobs(jobIds)
		if err != nil {
			b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
			return err
		}
	} else {
		log.Info("skip copy data because it is a metaOnly backup request")
	}
	backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_SUCCESS
	b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_SUCCESS), meta.SetEndTime(time.Now().UnixNano()/int64(time.Millisecond)))

	if request.GetRbac() {
		err = b.backupRBAC(ctx, backupInfo)
		if err != nil {
			backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_FAIL
			backupInfo.ErrorMessage = err.Error()
			return err
		}
	}

	// 7, write meta data
	err = b.writeBackupInfoMeta(ctx, backupInfo.GetId())
	if err != nil {
		backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_FAIL
		backupInfo.ErrorMessage = err.Error()
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL),
			meta.SetErrorMessage(err.Error()))
		return err
	}
	log.Info("finish backup all collections",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.Bool("async", request.GetAsync()))
	return nil
}

func (b *BackupContext) writeBackupInfoMeta(ctx context.Context, id string) error {
	backupInfo := b.meta.GetFullMeta(id)
	log.Debug("Final backupInfo", zap.String("backupInfo", backupInfo.String()))
	output, _ := meta.Serialize(backupInfo)
	log.Debug("backup meta", zap.String("value", string(output.BackupMetaBytes)))
	log.Debug("collection meta", zap.String("value", string(output.CollectionMetaBytes)))
	log.Debug("partition meta", zap.String("value", string(output.PartitionMetaBytes)))
	log.Debug("segment meta", zap.String("value", string(output.SegmentMetaBytes)))

	collectionBackups := backupInfo.GetCollectionBackups()
	collectionPositions := make(map[string][]*backuppb.ChannelPosition)
	for _, collectionBackup := range collectionBackups {
		collectionCPs := make([]*backuppb.ChannelPosition, 0, len(collectionBackup.GetChannelCheckpoints()))
		for vCh, position := range collectionBackup.GetChannelCheckpoints() {
			collectionCPs = append(collectionCPs, &backuppb.ChannelPosition{
				Name:     vCh,
				Position: position,
			})
		}
		collectionPositions[collectionBackup.GetCollectionName()] = collectionCPs
	}
	channelCPsBytes, err := json.Marshal(collectionPositions)
	if err != nil {
		return err
	}
	log.Debug("channel cp meta", zap.String("value", string(channelCPsBytes)))

	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.BackupMetaPath(b.backupRootPath, backupInfo.GetName()), output.BackupMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.CollectionMetaPath(b.backupRootPath, backupInfo.GetName()), output.CollectionMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.PartitionMetaPath(b.backupRootPath, backupInfo.GetName()), output.PartitionMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.SegmentMetaPath(b.backupRootPath, backupInfo.GetName()), output.SegmentMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.FullMetaPath(b.backupRootPath, backupInfo.GetName()), output.FullMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.ChannelCPMetaPath(b.backupRootPath, backupInfo.GetName()), channelCPsBytes)

	log.Info("finish writeBackupInfoMeta",
		zap.String("path", meta.BackupDirPath(b.backupRootPath, backupInfo.GetName())),
		zap.String("backupName", backupInfo.GetName()),
		zap.String("backup meta", string(output.BackupMetaBytes)))
	return nil
}

func (b *BackupContext) copySegments(ctx context.Context, backupBinlogPath string, segmentIDs []int64) error {
	jobIds := make([]int64, 0)
	for _, v := range segmentIDs {
		segmentID := v
		segment := b.meta.GetSegment(segmentID)
		job := func(ctx context.Context) error {
			return b.copySegment(ctx, backupBinlogPath, segment)
		}
		jobId := b.getCopyDataWorkerPool().SubmitWithId(job)
		jobIds = append(jobIds, jobId)
	}

	err := b.getCopyDataWorkerPool().WaitJobs(jobIds)
	return err
}

func (b *BackupContext) copySegment(ctx context.Context, backupBinlogPath string, segment *backuppb.SegmentBackupInfo) error {
	log := log.With(zap.Int64("collection_id", segment.GetCollectionId()),
		zap.Int64("partition_id", segment.GetPartitionId()),
		zap.Int64("segment_id", segment.GetSegmentId()),
		zap.Int64("group_id", segment.GetGroupId()))
	log.Info("copy segment", zap.String("backupBinlogPath", backupBinlogPath))
	// generate target path
	// milvus_rootpath/insert_log/collection_id/partition_id/segment_id/ =>
	// backup_rootpath/backup_name/binlog/insert_log/collection_id/partition_id/group_id/segment_id
	backupPathFunc := func(binlogPath, rootPath, backupBinlogPath string) string {
		if rootPath == "" {
			return backupBinlogPath + meta.SEPERATOR + binlogPath
		} else {
			return strings.Replace(binlogPath, rootPath, backupBinlogPath, 1)
		}
	}
	// insert log
	for _, binlogs := range segment.GetBinlogs() {
		for _, binlog := range binlogs.GetBinlogs() {
			targetPath := backupPathFunc(binlog.GetLogPath(), b.milvusRootPath, backupBinlogPath)
			// use segmentID as group id
			segment.GroupId = segment.SegmentId
			if segment.GetGroupId() != 0 {
				targetPath = strings.Replace(targetPath,
					strconv.FormatInt(segment.GetPartitionId(), 10),
					strconv.FormatInt(segment.GetPartitionId(), 10)+"/"+strconv.FormatInt(segment.GetGroupId(), 10),
					1)
			}
			if targetPath == binlog.GetLogPath() {
				return errors.New(fmt.Sprintf("copy src path and dst path can not be the same, src: %s dst: %s", binlog.GetLogPath(), targetPath))
			}

			//binlog := binlog
			exist, err := b.getMilvusStorageClient().Exist(ctx, b.milvusBucketName, binlog.GetLogPath())
			if err != nil {
				log.Info("Fail to check file exist",
					zap.Error(err),
					zap.String("file", binlog.GetLogPath()))
				return err
			}
			if !exist {
				log.Error("Binlog file not exist",
					zap.Error(err),
					zap.String("file", binlog.GetLogPath()))
				return err
			}

			err = retry.Do(ctx, func() error {
				path := binlog.GetLogPath()
				return b.getBackupCopier().Copy(ctx, path, targetPath, b.milvusBucketName, b.backupBucketName)
			}, retry.Sleep(2*time.Second), retry.Attempts(5))
			if err != nil {
				log.Info("Fail to copy file after retry",
					zap.Error(err),
					zap.String("from", binlog.GetLogPath()),
					zap.String("to", targetPath))
				return err
			} else {
				log.Debug("Successfully copy file",
					zap.String("from", binlog.GetLogPath()),
					zap.String("to", targetPath))
			}
		}
	}
	// delta log
	for _, binlogs := range segment.GetDeltalogs() {
		for _, binlog := range binlogs.GetBinlogs() {
			targetPath := backupPathFunc(binlog.GetLogPath(), b.milvusRootPath, backupBinlogPath)
			if segment.GetGroupId() != 0 {
				targetPath = strings.Replace(targetPath,
					strconv.FormatInt(segment.GetPartitionId(), 10),
					strconv.FormatInt(segment.GetPartitionId(), 10)+"/"+strconv.FormatInt(segment.GetGroupId(), 10),
					1)
			}
			if targetPath == binlog.GetLogPath() {
				return errors.New(fmt.Sprintf("copy src path and dst path can not be the same, src: %s dst: %s", binlog.GetLogPath(), targetPath))
			}

			//binlog := binlog
			exist, err := b.getMilvusStorageClient().Exist(ctx, b.milvusBucketName, binlog.GetLogPath())
			if err != nil {
				log.Info("Fail to check file exist",
					zap.Error(err),
					zap.String("file", binlog.GetLogPath()))
				return err
			}
			if !exist {
				log.Error("Binlog file not exist",
					zap.Error(err),
					zap.String("file", binlog.GetLogPath()))
				return errors.New("Binlog file not exist " + binlog.GetLogPath())
			}
			err = retry.Do(ctx, func() error {
				path := binlog.GetLogPath()
				return b.getBackupCopier().Copy(ctx, path, targetPath, b.milvusBucketName, b.backupBucketName)
			}, retry.Sleep(2*time.Second), retry.Attempts(5))
			if err != nil {
				log.Info("Fail to copy file after retry",
					zap.Error(err),
					zap.String("from", binlog.GetLogPath()),
					zap.String("to", targetPath))
				return err
			} else {
				log.Debug("Successfully copy file",
					zap.String("from", binlog.GetLogPath()),
					zap.String("to", targetPath))
			}
		}
	}
	b.meta.UpdateSegment(segment.GetPartitionId(), segment.GetSegmentId(), meta.SetSegmentBackuped(true))
	return nil
}

func (b *BackupContext) fillSegmentBackupInfo(ctx context.Context, segmentBackupInfo *backuppb.SegmentBackupInfo) error {
	var size int64 = 0
	var rootPath string

	if b.params.MinioCfg.RootPath != "" {
		rootPath = fmt.Sprintf("%s/", b.params.MinioCfg.RootPath)
	} else {
		rootPath = ""
	}

	insertPath := fmt.Sprintf("%s%s/%v/%v/%v/", rootPath, "insert_log", segmentBackupInfo.GetCollectionId(), segmentBackupInfo.GetPartitionId(), segmentBackupInfo.GetSegmentId())
	log.Debug("insertPath", zap.String("bucket", b.milvusBucketName), zap.String("insertPath", insertPath))
	fieldsLogDir, _, err := b.getMilvusStorageClient().ListWithPrefix(ctx, b.milvusBucketName, insertPath, false)
	// handle segment level
	isL0 := false
	if len(fieldsLogDir) == 0 {
		isL0 = true
	}
	if err != nil {
		log.Error("Fail to list segment path", zap.String("insertPath", insertPath), zap.Error(err))
		return err
	}
	log.Debug("fieldsLogDir", zap.String("bucket", b.milvusBucketName), zap.Any("fieldsLogDir", fieldsLogDir))
	insertLogs := make([]*backuppb.FieldBinlog, 0)
	for _, fieldLogDir := range fieldsLogDir {
		binlogPaths, sizes, _ := b.getMilvusStorageClient().ListWithPrefix(ctx, b.milvusBucketName, fieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(fieldLogDir, insertPath, "", 1), meta.SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*backuppb.Binlog, 0)
		for index, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &backuppb.Binlog{
				LogPath: binlogPath,
				LogSize: sizes[index],
			})
			size += sizes[index]
		}
		insertLogs = append(insertLogs, &backuppb.FieldBinlog{
			FieldID: fieldId,
			Binlogs: binlogs,
		})
	}

	deltaLogPath := fmt.Sprintf("%s%s/%v/%v/%v/", rootPath, "delta_log", segmentBackupInfo.GetCollectionId(), segmentBackupInfo.GetPartitionId(), segmentBackupInfo.GetSegmentId())
	deltaFieldsLogDir, _, _ := b.getMilvusStorageClient().ListWithPrefix(ctx, b.milvusBucketName, deltaLogPath, false)
	deltaLogs := make([]*backuppb.FieldBinlog, 0)
	for _, deltaFieldLogDir := range deltaFieldsLogDir {
		binlogPaths, sizes, _ := b.getMilvusStorageClient().ListWithPrefix(ctx, b.milvusBucketName, deltaFieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(deltaFieldLogDir, deltaLogPath, "", 1), meta.SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*backuppb.Binlog, 0)
		for index, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &backuppb.Binlog{
				LogPath: binlogPath,
				LogSize: sizes[index],
			})
			size += sizes[index]
		}
		deltaLogs = append(deltaLogs, &backuppb.FieldBinlog{
			FieldID: fieldId,
			Binlogs: binlogs,
		})
	}
	if len(deltaLogs) == 0 {
		deltaLogs = append(deltaLogs, &backuppb.FieldBinlog{FieldID: 0})
	}

	segmentBackupInfo.Size = size
	segmentBackupInfo.IsL0 = isL0
	b.meta.UpdateSegment(segmentBackupInfo.GetPartitionId(), segmentBackupInfo.GetSegmentId(), meta.SetSegmentBinlogs(insertLogs), meta.SetSegmentDeltaBinlogs(deltaLogs), meta.SetSegmentSize(size), meta.SetSegmentL0(isL0))
	log.Debug("fill segment info", zap.Int64("segId", segmentBackupInfo.GetSegmentId()), zap.Int64("size", size))
	return nil
}

func (b *BackupContext) backupRBAC(ctx context.Context, backupInfo *backuppb.BackupInfo) error {
	log.Info("backup RBAC")
	resp, err := b.getMilvusClient().BackupRBAC(ctx)
	if err != nil {
		log.Error("fail in BackupMeta", zap.Error(err))
		return err
	}
	rbacMeta := resp.RBACMeta

	users := make([]*backuppb.UserInfo, 0, len(rbacMeta.Users))
	roles := make([]*backuppb.RoleEntity, 0, len(rbacMeta.Roles))
	grants := make([]*backuppb.GrantEntity, 0, len(rbacMeta.Grants))
	for _, user := range rbacMeta.Users {
		rs := lo.Map(user.Roles, func(role *milvuspb.RoleEntity, index int) *backuppb.RoleEntity {
			return &backuppb.RoleEntity{Name: role.Name}
		})
		userP := &backuppb.UserInfo{
			User:     user.User,
			Password: user.Password,
			Roles:    rs,
		}
		users = append(users, userP)
	}

	for _, role := range rbacMeta.Roles {
		roleP := &backuppb.RoleEntity{
			Name: role.Name,
		}
		roles = append(roles, roleP)
	}

	for _, roleGrant := range rbacMeta.Grants {
		roleGrantP := &backuppb.GrantEntity{
			Role: &backuppb.RoleEntity{
				Name: roleGrant.Role.Name,
			},
			Object: &backuppb.ObjectEntity{
				Name: roleGrant.Object.Name,
			},
			ObjectName: roleGrant.ObjectName,
			Grantor: &backuppb.GrantorEntity{
				User: &backuppb.UserEntity{
					Name: roleGrant.Grantor.User.Name,
				},
				Privilege: &backuppb.PrivilegeEntity{
					Name: roleGrant.Grantor.Privilege.Name,
				},
			},
			DbName: roleGrant.DbName,
		}
		grants = append(grants, roleGrantP)
	}

	rbacPb := &backuppb.RBACMeta{
		Users:  users,
		Roles:  roles,
		Grants: grants,
	}

	log.Info("backup RBAC", zap.Int("users", len(users)), zap.Int("roles", len(roles)), zap.Int("grants", len(grants)))
	b.meta.UpdateBackup(backupInfo.Id, meta.SetRBACMeta(rbacPb))
	return nil
}
