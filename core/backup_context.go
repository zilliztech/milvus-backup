package core

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"

	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"
)

const (
	BULKINSERT_TIMEOUT        = 60 * 60
	BULKINSERT_SLEEP_INTERVAL = 5
	BACKUP_NAME               = "BACKUP_NAME"
	COLLECTION_RENAME_SUFFIX  = "COLLECTION_RENAME_SUFFIX"
	WORKER_NUM                = 100
	RPS                       = 1000
)

// makes sure BackupContext implements `Backup`
var _ Backup = (*BackupContext)(nil)

type BackupContext struct {
	ctx context.Context
	// lock to make sure only one backup is creating or restoring
	mu      sync.Mutex
	started bool
	params  paramtable.BackupParams

	// milvus client
	milvusClient gomilvus.Client

	// data storage client
	storageClient    storage.ChunkManager
	milvusBucketName string
	backupBucketName string
	milvusRootPath   string
	backupRootPath   string

	idGenerator utils.IdGenerator

	backupNameIdDict sync.Map //map[string]string
	backupTasks      sync.Map //map[string]*backuppb.BackupInfo

	restoreTasks map[string]*backuppb.RestoreBackupTask
}

func CreateMilvusClient(ctx context.Context, params paramtable.BackupParams) (gomilvus.Client, error) {
	milvusEndpoint := params.MilvusCfg.Address + ":" + params.MilvusCfg.Port
	log.Debug("Start Milvus client", zap.String("endpoint", milvusEndpoint))
	var c gomilvus.Client
	var err error
	if params.MilvusCfg.AuthorizationEnabled && params.MilvusCfg.User != "" && params.MilvusCfg.Password != "" {
		if params.MilvusCfg.TLSMode == 0 {
			c, err = gomilvus.NewDefaultGrpcClientWithAuth(ctx, milvusEndpoint, params.MilvusCfg.User, params.MilvusCfg.Password)
		} else if params.MilvusCfg.TLSMode == 1 || params.MilvusCfg.TLSMode == 2 {
			c, err = gomilvus.NewDefaultGrpcClientWithTLSAuth(ctx, milvusEndpoint, params.MilvusCfg.User, params.MilvusCfg.Password)
		} else {
			log.Error("milvus.TLSMode is not illegal, support value 0, 1, 2")
			return nil, errors.New("milvus.TLSMode is not illegal, support value 0, 1, 2")
		}
	} else {
		c, err = gomilvus.NewGrpcClient(ctx, milvusEndpoint)
	}
	if err != nil {
		log.Error("failed to connect to milvus", zap.Error(err))
		return nil, err
	}
	return c, nil
}

func CreateStorageClient(ctx context.Context, params paramtable.BackupParams) (storage.ChunkManager, error) {
	minioEndPoint := params.MinioCfg.Address + ":" + params.MinioCfg.Port
	log.Debug("Start minio client",
		zap.String("address", minioEndPoint),
		zap.String("bucket", params.MinioCfg.BucketName),
		zap.String("backupBucket", params.MinioCfg.BackupBucketName))
	minioClient, err := storage.NewMinioChunkManager(ctx,
		storage.Address(minioEndPoint),
		storage.AccessKeyID(params.MinioCfg.AccessKeyID),
		storage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey),
		storage.UseSSL(params.MinioCfg.UseSSL),
		storage.BucketName(params.MinioCfg.BackupBucketName),
		storage.RootPath(params.MinioCfg.RootPath),
		storage.CloudProvider(params.MinioCfg.CloudProvider),
		storage.UseIAM(params.MinioCfg.UseIAM),
		storage.IAMEndpoint(params.MinioCfg.IAMEndpoint),
		storage.CreateBucket(true),
	)
	return minioClient, err
}

func (b *BackupContext) Start() error {
	// start milvus go SDK client
	milvusClient, err := CreateMilvusClient(b.ctx, b.params)
	if err != nil {
		log.Error("failed to initial milvus client", zap.Error(err))
		return err
	}
	b.milvusClient = milvusClient

	// start milvus storage client
	minioClient, err := CreateStorageClient(b.ctx, b.params)
	if err != nil {
		log.Error("failed to initial storage client", zap.Error(err))
		return err
	}
	b.storageClient = minioClient

	// init id generator to alloc id to tasks
	b.idGenerator = utils.NewFlakeIdGenerator()
	b.backupTasks = sync.Map{}
	b.backupNameIdDict = sync.Map{}
	b.restoreTasks = make(map[string]*backuppb.RestoreBackupTask)
	b.started = true
	return nil
}

func (b *BackupContext) Close() error {
	b.started = false
	err := b.milvusClient.Close()
	return err
}

func CreateBackupContext(ctx context.Context, params paramtable.BackupParams) *BackupContext {
	return &BackupContext{
		ctx:              ctx,
		params:           params,
		milvusBucketName: params.MinioCfg.BucketName,
		backupBucketName: params.MinioCfg.BackupBucketName,
		milvusRootPath:   params.MinioCfg.RootPath,
		backupRootPath:   params.MinioCfg.BackupRootPath,
	}
}

func (b BackupContext) CreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest) *backuppb.BackupInfoResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive CreateBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.Bool("async", request.GetAsync()))

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
	if request.GetBackupName() != "" {
		exist, err := b.storageClient.Exist(b.ctx, b.backupBucketName, b.backupRootPath+SEPERATOR+request.GetBackupName())
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
	err := utils.ValidateType(request.GetBackupName(), BACKUP_NAME)
	if err != nil {
		log.Error("illegal backup name", zap.Error(err))
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = err.Error()
		return resp
	}

	id := utils.UUID()
	var name string
	if request.GetBackupName() == "" {
		name = "backup_" + fmt.Sprint(time.Now().Unix())
	} else {
		name = request.BackupName
	}

	milvusVersion, err := b.milvusClient.GetVersion(b.ctx)
	if err != nil {
		log.Error("fail to get milvus version", zap.Error(err))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}
	//support, err := utils.IsSupportVersion(milvusVersion)
	//if err != nil {
	//	log.Error("check milvus version fail", zap.String("milvusVersion", milvusVersion), zap.Error(err))
	//	resp.Code = backuppb.ResponseCode_Fail
	//	resp.Msg = err.Error()
	//	return resp
	//}
	//if !support {
	//	msg := utils.NotSupportVersionMsg
	//	log.Error(msg, zap.String("version", milvusVersion))
	//	resp.Code = backuppb.ResponseCode_Fail
	//	resp.Msg = msg
	//	return resp
	//}
	backup := &backuppb.BackupInfo{
		Id:            id,
		StateCode:     backuppb.BackupTaskStateCode_BACKUP_INITIAL,
		StartTime:     time.Now().UnixNano() / int64(time.Millisecond),
		Name:          name,
		MilvusVersion: milvusVersion,
	}
	b.backupTasks.Store(id, backup)
	b.backupNameIdDict.Store(name, id)

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
		task, err := b.executeCreateBackup(ctx, request, backup)
		resp.Data = task
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

func (b BackupContext) executeCreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest, backupInfo *backuppb.BackupInfo) (*backuppb.BackupInfo, error) {
	// rethink about the lock logic
	b.mu.Lock()
	defer b.mu.Unlock()

	id := backupInfo.GetId()

	refreshBackupMetaFunc := func(id string, leveledBackupInfo *LeveledBackupInfo) (*backuppb.BackupInfo, error) {
		log.Debug("call refreshBackupMetaFunc", zap.String("id", id))
		backup, err := levelToTree(leveledBackupInfo)
		if err != nil {
			return backupInfo, err
		}
		b.backupTasks.Store(id, backupInfo)
		return backup, nil
	}

	// timestamp
	backupInfo.BackupTimestamp = uint64(time.Now().UnixNano() / int64(time.Millisecond))
	backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_EXECUTING
	leveledBackupInfo := &LeveledBackupInfo{
		backupLevel: backupInfo,
	}
	defer refreshBackupMetaFunc(id, leveledBackupInfo)

	// 1, get collection level meta
	log.Debug("Request collection names",
		zap.Strings("request_collection_names", request.GetCollectionNames()),
		zap.Int("length", len(request.GetCollectionNames())))

	var toBackupCollections []*entity.Collection
	if request.GetCollectionNames() == nil || len(request.GetCollectionNames()) == 0 {
		collections, err := b.milvusClient.ListCollections(b.ctx)
		if err != nil {
			log.Error("fail in ListCollections", zap.Error(err))
			return backupInfo, err
		}
		log.Debug(fmt.Sprintf("List %v collections", len(collections)))
		toBackupCollections = collections
	} else {
		//toBackupCollections := make([]*entity.Collection, 0)
		for _, collectionName := range request.GetCollectionNames() {
			exist, err := b.milvusClient.HasCollection(b.ctx, collectionName)
			if err != nil {
				log.Error("fail in HasCollection", zap.Error(err))
				return backupInfo, err
			}
			if !exist {
				errMsg := fmt.Sprintf("request backup collection does not exist: %s", collectionName)
				log.Error(errMsg)
				return backupInfo, err
			}
			collection, err := b.milvusClient.DescribeCollection(b.ctx, collectionName)
			if err != nil {
				log.Error("fail in DescribeCollection", zap.Error(err))
				return backupInfo, err
			}
			toBackupCollections = append(toBackupCollections, collection)
		}
	}
	log.Info("collections to backup", zap.Any("collections", toBackupCollections))

	collectionBackupInfos := make([]*backuppb.CollectionBackupInfo, 0)
	partitionLevelBackupInfos := make([]*backuppb.PartitionBackupInfo, 0)
	for _, collection := range toBackupCollections {
		// list collection result is not complete
		completeCollection, err := b.milvusClient.DescribeCollection(b.ctx, collection.Name)
		if err != nil {
			log.Error("fail in DescribeCollection", zap.Error(err))
			return backupInfo, err
		}
		fields := make([]*backuppb.FieldSchema, 0)
		for _, field := range completeCollection.Schema.Fields {
			fields = append(fields, &backuppb.FieldSchema{
				FieldID:      field.ID,
				Name:         field.Name,
				IsPrimaryKey: field.PrimaryKey,
				Description:  field.Description,
				AutoID:       field.AutoID,
				DataType:     backuppb.DataType(field.DataType),
				TypeParams:   utils.MapToKVPair(field.TypeParams),
				IndexParams:  utils.MapToKVPair(field.IndexParams),
			})
		}
		schema := &backuppb.CollectionSchema{
			Name:        completeCollection.Schema.CollectionName,
			Description: completeCollection.Schema.Description,
			AutoID:      completeCollection.Schema.AutoID,
			Fields:      fields,
		}

		hasIndex := false
		indexInfos := make([]*backuppb.IndexInfo, 0)
		indexDict := make(map[string]*backuppb.IndexInfo, 0)
		log.Info("try to get index",
			zap.String("collection_name", completeCollection.Name))
		for _, field := range completeCollection.Schema.Fields {
			//if field.DataType != entity.FieldTypeBinaryVector && field.DataType != entity.FieldTypeFloatVector {
			//	continue
			//}
			fieldIndex, err := b.milvusClient.DescribeIndex(b.ctx, completeCollection.Name, field.Name)
			if err != nil {
				if strings.HasPrefix(err.Error(), "index doesn't exist") {
					// todo
					log.Warn("field has no index",
						zap.String("collection_name", completeCollection.Name),
						zap.String("field_name", field.Name),
						zap.Error(err))
					continue
				} else {
					log.Error("fail in DescribeIndex", zap.Error(err))
					return backupInfo, err
				}
			}
			log.Info("field index",
				zap.String("collection_name", completeCollection.Name),
				zap.String("field_name", field.Name),
				zap.Any("index info", fieldIndex))
			for _, index := range fieldIndex {
				if _, ok := indexDict[index.Name()]; ok {
					continue
				} else {
					indexInfo := &backuppb.IndexInfo{
						FieldName: index.FieldName(),
						IndexName: index.Name(),
						IndexType: string(index.IndexType()),
						Params:    index.Params(),
					}
					indexInfos = append(indexInfos, indexInfo)
					indexDict[index.Name()] = indexInfo
				}
			}
		}

		collectionBackupId := utils.UUID()
		collectionBackup := &backuppb.CollectionBackupInfo{
			Id:               collectionBackupId,
			StateCode:        backuppb.BackupTaskStateCode_BACKUP_INITIAL,
			StartTime:        time.Now().Unix(),
			CollectionId:     completeCollection.ID,
			DbName:           "", // todo currently db_name is not used in many places
			CollectionName:   completeCollection.Name,
			Schema:           schema,
			ShardsNum:        completeCollection.ShardNum,
			ConsistencyLevel: backuppb.ConsistencyLevel(completeCollection.ConsistencyLevel),
			HasIndex:         hasIndex,
			IndexInfos:       indexInfos,
		}
		collectionBackupInfos = append(collectionBackupInfos, collectionBackup)
	}
	leveledBackupInfo.collectionLevel = &backuppb.CollectionLevelBackupInfo{
		Infos: collectionBackupInfos,
	}
	refreshBackupMetaFunc(id, leveledBackupInfo)

	segmentLevelBackupInfos := make([]*backuppb.SegmentBackupInfo, 0)
	// backup collection
	for _, collection := range collectionBackupInfos {
		partitionBackupInfos := make([]*backuppb.PartitionBackupInfo, 0)
		partitions, err := b.milvusClient.ShowPartitions(b.ctx, collection.GetCollectionName())
		if err != nil {
			log.Error("fail to ShowPartitions", zap.Error(err))
			return backupInfo, err
		}

		// use GetLoadingProgress currently, GetLoadState is a new interface @20230104  milvus pr#21515
		collectionLoadProgress, err := b.milvusClient.GetLoadingProgress(ctx, collection.GetCollectionName(), []string{})
		if err != nil {
			log.Error("fail to GetLoadingProgress of collection", zap.Error(err))
			return backupInfo, err
		}

		var collectionLoadState string
		partitionLoadStates := make(map[string]string, 0)
		if collectionLoadProgress == 0 {
			collectionLoadState = LoadState_NotLoad
			for _, partition := range partitions {
				partitionLoadStates[partition.Name] = LoadState_NotLoad
			}
		} else if collectionLoadProgress == 100 {
			collectionLoadState = LoadState_Loaded
			for _, partition := range partitions {
				partitionLoadStates[partition.Name] = LoadState_Loaded
			}
		} else {
			collectionLoadState = LoadState_Loading
			for _, partition := range partitions {
				loadProgress, err := b.milvusClient.GetLoadingProgress(ctx, collection.GetCollectionName(), []string{partition.Name})
				if err != nil {
					log.Error("fail to GetLoadingProgress of partition", zap.Error(err))
					return backupInfo, err
				}
				if loadProgress == 0 {
					partitionLoadStates[partition.Name] = LoadState_NotLoad
				} else if loadProgress == 100 {
					partitionLoadStates[partition.Name] = LoadState_Loaded
				} else {
					partitionLoadStates[partition.Name] = LoadState_Loading
				}
			}
		}

		// Flush
		segmentEntitiesBeforeFlush, err := b.milvusClient.GetPersistentSegmentInfo(ctx, collection.GetCollectionName())
		if err != nil {
			return backupInfo, err
		}
		log.Info("GetPersistentSegmentInfo before flush from milvus",
			zap.String("collectionName", collection.GetCollectionName()),
			zap.Int("segmentNumBeforeFlush", len(segmentEntitiesBeforeFlush)))

		newSealedSegmentIDs, flushedSegmentIDs, timeOfSeal, err := b.milvusClient.Flush(ctx, collection.GetCollectionName(), false)
		if err != nil {
			log.Error(fmt.Sprintf("fail to flush the collection: %s", collection.GetCollectionName()))
			return backupInfo, err
		}
		log.Info("flush segments",
			zap.String("collectionName", collection.GetCollectionName()),
			zap.Int64s("newSealedSegmentIDs", newSealedSegmentIDs),
			zap.Int64s("flushedSegmentIDs", flushedSegmentIDs),
			zap.Int64("timeOfSeal", timeOfSeal))
		collection.BackupTimestamp = utils.ComposeTS(timeOfSeal, 0)
		collection.BackupPhysicalTimestamp = uint64(timeOfSeal)

		flushSegmentIDs := append(newSealedSegmentIDs, flushedSegmentIDs...)
		segmentEntitiesAfterFlush, err := b.milvusClient.GetPersistentSegmentInfo(ctx, collection.GetCollectionName())
		if err != nil {
			return backupInfo, err
		}
		log.Info("GetPersistentSegmentInfo after flush from milvus",
			zap.String("collectionName", collection.GetCollectionName()),
			zap.Int("segmentNumBeforeFlush", len(segmentEntitiesBeforeFlush)),
			zap.Int("segmentNumAfterFlush", len(segmentEntitiesAfterFlush)))

		// fill segments
		filledSegments := make([]*entity.Segment, 0)
		segmentDict := utils.ArrayToMap(flushSegmentIDs)
		for _, seg := range segmentEntitiesAfterFlush {
			sid := seg.ID
			if _, ok := segmentDict[sid]; ok {
				delete(segmentDict, sid)
				filledSegments = append(filledSegments, seg)
			} else {
				log.Warn("this may be new segments after flush, skip it", zap.Int64("id", sid))
			}
		}
		for _, seg := range segmentEntitiesBeforeFlush {
			sid := seg.ID
			if _, ok := segmentDict[sid]; ok {
				delete(segmentDict, sid)
				filledSegments = append(filledSegments, seg)
			} else {
				log.Warn("this may be old segments before flush, skip it", zap.Int64("id", sid))
			}
		}
		if len(segmentDict) > 0 {
			// very rare situation, segments return in flush doesn't exist in either segmentEntitiesBeforeFlush and segmentEntitiesAfterFlush
			errorMsg := "Segment return in Flush not exist in GetPersistentSegmentInfo. segment ids: " + fmt.Sprint(utils.MapKeyArray(segmentDict))
			log.Warn(errorMsg)
		}

		if err != nil {
			collection.StateCode = backuppb.BackupTaskStateCode_BACKUP_FAIL
			collection.ErrorMessage = err.Error()
			return backupInfo, err
		}
		log.Info("Finished fill segment",
			zap.String("collectionName", collection.GetCollectionName()))

		segmentBackupInfos := make([]*backuppb.SegmentBackupInfo, 0)
		partSegInfoMap := make(map[int64][]*backuppb.SegmentBackupInfo)
		for _, segment := range filledSegments {
			segmentInfo, err := b.readSegmentInfo(ctx, segment.CollectionID, segment.ParititionID, segment.ID, segment.NumRows)
			if err != nil {
				return backupInfo, err
			}
			if len(segmentInfo.Binlogs) == 0 {
				log.Warn("this segment has no insert binlog", zap.Int64("id", segment.ID))
			}
			partSegInfoMap[segment.ParititionID] = append(partSegInfoMap[segment.ParititionID], segmentInfo)
			segmentBackupInfos = append(segmentBackupInfos, segmentInfo)
			segmentLevelBackupInfos = append(segmentLevelBackupInfos, segmentInfo)
		}
		log.Info("readSegmentInfo from storage",
			zap.String("collectionName", collection.GetCollectionName()),
			zap.Int("segmentNum", len(filledSegments)))

		leveledBackupInfo.segmentLevel = &backuppb.SegmentLevelBackupInfo{
			Infos: segmentLevelBackupInfos,
		}

		for _, partition := range partitions {
			partitionSegments := partSegInfoMap[partition.ID]
			var size int64 = 0
			for _, seg := range partitionSegments {
				size += seg.GetSize()
			}
			partitionBackupInfo := &backuppb.PartitionBackupInfo{
				PartitionId:    partition.ID,
				PartitionName:  partition.Name,
				CollectionId:   collection.GetCollectionId(),
				SegmentBackups: partSegInfoMap[partition.ID],
				Size:           size,
				LoadState:      partitionLoadStates[partition.Name],
			}
			partitionBackupInfos = append(partitionBackupInfos, partitionBackupInfo)
			partitionLevelBackupInfos = append(partitionLevelBackupInfos, partitionBackupInfo)
		}

		leveledBackupInfo.partitionLevel = &backuppb.PartitionLevelBackupInfo{
			Infos: partitionLevelBackupInfos,
		}
		collection.PartitionBackups = partitionBackupInfos
		collection.LoadState = collectionLoadState
		refreshBackupMetaFunc(id, leveledBackupInfo)
		log.Info("finish build partition info",
			zap.String("collectionName", collection.GetCollectionName()),
			zap.Int("partitionNum", len(partitionBackupInfos)))

		log.Info("Begin copy data",
			zap.String("collectionName", collection.GetCollectionName()),
			zap.Int("segmentNum", len(segmentBackupInfos)))

		for _, part := range partitionBackupInfos {
			if part.GetSize() > b.params.BackupCfg.MaxSegmentGroupSize {
				log.Info("partition size is larger than MaxSegmentGroupSize, will separate segments into groups in backup files",
					zap.Int64("collectionId", part.GetCollectionId()),
					zap.Int64("partitionId", part.GetPartitionId()),
					zap.Int64("partitionSize", part.GetSize()),
					zap.Int64("MaxSegmentGroupSize", b.params.BackupCfg.MaxSegmentGroupSize))
				segments := partSegInfoMap[part.GetPartitionId()]
				var bufferSize int64 = 0
				// 0 is illegal value, start from 1
				var segGroupID int64 = 1
				for _, seg := range segments {
					if seg.Size > b.params.BackupCfg.MaxSegmentGroupSize && bufferSize == 0 {
						seg.GroupId = segGroupID
						segGroupID = segGroupID + 1
					} else if bufferSize+seg.Size > b.params.BackupCfg.MaxSegmentGroupSize {
						segGroupID = segGroupID + 1
						seg.GroupId = segGroupID
						bufferSize = 0
						bufferSize = bufferSize + seg.Size
					} else {
						seg.GroupId = segGroupID
						bufferSize = bufferSize + seg.Size
					}
				}
			} else {
				log.Info("partition size is smaller than MaxSegmentGroupSize, won't separate segments into groups in backup files",
					zap.Int64("collectionId", part.GetCollectionId()),
					zap.Int64("partitionId", part.GetPartitionId()),
					zap.Int64("partitionSize", part.GetSize()),
					zap.Int64("MaxSegmentGroupSize", b.params.BackupCfg.MaxSegmentGroupSize))
			}
		}

		wp := common.NewWorkerPool(ctx, WORKER_NUM, RPS)
		wp.Start()
		for _, segment := range segmentBackupInfos {
			start := time.Now().Unix()
			log.Debug("copy segment",
				zap.Int64("collection_id", segment.GetCollectionId()),
				zap.Int64("partition_id", segment.GetPartitionId()),
				zap.Int64("segment_id", segment.GetSegmentId()),
				zap.Int64("group_id", segment.GetGroupId()))
			// insert log
			for _, binlogs := range segment.GetBinlogs() {
				for _, binlog := range binlogs.GetBinlogs() {
					// generate target path
					// milvus_rootpath/insert_log/collection_id/partition_id/segment_id/ =>
					// backup_rootpath/backup_name/insert_log/collection_id/partition_id/group_id/segment_id
					targetPath := strings.Replace(binlog.GetLogPath(),
						b.milvusRootPath,
						BackupBinlogDirPath(b.backupRootPath, backupInfo.GetName()),
						1)
					if segment.GetGroupId() != 0 {
						targetPath = strings.Replace(targetPath,
							strconv.FormatInt(segment.GetPartitionId(), 10),
							strconv.FormatInt(segment.GetPartitionId(), 10)+"/"+strconv.FormatInt(segment.GetGroupId(), 10),
							1)
					}
					if targetPath == binlog.GetLogPath() {
						log.Error("wrong target path",
							zap.String("from", binlog.GetLogPath()),
							zap.String("to", targetPath))
						return backupInfo, err
					}

					binlog := binlog
					job := func(ctx context.Context) error {
						exist, err := b.storageClient.Exist(ctx, b.milvusBucketName, binlog.GetLogPath())
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

						err = b.storageClient.Copy(ctx, b.milvusBucketName, b.backupBucketName, binlog.GetLogPath(), targetPath)
						if err != nil {
							log.Info("Fail to copy file",
								zap.Error(err),
								zap.String("from", binlog.GetLogPath()),
								zap.String("to", targetPath))
							return err
						} else {
							log.Debug("Successfully copy file",
								zap.String("from", binlog.GetLogPath()),
								zap.String("to", targetPath))
						}

						return nil
					}
					wp.Submit(job)
				}
			}
			// delta log
			for _, binlogs := range segment.GetDeltalogs() {
				for _, binlog := range binlogs.GetBinlogs() {
					// generate target path
					// milvus_rootpath/delta_log/collection_id/partition_id/segment_id/ =>
					// backup_rootpath/backup_name/delta_log/collection_id/partition_id/group_id/segment_id
					targetPath := strings.Replace(binlog.GetLogPath(),
						b.milvusRootPath,
						BackupBinlogDirPath(b.backupRootPath, backupInfo.GetName()),
						1)
					if segment.GetGroupId() != 0 {
						targetPath = strings.Replace(targetPath,
							strconv.FormatInt(segment.GetPartitionId(), 10),
							strconv.FormatInt(segment.GetPartitionId(), 10)+"/"+strconv.FormatInt(segment.GetGroupId(), 10),
							1)
					}
					if targetPath == binlog.GetLogPath() {
						log.Error("wrong target path",
							zap.String("from", binlog.GetLogPath()),
							zap.String("to", targetPath))
						return backupInfo, err
					}

					binlog := binlog
					job := func(ctx context.Context) error {
						exist, err := b.storageClient.Exist(ctx, b.milvusBucketName, binlog.GetLogPath())
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
						err = b.storageClient.Copy(ctx, b.milvusBucketName, b.backupBucketName, binlog.GetLogPath(), targetPath)
						if err != nil {
							log.Info("Fail to copy file",
								zap.Error(err),
								zap.String("from", binlog.GetLogPath()),
								zap.String("to", targetPath))
							return err
						} else {
							log.Info("Successfully copy file",
								zap.String("from", binlog.GetLogPath()),
								zap.String("to", targetPath))
						}
						return err
					}
					wp.Submit(job)
				}
			}
			duration := time.Now().Unix() - start
			log.Debug("copy segment finished",
				zap.Int64("collection_id", segment.GetCollectionId()),
				zap.Int64("partition_id", segment.GetPartitionId()),
				zap.Int64("segment_id", segment.GetSegmentId()),
				zap.Int64("cost_time", duration))
		}
		wp.Done()
		if err := wp.Wait(); err != nil {
			return backupInfo, err
		}
		refreshBackupMetaFunc(id, leveledBackupInfo)
	}

	var backupSize int64 = 0
	for _, coll := range leveledBackupInfo.collectionLevel.GetInfos() {
		backupSize += coll.GetSize()
	}
	backupInfo.Size = backupSize
	backupInfo, err := refreshBackupMetaFunc(id, leveledBackupInfo)
	if err != nil {
		return backupInfo, err
	}
	backupInfo.EndTime = time.Now().UnixNano() / int64(time.Millisecond)
	backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_SUCCESS
	// 7, write meta data
	output, _ := serialize(backupInfo)
	log.Debug("backup meta", zap.String("value", string(output.BackupMetaBytes)))
	log.Debug("collection meta", zap.String("value", string(output.CollectionMetaBytes)))
	log.Debug("partition meta", zap.String("value", string(output.PartitionMetaBytes)))
	log.Debug("segment meta", zap.String("value", string(output.SegmentMetaBytes)))

	b.storageClient.Write(ctx, b.backupBucketName, BackupMetaPath(b.backupRootPath, backupInfo.GetName()), output.BackupMetaBytes)
	b.storageClient.Write(ctx, b.backupBucketName, CollectionMetaPath(b.backupRootPath, backupInfo.GetName()), output.CollectionMetaBytes)
	b.storageClient.Write(ctx, b.backupBucketName, PartitionMetaPath(b.backupRootPath, backupInfo.GetName()), output.PartitionMetaBytes)
	b.storageClient.Write(ctx, b.backupBucketName, SegmentMetaPath(b.backupRootPath, backupInfo.GetName()), output.SegmentMetaBytes)

	log.Info("finish executeCreateBackup",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.Bool("async", request.GetAsync()),
		zap.String("backup meta", string(output.BackupMetaBytes)))
	return backupInfo, nil
}

func (b BackupContext) GetBackup(ctx context.Context, request *backuppb.GetBackupRequest) *backuppb.BackupInfoResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive GetBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.String("backupId", request.GetBackupId()),
		zap.String("bucketName", request.GetBucketName()),
		zap.String("path", request.GetPath()))

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

	if request.GetBackupId() == "" && request.GetBackupName() == "" {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "empty backup name and backup id"
		return resp
	}

	if request.GetBackupId() != "" {
		if value, ok := b.backupTasks.Load(request.GetBackupId()); ok {
			resp.Code = backuppb.ResponseCode_Success
			resp.Msg = "success"
			resp.Data = value.(*backuppb.BackupInfo)
			return resp
		}
	}

	if request.GetBackupName() != "" {
		if id, ok := b.backupNameIdDict.Load(request.GetBackupName()); ok {
			resp.Code = backuppb.ResponseCode_Success
			resp.Msg = "success"
			backup, ok := b.backupTasks.Load(id)
			if ok {
				resp.Data = backup.(*backuppb.BackupInfo)
			}
			return resp
		} else {
			var backupBucketName string
			var backupPath string
			if request.GetBucketName() == "" || request.GetPath() == "" {
				backupBucketName = b.backupBucketName
				backupPath = b.backupRootPath + SEPERATOR + request.GetBackupName()
			} else {
				backupBucketName = request.GetBucketName()
				backupPath = request.GetPath() + SEPERATOR + request.GetBackupName()
			}
			backup, err := b.readBackup(ctx, backupBucketName, backupPath)
			if err != nil {
				log.Warn("Fail to read backup",
					zap.String("backupBucketName", backupBucketName),
					zap.String("backupPath", backupPath),
					zap.Error(err))
				resp.Code = backuppb.ResponseCode_Fail
				resp.Msg = err.Error()
				return resp
			}

			resp.Data = backup
			if backup == nil {
				resp.Code = backuppb.ResponseCode_Request_Object_Not_Found
				resp.Msg = "not found"
			} else {
				resp.Code = backuppb.ResponseCode_Success
				resp.Msg = "success"
			}
		}
	}

	log.Info("finish GetBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.String("backupId", request.GetBackupId()),
		zap.String("bucketName", request.GetBucketName()),
		zap.String("path", request.GetPath()),
		zap.Any("resp", resp))
	return resp
}

func (b BackupContext) ListBackups(ctx context.Context, request *backuppb.ListBackupsRequest) *backuppb.ListBackupsResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive ListBackupsRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("collectionName", request.GetCollectionName()))

	resp := &backuppb.ListBackupsResponse{
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

	// 1, trigger inner sync to get the newest backup list in the milvus cluster
	backupPaths, _, err := b.storageClient.ListWithPrefix(ctx, b.backupBucketName, b.backupRootPath+SEPERATOR, false)
	if err != nil {
		log.Error("Fail to list backup directory", zap.Error(err))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	log.Info("List Backups' path", zap.Strings("backup_paths", backupPaths))
	backupInfos := make([]*backuppb.BackupInfo, 0)
	backupNames := make([]string, 0)
	for _, backupPath := range backupPaths {
		backupResp := b.GetBackup(ctx, &backuppb.GetBackupRequest{
			BackupName: BackupPathToName(b.backupRootPath, backupPath),
		})
		if backupResp.GetCode() != backuppb.ResponseCode_Success {
			log.Warn("Fail to read backup",
				zap.String("path", backupPath),
				zap.String("error", backupResp.GetMsg()))
			// ignore get failed
			continue
			//resp.Code = backuppb.ResponseCode_Fail
			//resp.Msg = backupResp.Msg
			//return resp
		}

		// 2, list wanted backup
		if backupResp.GetData() != nil {
			if request.GetCollectionName() != "" {
				// if request.GetCollectionName() is defined only return backups contains the certain collection
				for _, collectionMeta := range backupResp.GetData().GetCollectionBackups() {
					if collectionMeta.GetCollectionName() == request.GetCollectionName() {
						backupInfos = append(backupInfos, backupResp.GetData())
						backupNames = append(backupNames, backupResp.GetData().GetName())
					}
				}
			} else {
				backupInfos = append(backupInfos, backupResp.GetData())
				backupNames = append(backupNames, backupResp.GetData().GetName())
			}
		}
	}

	// 3, return
	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = backupInfos
	log.Info("return ListBackupsResponse",
		zap.String("requestId", resp.GetRequestId()),
		zap.Int32("code", int32(resp.GetCode())),
		zap.String("msg", resp.GetMsg()),
		zap.Strings("data: list_backup_names", backupNames))
	return resp
}

func (b BackupContext) DeleteBackup(ctx context.Context, request *backuppb.DeleteBackupRequest) *backuppb.DeleteBackupResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive DeleteBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()))

	resp := &backuppb.DeleteBackupResponse{
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

	if request.GetBackupName() == "" {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "empty backup name"
		return resp
	}

	getResp := b.GetBackup(b.ctx, &backuppb.GetBackupRequest{
		BackupName: request.GetBackupName(),
	})
	if getResp.GetCode() == backuppb.ResponseCode_Request_Object_Not_Found {
		resp.Code = backuppb.ResponseCode_Request_Object_Not_Found
		resp.Msg = getResp.GetMsg()
		return resp
	} else if getResp.GetCode() != backuppb.ResponseCode_Success {
		log.Error("fail in GetBackup", zap.String("msg", getResp.GetMsg()))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = getResp.GetMsg()
		return resp
	} else if getResp.GetData() == nil {
		errMsg := fmt.Sprintf("backup does not exist: %s", request.GetBackupName())
		log.Warn(errMsg)
		resp.Code = backuppb.ResponseCode_Request_Object_Not_Found
		resp.Msg = errMsg
		return resp
	}

	err := b.storageClient.RemoveWithPrefix(ctx, b.backupBucketName, BackupDirPath(b.backupRootPath, request.GetBackupName()))

	if err != nil {
		log.Error("Fail to delete backup", zap.String("backupName", request.GetBackupName()), zap.Error(err))
		return nil
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	log.Info("return DeleteBackupResponse",
		zap.String("requestId", resp.GetRequestId()),
		zap.Int32("code", int32(resp.GetCode())))
	return resp
}

func (b BackupContext) RestoreBackup(ctx context.Context, request *backuppb.RestoreBackupRequest) *backuppb.RestoreBackupResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive RestoreBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.String("CollectionSuffix", request.GetCollectionSuffix()),
		zap.Any("CollectionRenames", request.GetCollectionRenames()),
		zap.Bool("async", request.GetAsync()),
		zap.String("bucketName", request.GetBucketName()),
		zap.String("path", request.GetPath()))

	resp := &backuppb.RestoreBackupResponse{
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

	//milvusVersion, err := b.milvusClient.GetVersion(b.ctx)
	//if err != nil {
	//	log.Error("fail to get milvus version", zap.Error(err))
	//	resp.Code = backuppb.ResponseCode_Fail
	//	resp.Msg = err.Error()
	//	return resp
	//}
	//support, err := utils.IsSupportVersion(milvusVersion)
	//if err != nil {
	//	log.Error("check milvus version fail", zap.String("milvusVersion", milvusVersion), zap.Error(err))
	//	resp.Code = backuppb.ResponseCode_Fail
	//	resp.Msg = err.Error()
	//	return resp
	//}
	//if !support {
	//	msg := utils.NotSupportVersionMsg
	//	log.Error(msg, zap.String("version", milvusVersion))
	//	resp.Code = backuppb.ResponseCode_Fail
	//	resp.Msg = msg
	//	return resp
	//}

	// 1, get and validate
	if request.GetCollectionSuffix() != "" {
		err := utils.ValidateType(request.GetCollectionSuffix(), COLLECTION_RENAME_SUFFIX)
		if err != nil {
			log.Error("illegal collection rename suffix", zap.Error(err))
			resp.Code = backuppb.ResponseCode_Parameter_Error
			resp.Msg = "illegal collection rename suffix" + err.Error()
			return resp
		}
	}

	getResp := b.GetBackup(ctx, &backuppb.GetBackupRequest{
		BackupName: request.GetBackupName(),
		BucketName: request.GetBucketName(),
		Path:       request.GetPath(),
	})

	var backupBucketName string
	var backupPath string
	if request.GetBucketName() == "" || request.GetPath() == "" {
		backupBucketName = b.backupBucketName
		backupPath = b.backupRootPath + SEPERATOR + request.GetBackupName()
	} else {
		backupBucketName = request.GetBucketName()
		backupPath = request.GetPath() + SEPERATOR + request.GetBackupName()
	}

	if getResp.GetCode() != backuppb.ResponseCode_Success {
		log.Error("fail to get backup",
			zap.String("backupName", request.GetBackupName()),
			zap.String("msg", getResp.GetMsg()))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = getResp.GetMsg()
		return resp
	}
	if getResp.GetData() == nil {
		log.Error("backup doesn't exist", zap.String("backupName", request.GetBackupName()))
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "backup doesn't exist"
		return resp
	}

	backup := getResp.GetData()

	id := utils.UUID()

	task := &backuppb.RestoreBackupTask{
		Id:        id,
		StateCode: backuppb.RestoreTaskStateCode_INITIAL,
		StartTime: time.Now().Unix(),
		Progress:  0,
	}

	// 2, initial restoreCollectionTasks
	toRestoreCollectionBackups := make([]*backuppb.CollectionBackupInfo, 0)
	if len(request.GetCollectionNames()) == 0 {
		toRestoreCollectionBackups = backup.GetCollectionBackups()
	} else {
		collectionNameDict := make(map[string]bool)
		for _, collectName := range request.GetCollectionNames() {
			collectionNameDict[collectName] = true
		}
		for _, collectionBackup := range backup.GetCollectionBackups() {
			if collectionNameDict[collectionBackup.GetCollectionName()] {
				toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
			}
		}
	}
	log.Info("Collections to restore", zap.Int("collection_num", len(toRestoreCollectionBackups)))

	restoreCollectionTasks := make([]*backuppb.RestoreCollectionTask, 0)
	for _, restoreCollection := range toRestoreCollectionBackups {
		backupCollectionName := restoreCollection.GetSchema().GetName()
		var targetCollectionName string
		// rename collection, rename map has higher priority then suffix
		if len(request.GetCollectionRenames()) > 0 && request.GetCollectionRenames()[backupCollectionName] != "" {
			targetCollectionName = request.GetCollectionRenames()[backupCollectionName]
		} else if request.GetCollectionSuffix() != "" {
			targetCollectionName = backupCollectionName + request.GetCollectionSuffix()
		} else {
			targetCollectionName = backupCollectionName
		}

		exist, err := b.milvusClient.HasCollection(ctx, targetCollectionName)
		if err != nil {
			errorMsg := fmt.Sprintf("fail to check whether the collection is exist, collection_name: %s, err: %s", targetCollectionName, err)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
		}
		if exist {
			errorMsg := fmt.Sprintf("The collection to restore already exists, backupCollectName: %s, targetCollectionName: %s", backupCollectionName, targetCollectionName)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
		}

		var toRestoreSize int64 = 0
		for _, partitionBackup := range restoreCollection.GetPartitionBackups() {
			toRestoreSize += partitionBackup.GetSize()
		}
		id := utils.UUID()

		restoreCollectionTask := &backuppb.RestoreCollectionTask{
			Id:                    id,
			StateCode:             backuppb.RestoreTaskStateCode_INITIAL,
			StartTime:             time.Now().Unix(),
			CollBackup:            restoreCollection,
			TargetCollectionName:  targetCollectionName,
			PartitionRestoreTasks: []*backuppb.RestorePartitionTask{},
			ToRestoreSize:         toRestoreSize,
			RestoredSize:          0,
			Progress:              0,
		}
		restoreCollectionTasks = append(restoreCollectionTasks, restoreCollectionTask)
		task.CollectionRestoreTasks = restoreCollectionTasks
		task.ToRestoreSize = task.GetToRestoreSize() + toRestoreSize
	}

	if request.Async {
		go b.executeRestoreBackupTask(ctx, backupBucketName, backupPath, backup, task)
		asyncResp := &backuppb.RestoreBackupResponse{
			RequestId: request.GetRequestId(),
			Code:      backuppb.ResponseCode_Success,
			Msg:       "restore backup is executing asynchronously",
			Data:      task,
		}
		return asyncResp
	} else {
		endTask, err := b.executeRestoreBackupTask(ctx, backupBucketName, backupPath, backup, task)
		resp.Data = endTask
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			log.Error("execute restore collection fail", zap.String("backupId", backup.GetId()), zap.Error(err))
			resp.Msg = err.Error()
		} else {
			resp.Code = backuppb.ResponseCode_Success
			resp.Msg = "success"
		}
		return resp
	}
}

func (b BackupContext) executeRestoreBackupTask(ctx context.Context, backupBucketName string, backupPath string, backup *backuppb.BackupInfo, task *backuppb.RestoreBackupTask) (*backuppb.RestoreBackupTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := task.GetId()
	b.restoreTasks[id] = task
	task.StateCode = backuppb.RestoreTaskStateCode_EXECUTING

	log.Info("executeRestoreBackupTask start",
		zap.String("backup_name", backup.GetName()),
		zap.String("backupBucketName", backupBucketName),
		zap.String("backupPath", backupPath))
	updateRestoreTaskFunc := func(id string, task *backuppb.RestoreBackupTask) {
		b.restoreTasks[id] = task
	}
	defer updateRestoreTaskFunc(id, task)

	restoreCollectionTasks := task.GetCollectionRestoreTasks()

	// 3, execute restoreCollectionTasks
	for _, restoreCollectionTask := range restoreCollectionTasks {
		endTask, err := b.executeRestoreCollectionTask(ctx, backupBucketName, backupPath, restoreCollectionTask)
		if err != nil {
			log.Error("executeRestoreCollectionTask failed",
				zap.String("TargetCollectionName", restoreCollectionTask.GetTargetCollectionName()),
				zap.Error(err))
			return task, err
		}
		log.Info("finish restore collection", zap.String("collection_name", restoreCollectionTask.GetTargetCollectionName()))
		restoreCollectionTask.StateCode = backuppb.RestoreTaskStateCode_SUCCESS
		task.RestoredSize += endTask.RestoredSize
		if task.GetToRestoreSize() == 0 {
			task.Progress = 100
		} else {
			task.Progress = int32(100 * task.GetRestoredSize() / task.GetToRestoreSize())
		}
		updateRestoreTaskFunc(id, task)
	}

	task.StateCode = backuppb.RestoreTaskStateCode_SUCCESS
	task.EndTime = time.Now().Unix()
	return task, nil
}

func (b BackupContext) executeRestoreCollectionTask(ctx context.Context, backupBucketName string, backupPath string, task *backuppb.RestoreCollectionTask) (*backuppb.RestoreCollectionTask, error) {
	targetCollectionName := task.GetTargetCollectionName()
	task.StateCode = backuppb.RestoreTaskStateCode_EXECUTING
	log.Info("start restore",
		zap.String("collection_name", task.GetTargetCollectionName()),
		zap.String("backupBucketName", backupBucketName),
		zap.String("backupPath", backupPath))
	// create collection
	fields := make([]*entity.Field, 0)
	for _, field := range task.GetCollBackup().GetSchema().GetFields() {
		fields = append(fields, &entity.Field{
			ID:          field.GetFieldID(),
			Name:        field.GetName(),
			PrimaryKey:  field.GetIsPrimaryKey(),
			AutoID:      field.GetAutoID(),
			Description: field.GetDescription(),
			DataType:    entity.FieldType(field.GetDataType()),
			TypeParams:  utils.KvPairsMap(field.GetTypeParams()),
			IndexParams: utils.KvPairsMap(field.GetIndexParams()),
		})
	}

	collectionSchema := &entity.Schema{
		CollectionName: targetCollectionName,
		Description:    task.GetCollBackup().GetSchema().GetDescription(),
		AutoID:         task.GetCollBackup().GetSchema().GetAutoID(),
		Fields:         fields,
	}

	err := retry.Do(ctx, func() error {
		return b.milvusClient.CreateCollection(
			ctx,
			collectionSchema,
			task.GetCollBackup().GetShardsNum(),
			gomilvus.WithConsistencyLevel(entity.ConsistencyLevel(task.GetCollBackup().GetConsistencyLevel())))
	}, retry.Attempts(10), retry.Sleep(1*time.Second))

	if err != nil {
		errorMsg := fmt.Sprintf("fail to create collection, targetCollectionName: %s err: %s", targetCollectionName, err)
		log.Error(errorMsg)
		task.StateCode = backuppb.RestoreTaskStateCode_FAIL
		task.ErrorMessage = errorMsg
		return task, err
	}
	log.Info("create collection", zap.String("collectionName", targetCollectionName))

	for _, partitionBackup := range task.GetCollBackup().GetPartitionBackups() {
		exist, err := b.milvusClient.HasPartition(ctx, targetCollectionName, partitionBackup.GetPartitionName())
		if err != nil {
			log.Error("fail to check has partition", zap.Error(err))
			return task, err
		}
		if !exist {
			err = retry.Do(ctx, func() error {
				return b.milvusClient.CreatePartition(ctx, targetCollectionName, partitionBackup.GetPartitionName())
			}, retry.Attempts(10), retry.Sleep(1*time.Second))
			if err != nil {
				log.Error("fail to create partition", zap.Error(err))
				return task, err
			}
		}
		log.Info("create partition",
			zap.String("collectionName", targetCollectionName),
			zap.String("partitionName", partitionBackup.GetPartitionName()))

		// bulk insert
		groupIds := collectGroupIdsFromSegments(partitionBackup.GetSegmentBackups())

		if len(groupIds) == 1 && groupIds[0] == 0 {
			// backward compatible old backup without group id
			files, err := b.getBackupPartitionPaths(ctx, backupBucketName, backupPath, partitionBackup)
			if err != nil {
				log.Error("fail to get partition backup binlog files",
					zap.Error(err),
					zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
					zap.String("targetCollectionName", targetCollectionName),
					zap.String("partition", partitionBackup.GetPartitionName()))
				return task, err
			}
			err = b.executeBulkInsert(ctx, targetCollectionName, partitionBackup.GetPartitionName(), files, int64(task.GetCollBackup().BackupTimestamp))
			if err != nil {
				log.Error("fail to bulk insert to partition",
					zap.Error(err),
					zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
					zap.String("targetCollectionName", targetCollectionName),
					zap.String("partition", partitionBackup.GetPartitionName()))
				return task, err
			}
		} else {
			// bulk insert by segment groups
			for _, groupId := range groupIds {
				files, err := b.getBackupPartitionPathsWithGroupID(ctx, backupBucketName, backupPath, partitionBackup, groupId)
				if err != nil {
					log.Error("fail to get partition backup binlog files",
						zap.Error(err),
						zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
						zap.String("targetCollectionName", targetCollectionName),
						zap.String("partition", partitionBackup.GetPartitionName()))
					return task, err
				}
				err = b.executeBulkInsert(ctx, targetCollectionName, partitionBackup.GetPartitionName(), files, int64(task.GetCollBackup().BackupTimestamp))
				if err != nil {
					log.Error("fail to bulk insert to partition",
						zap.Error(err),
						zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
						zap.String("targetCollectionName", targetCollectionName),
						zap.String("partition", partitionBackup.GetPartitionName()))
					return task, err
				}
			}
		}
		task.RestoredSize = task.RestoredSize + partitionBackup.GetSize()
		if task.ToRestoreSize == 0 {
			task.Progress = 100
		} else {
			task.Progress = int32(100 * task.RestoredSize / task.ToRestoreSize)
		}
	}

	return task, err
}

func collectGroupIdsFromSegments(segments []*backuppb.SegmentBackupInfo) []int64 {
	dict := make(map[int64]bool)
	res := make([]int64, 0)
	for _, seg := range segments {
		if _, ok := dict[seg.GetGroupId()]; !ok {
			dict[seg.GetGroupId()] = true
			res = append(res, seg.GetGroupId())
		}
	}
	return res
}

func (b BackupContext) executeBulkInsert(ctx context.Context, coll string, partition string, files []string, endTime int64) error {
	log.Debug("execute bulk insert",
		zap.String("collection", coll),
		zap.String("partition", partition),
		zap.Strings("files", files),
		zap.Int64("endTime", endTime))
	taskId, err := b.milvusClient.BulkInsert(ctx, coll, partition, files, gomilvus.IsBackup(), gomilvus.WithEndTs(endTime))
	if err != nil {
		log.Error("fail to bulk insert",
			zap.Error(err),
			zap.String("collectionName", coll),
			zap.String("partitionName", partition),
			zap.Strings("files", files))
		return err
	}
	err = b.watchBulkInsertState(ctx, taskId, BULKINSERT_TIMEOUT, BULKINSERT_SLEEP_INTERVAL)
	if err != nil {
		log.Error("fail or timeout to bulk insert",
			zap.Error(err),
			zap.Int64("taskId", taskId),
			zap.String("targetCollectionName", coll),
			zap.String("partitionName", partition))
		return err
	}
	return nil
}

func (b BackupContext) watchBulkInsertState(ctx context.Context, taskId int64, timeout int64, sleepSeconds int) error {
	lastProgress := 0
	lastUpdateTime := time.Now().Unix()
	for {
		importTaskState, err := b.milvusClient.GetBulkInsertState(ctx, taskId)
		currentTimestamp := time.Now().Unix()
		log.Info("bulkinsert task state",
			zap.Int64("id", taskId),
			zap.Int32("state", int32(importTaskState.State)),
			zap.Any("state", importTaskState),
			zap.Int("progress", importTaskState.Progress()),
			zap.Int64("currentTimestamp", currentTimestamp),
			zap.Int64("lastUpdateTime", lastUpdateTime))
		if err != nil {
			return err
		}
		switch importTaskState.State {
		case entity.BulkInsertFailed:
			if value, ok := importTaskState.Infos["failed_reason"]; ok {
				return errors.New("bulk insert fail, info: " + value)
			} else {
				return errors.New("bulk insert fail")
			}
		case entity.BulkInsertCompleted:
			return nil
		default:
			currentProgress := importTaskState.Progress()
			if currentProgress > lastProgress {
				lastUpdateTime = time.Now().Unix()
			} else if (currentTimestamp - lastUpdateTime) >= timeout {
				log.Warn(fmt.Sprintf("bulkinsert task state progress hang for more than %d s", timeout))
				return errors.New("import task timeout")
			}
			time.Sleep(time.Second * time.Duration(sleepSeconds))
			continue
		}
	}
	return errors.New("import task timeout")
}

func (b BackupContext) getBackupPartitionPaths(ctx context.Context, bucketName string, backupPath string, partition *backuppb.PartitionBackupInfo) ([]string, error) {
	log.Info("getBackupPartitionPaths",
		zap.String("bucketName", bucketName),
		zap.String("backupPath", backupPath),
		zap.Int64("partitionID", partition.PartitionId))

	insertPath := fmt.Sprintf("%s/%s/%s/%v/%v/", backupPath, BINGLOG_DIR, INSERT_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())
	deltaPath := fmt.Sprintf("%s/%s/%s/%v/%v/", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())

	exist, err := b.storageClient.Exist(ctx, bucketName, deltaPath)
	if err != nil {
		log.Warn("check binlog exist fail", zap.Error(err))
		return []string{}, err
	}
	if !exist {
		return []string{insertPath, ""}, nil
	}
	return []string{insertPath, deltaPath}, nil
}

func (b BackupContext) getBackupPartitionPathsWithGroupID(ctx context.Context, bucketName string, backupPath string, partition *backuppb.PartitionBackupInfo, groupId int64) ([]string, error) {
	log.Info("getBackupPartitionPaths",
		zap.String("bucketName", bucketName),
		zap.String("backupPath", backupPath),
		zap.Int64("partitionID", partition.GetPartitionId()),
		zap.Int64("groupId", groupId))

	insertPath := fmt.Sprintf("%s/%s/%s/%v/%v/%d", backupPath, BINGLOG_DIR, INSERT_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId(), groupId)
	deltaPath := fmt.Sprintf("%s/%s/%s/%v/%v/%d", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId(), groupId)

	exist, err := b.storageClient.Exist(ctx, bucketName, deltaPath)
	if err != nil {
		log.Warn("check binlog exist fail", zap.Error(err))
		return []string{}, err
	}
	if !exist {
		return []string{insertPath, ""}, nil
	}
	return []string{insertPath, deltaPath}, nil
}

func (b BackupContext) readBackup(ctx context.Context, bucketName string, backupPath string) (*backuppb.BackupInfo, error) {
	backupMetaDirPath := backupPath + SEPERATOR + META_PREFIX
	backupMetaPath := backupMetaDirPath + SEPERATOR + BACKUP_META_FILE
	collectionMetaPath := backupMetaDirPath + SEPERATOR + COLLECTION_META_FILE
	partitionMetaPath := backupMetaDirPath + SEPERATOR + PARTITION_META_FILE
	segmentMetaPath := backupMetaDirPath + SEPERATOR + SEGMENT_META_FILE

	exist, err := b.storageClient.Exist(ctx, bucketName, backupMetaPath)
	if err != nil {
		log.Error("check backup meta file failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	if !exist {
		log.Warn("read backup meta file not exist", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}

	backupMetaBytes, err := b.storageClient.Read(ctx, bucketName, backupMetaPath)
	if err != nil {
		log.Error("Read backup meta failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	collectionBackupMetaBytes, err := b.storageClient.Read(ctx, bucketName, collectionMetaPath)
	if err != nil {
		log.Error("Read collection meta failed", zap.String("path", collectionMetaPath), zap.Error(err))
		return nil, err
	}
	partitionBackupMetaBytes, err := b.storageClient.Read(ctx, bucketName, partitionMetaPath)
	if err != nil {
		log.Error("Read partition meta failed", zap.String("path", partitionMetaPath), zap.Error(err))
		return nil, err
	}
	segmentBackupMetaBytes, err := b.storageClient.Read(ctx, bucketName, segmentMetaPath)
	if err != nil {
		log.Error("Read segment meta failed", zap.String("path", segmentMetaPath), zap.Error(err))
		return nil, err
	}

	completeBackupMetas := &BackupMetaBytes{
		BackupMetaBytes:     backupMetaBytes,
		CollectionMetaBytes: collectionBackupMetaBytes,
		PartitionMetaBytes:  partitionBackupMetaBytes,
		SegmentMetaBytes:    segmentBackupMetaBytes,
	}

	backupInfo, err := deserialize(completeBackupMetas)
	if err != nil {
		log.Error("Fail to deserialize backup info", zap.String("backupPath", backupPath), zap.Error(err))
		return nil, err
	}

	return backupInfo, nil
}

func (b BackupContext) readSegmentInfo(ctx context.Context, collecitonID int64, partitionID int64, segmentID int64, numOfRows int64) (*backuppb.SegmentBackupInfo, error) {
	segmentBackupInfo := backuppb.SegmentBackupInfo{
		SegmentId:    segmentID,
		CollectionId: collecitonID,
		PartitionId:  partitionID,
		NumOfRows:    numOfRows,
	}
	var size int64 = 0

	insertPath := fmt.Sprintf("%s/%s/%v/%v/%v/", b.params.MinioCfg.RootPath, "insert_log", collecitonID, partitionID, segmentID)
	log.Debug("insertPath", zap.String("insertPath", insertPath))
	fieldsLogDir, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, insertPath, false)
	log.Debug("fieldsLogDir", zap.Any("fieldsLogDir", fieldsLogDir))
	insertLogs := make([]*backuppb.FieldBinlog, 0)
	for _, fieldLogDir := range fieldsLogDir {
		binlogPaths, sizes, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, fieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(fieldLogDir, insertPath, "", 1), SEPERATOR, "", -1)
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

	deltaLogPath := fmt.Sprintf("%s/%s/%v/%v/%v/", b.params.MinioCfg.RootPath, "delta_log", collecitonID, partitionID, segmentID)
	deltaFieldsLogDir, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, deltaLogPath, false)
	deltaLogs := make([]*backuppb.FieldBinlog, 0)
	for _, deltaFieldLogDir := range deltaFieldsLogDir {
		binlogPaths, sizes, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, deltaFieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(deltaFieldLogDir, deltaLogPath, "", 1), SEPERATOR, "", -1)
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
		deltaLogs = append(deltaLogs, &backuppb.FieldBinlog{
			FieldID: 0,
		})
	}

	//statsLogPath := fmt.Sprintf("%s/%s/%v/%v/%v/", b.params.MinioCfg.RootPath, "stats_log", collecitonID, partitionID, segmentID)
	//statsFieldsLogDir, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, statsLogPath, false)
	//statsLogs := make([]*backuppb.FieldBinlog, 0)
	//for _, statsFieldLogDir := range statsFieldsLogDir {
	//	binlogPaths, sizes, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, statsFieldLogDir, false)
	//	fieldIdStr := strings.Replace(strings.Replace(statsFieldLogDir, statsLogPath, "", 1), SEPERATOR, "", -1)
	//	fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
	//	binlogs := make([]*backuppb.Binlog, 0)
	//	for index, binlogPath := range binlogPaths {
	//		binlogs = append(binlogs, &backuppb.Binlog{
	//			LogPath: binlogPath,
	//			LogSize: sizes[index],
	//		})
	//	}
	//	statsLogs = append(statsLogs, &backuppb.FieldBinlog{
	//		FieldID: fieldId,
	//		Binlogs: binlogs,
	//	})
	//}

	segmentBackupInfo.Binlogs = insertLogs
	segmentBackupInfo.Deltalogs = deltaLogs
	//segmentBackupInfo.Statslogs = statsLogs

	segmentBackupInfo.Size = size
	return &segmentBackupInfo, nil
}

func (b *BackupContext) GetRestore(ctx context.Context, request *backuppb.GetRestoreStateRequest) *backuppb.RestoreBackupResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive GetRestoreStateRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("id", request.GetId()))

	resp := &backuppb.RestoreBackupResponse{
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

	if request.GetId() == "" {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "empty restore id"
		return resp
	}

	if value, ok := b.restoreTasks[request.GetId()]; ok {
		resp.Code = backuppb.ResponseCode_Success
		resp.Msg = "success"
		resp.Data = UpdateRestoreBackupTask(value)
		return resp
	} else {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "restore id not exist in context"
		return resp
	}
}
