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
	"github.com/zilliztech/milvus-backup/internal/log"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"
)

const (
	BACKUP_ROW_BASED         = false // bulkload backup should be columned based
	BULKLOAD_TIMEOUT         = 10 * 60
	BULKLOAD_SLEEP_INTERVAL  = 3
	BACKUP_NAME              = "BACKUP_NAME"
	COLLECTION_RENAME_SUFFIX = "COLLECTION_RENAME_SUFFIX"
)

// makes sure BackupContext implements `Backup`
var _ Backup = (*BackupContext)(nil)

// Don't use it, only exist in tests
var Params paramtable.BackupParams

type BackupContext struct {
	ctx context.Context
	// lock to make sure only one backup is creating or loading
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
}

func (b *BackupContext) GetLoadBackupState(ctx context.Context, request *backuppb.LoadBackupRequest) (*backuppb.LoadBackupResponse, error) {
	//TODO implement me
	panic("implement me")
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

func (b BackupContext) CreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest) (*backuppb.BackupInfoResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.started {
		err := b.Start()
		if err != nil {
			return &backuppb.BackupInfoResponse{
				Status: &backuppb.Status{StatusCode: backuppb.StatusCode_ConnectFailed},
			}, nil
		}
	}

	errorResp := &backuppb.BackupInfoResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_UnexpectedError,
		},
	}

	leveledBackupInfo := &LeveledBackupInfo{}

	// backup name validate
	if request.GetBackupName() != "" {
		resp, err := b.GetBackup(b.ctx, &backuppb.GetBackupRequest{
			BackupName: request.GetBackupName(),
		})
		if err != nil {
			log.Error("fail in GetBackup", zap.Error(err))
			errorResp.Status.Reason = err.Error()
			return errorResp, nil
		}
		if resp.GetBackupInfo() != nil {
			errMsg := fmt.Sprintf("backup already exist with the name: %s", request.GetBackupName())
			log.Error(errMsg)
			errorResp.Status.Reason = errMsg
			return errorResp, nil
		}
	}
	err := utils.ValidateType(request.GetBackupName(), BACKUP_NAME)
	if err != nil {
		log.Error("illegal backup name", zap.Error(err))
		errorResp.Status.Reason = err.Error()
		return errorResp, nil
	}

	// 1, get collection level meta
	log.Debug("Request collection names",
		zap.Strings("request_collection_names", request.GetCollectionNames()),
		zap.Int("length", len(request.GetCollectionNames())))

	var toBackupCollections []*entity.Collection
	if request.GetCollectionNames() == nil || len(request.GetCollectionNames()) == 0 {
		collections, err := b.milvusClient.ListCollections(b.ctx)
		if err != nil {
			log.Error("fail in ListCollections", zap.Error(err))
			errorResp.Status.Reason = err.Error()
			return errorResp, nil
		}
		log.Debug(fmt.Sprintf("List %v collections", len(collections)))
		toBackupCollections = collections
	} else {
		toBackupCollections := make([]*entity.Collection, 0)
		for _, collectionName := range request.GetCollectionNames() {
			exist, err := b.milvusClient.HasCollection(b.ctx, collectionName)
			if err != nil {
				log.Error("fail in HasCollection", zap.Error(err))
				errorResp.Status.Reason = err.Error()
				return errorResp, nil
			}
			if !exist {
				errMsg := fmt.Sprintf("request backup collection does not exist: %s", collectionName)
				log.Error(errMsg)
				errorResp.Status.Reason = errMsg
				return errorResp, nil
			}
			collection, err := b.milvusClient.DescribeCollection(b.ctx, collectionName)
			if err != nil {
				log.Error("fail in DescribeCollection", zap.Error(err))
				errorResp.Status.Reason = err.Error()
				return errorResp, nil
			}
			toBackupCollections = append(toBackupCollections, collection)
		}
	}

	log.Info("collections to backup", zap.Any("collections", toBackupCollections))

	collectionBackupInfos := make([]*backuppb.CollectionBackupInfo, 0)
	for _, collection := range toBackupCollections {
		// list collection result is not complete
		completeCollection, err := b.milvusClient.DescribeCollection(b.ctx, collection.Name)
		if err != nil {
			errorResp.Status.Reason = err.Error()
			return errorResp, nil
		}
		fields := make([]*schemapb.FieldSchema, 0)
		for _, field := range completeCollection.Schema.Fields {
			fields = append(fields, &schemapb.FieldSchema{
				FieldID:      field.ID,
				Name:         field.Name,
				IsPrimaryKey: field.PrimaryKey,
				Description:  field.Description,
				DataType:     schemapb.DataType(field.DataType),
				TypeParams:   utils.MapToKVPair(field.TypeParams),
				IndexParams:  utils.MapToKVPair(field.IndexParams),
			})
		}
		schema := &schemapb.CollectionSchema{
			Name:        completeCollection.Schema.CollectionName,
			Description: completeCollection.Schema.Description,
			AutoID:      completeCollection.Schema.AutoID,
			Fields:      fields,
		}
		collectionBackup := &backuppb.CollectionBackupInfo{
			CollectionId:     completeCollection.ID,
			DbName:           "", // todo currently db_name is not used in many places
			CollectionName:   completeCollection.Name,
			Schema:           schema,
			ShardsNum:        completeCollection.ShardNum,
			ConsistencyLevel: commonpb.ConsistencyLevel(completeCollection.ConsistencyLevel),
		}
		collectionBackupInfos = append(collectionBackupInfos, collectionBackup)
	}
	leveledBackupInfo.collectionLevel = &backuppb.CollectionLevelBackupInfo{
		Infos: collectionBackupInfos,
	}

	// 2, get partition level meta
	partitionBackupInfos := make([]*backuppb.PartitionBackupInfo, 0)
	for _, collection := range toBackupCollections {
		partitions, err := b.milvusClient.ShowPartitions(b.ctx, collection.Name)
		if err != nil {
			errorResp.Status.Reason = err.Error()
			return errorResp, nil
		}
		for _, partition := range partitions {
			partitionBackupInfos = append(partitionBackupInfos, &backuppb.PartitionBackupInfo{
				PartitionId:   partition.ID,
				PartitionName: partition.Name,
				CollectionId:  collection.ID,
				//SegmentBackups is now empty,
				//will fullfill after getting segments info and rearrange from level structure to tree structure
			})
		}
	}
	leveledBackupInfo.partitionLevel = &backuppb.PartitionLevelBackupInfo{
		Infos: partitionBackupInfos,
	}

	log.Info("Finish build backup collection meta")

	// 3, Flush
	collSegmentsMap := make(map[string][]int64)
	collSealTimeMap := make(map[string]int64)
	for _, coll := range toBackupCollections {
		newSealedSegmentIDs, flushedSegmentIDs, timeOfSeal, err := b.milvusClient.Flush(ctx, coll.Name, false)
		log.Info("flush segments",
			zap.Int64s("newSealedSegmentIDs", newSealedSegmentIDs),
			zap.Int64s("flushedSegmentIDs", flushedSegmentIDs),
			zap.Int64("timeOfSeal", timeOfSeal))
		collSegmentsMap[coll.Name] = append(newSealedSegmentIDs, flushedSegmentIDs...)
		collSealTimeMap[coll.Name] = timeOfSeal
		if err != nil {
			log.Error(fmt.Sprintf("fail to flush the collection: %s", coll.Name))
			errorResp.Status.Reason = err.Error()
			return errorResp, nil
		}
	}
	// set collection backup time = timeOfSeal
	for _, coll := range leveledBackupInfo.collectionLevel.GetInfos() {
		coll.BackupTimestamp = utils.ComposeTS(collSealTimeMap[coll.CollectionName], 0)
	}

	// 4, get segment level meta
	// get segment infos by milvus SDK
	// todo: make sure the Binlog filed is not needed: timestampTo, timestampFrom, EntriesNum, LogSize
	segmentBackupInfos := make([]*backuppb.SegmentBackupInfo, 0)
	for _, collection := range toBackupCollections {
		segmentDict := utils.ArrayToMap(collSegmentsMap[collection.Name])
		segments, err := b.milvusClient.GetPersistentSegmentInfo(ctx, collection.Name)
		if err != nil {
			errorResp.Status.Reason = err.Error()
			return errorResp, nil
		}
		for _, segment := range segments {
			if segmentDict[segment.ID] {
				segmentInfo, err := b.readSegmentInfo(ctx, segment.CollectionID, segment.ParititionID, segment.ID, segment.NumRows)
				if err != nil {
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}
				if len(segmentInfo.Binlogs) == 0 {
					log.Warn("this segment has no insert binlog", zap.Int64("id", segment.ID))
				}
				segmentBackupInfos = append(segmentBackupInfos, segmentInfo)
			} else {
				log.Debug("new segments after flush, skip it", zap.Int64("id", segment.ID))
			}
		}
	}
	log.Info(fmt.Sprintf("Get segment num %d", len(segmentBackupInfos)))

	leveledBackupInfo.segmentLevel = &backuppb.SegmentLevelBackupInfo{
		Infos: segmentBackupInfos,
	}

	// 5, wrap meta
	completeBackupInfo, err := levelToTree(leveledBackupInfo)
	if err != nil {
		errorResp.Status.Reason = err.Error()
		return errorResp, nil
	}
	completeBackupInfo.BackupTimestamp = uint64(time.Now().Unix())
	if request.GetBackupName() == "" {
		completeBackupInfo.Name = "backup_" + fmt.Sprint(time.Now().Unix())
	} else {
		completeBackupInfo.Name = request.BackupName
	}
	// todo generate ID
	completeBackupInfo.BackupState = &backuppb.BackupTaskState{
		Id: 0,
	}

	// 6, copy data
	for _, segment := range segmentBackupInfos {
		// insert log
		for _, binlogs := range segment.GetBinlogs() {
			for _, binlog := range binlogs.GetBinlogs() {
				targetPath := strings.Replace(binlog.GetLogPath(), b.milvusRootPath, BackupBinlogDirPath(b.backupRootPath, completeBackupInfo.GetName()), 1)
				if targetPath == binlog.GetLogPath() {
					log.Error("wrong target path",
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}

				exist, err := b.storageClient.Exist(ctx, b.milvusBucketName, binlog.GetLogPath())
				if err != nil {
					log.Info("Fail to check file exist",
						zap.Error(err),
						zap.String("file", binlog.GetLogPath()))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}
				if !exist {
					log.Error("Binlog file not exist",
						zap.Error(err),
						zap.String("file", binlog.GetLogPath()))
					errorResp.Status.Reason = "Binlog file not exist"
					return errorResp, nil
				}

				err = b.storageClient.Copy(ctx, b.milvusBucketName, b.backupBucketName, binlog.GetLogPath(), targetPath)
				if err != nil {
					log.Info("Fail to copy file",
						zap.Error(err),
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
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
				targetPath := strings.Replace(binlog.GetLogPath(), b.milvusRootPath, BackupBinlogDirPath(b.backupRootPath, completeBackupInfo.GetName()), 1)
				if targetPath == binlog.GetLogPath() {
					log.Error("wrong target path",
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}

				exist, err := b.storageClient.Exist(ctx, b.milvusBucketName, binlog.GetLogPath())
				if err != nil {
					log.Info("Fail to check file exist",
						zap.Error(err),
						zap.String("file", binlog.GetLogPath()))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}
				if !exist {
					log.Error("Binlog file not exist",
						zap.Error(err),
						zap.String("file", binlog.GetLogPath()))
					errorResp.Status.Reason = "Binlog file not exist"
					return errorResp, nil
				}
				err = b.storageClient.Copy(ctx, b.milvusBucketName, b.backupBucketName, binlog.GetLogPath(), targetPath)
				if err != nil {
					log.Info("Fail to copy file",
						zap.Error(err),
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				} else {
					log.Info("Successfully copy file",
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
				}
			}
		}
	}

	// 7, write meta data
	output, _ := serialize(completeBackupInfo)
	log.Info("backup meta", zap.String("value", string(output.BackupMetaBytes)))
	log.Info("collection meta", zap.String("value", string(output.CollectionMetaBytes)))
	log.Info("partition meta", zap.String("value", string(output.PartitionMetaBytes)))
	log.Info("segment meta", zap.String("value", string(output.SegmentMetaBytes)))

	b.storageClient.Write(ctx, b.backupBucketName, BackupMetaPath(b.backupRootPath, completeBackupInfo.GetName()), output.BackupMetaBytes)
	b.storageClient.Write(ctx, b.backupBucketName, CollectionMetaPath(b.backupRootPath, completeBackupInfo.GetName()), output.CollectionMetaBytes)
	b.storageClient.Write(ctx, b.backupBucketName, PartitionMetaPath(b.backupRootPath, completeBackupInfo.GetName()), output.PartitionMetaBytes)
	b.storageClient.Write(ctx, b.backupBucketName, SegmentMetaPath(b.backupRootPath, completeBackupInfo.GetName()), output.SegmentMetaBytes)

	return &backuppb.BackupInfoResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_Success,
		},
		BackupInfo: completeBackupInfo,
	}, nil
}

func (b BackupContext) GetBackup(ctx context.Context, request *backuppb.GetBackupRequest) (*backuppb.BackupInfoResponse, error) {
	// 1, trigger inner sync to get the newest backup list in the milvus cluster
	if !b.started {
		err := b.Start()
		if err != nil {
			return &backuppb.BackupInfoResponse{
				Status: &backuppb.Status{
					StatusCode: backuppb.StatusCode_ConnectFailed,
				},
			}, nil
		}
	}

	resp := &backuppb.BackupInfoResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_UnexpectedError,
		},
	}

	if request.GetBackupName() == "" {
		resp.Status.Reason = "empty backup name"
		return resp, nil
	}

	backup, err := b.readBackup(ctx, request.GetBackupName())
	if err != nil {
		log.Warn("Fail to read backup", zap.String("backupName", request.GetBackupName()), zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	return &backuppb.BackupInfoResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_Success,
		},
		BackupInfo: backup,
	}, nil
}

func (b BackupContext) ListBackups(ctx context.Context, request *backuppb.ListBackupsRequest) (*backuppb.ListBackupsResponse, error) {
	if !b.started {
		err := b.Start()
		if err != nil {
			return &backuppb.ListBackupsResponse{
				Status: &backuppb.Status{
					StatusCode: backuppb.StatusCode_ConnectFailed,
				},
			}, nil
		}
	}

	// 1, trigger inner sync to get the newest backup list in the milvus cluster
	backupPaths, _, err := b.storageClient.ListWithPrefix(ctx, b.backupBucketName, b.backupRootPath+SEPERATOR, false)
	resp := &backuppb.ListBackupsResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_UnexpectedError,
		},
		FailBackups: []string{},
	}
	if err != nil {
		log.Error("Fail to list backup directory", zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	log.Info("List Backups' path", zap.Strings("backup_paths", backupPaths))
	backupInfos := make([]*backuppb.BackupInfo, 0)
	failBackups := make([]string, 0)
	for _, backupPath := range backupPaths {
		backupResp, err := b.GetBackup(ctx, &backuppb.GetBackupRequest{
			BackupName: BackupPathToName(b.backupRootPath, backupPath),
		})
		if err != nil {
			log.Warn("Fail to read backup",
				zap.String("path", backupPath),
				zap.Error(err))
			resp.Status.Reason = err.Error()
			failBackups = append(failBackups, BackupPathToName(b.backupRootPath, backupPath))
			//return resp, nil
		}
		if backupResp.GetStatus().StatusCode != backuppb.StatusCode_Success {
			log.Warn("Fail to read backup",
				zap.String("path", backupPath),
				zap.String("error", backupResp.GetStatus().GetReason()))
			resp.Status.Reason = backupResp.GetStatus().GetReason()
			failBackups = append(failBackups, BackupPathToName(b.backupRootPath, backupPath))
			//return resp, nil
		}

		// 2, list wanted backup
		if backupResp.GetBackupInfo() != nil {
			if request.GetCollectionName() != "" {
				// if request.GetCollectionName() is defined only return backups contains the certain collection
				for _, collectionMeta := range backupResp.GetBackupInfo().GetCollectionBackups() {
					if collectionMeta.GetCollectionName() == request.GetCollectionName() {
						backupInfos = append(backupInfos, backupResp.GetBackupInfo())
					}
				}
			} else {
				backupInfos = append(backupInfos, backupResp.GetBackupInfo())
			}
		}
	}

	// 3, return
	return &backuppb.ListBackupsResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_Success,
		},
		BackupInfos: backupInfos,
		FailBackups: failBackups,
	}, nil
}

func (b BackupContext) DeleteBackup(ctx context.Context, request *backuppb.DeleteBackupRequest) (*backuppb.DeleteBackupResponse, error) {
	if !b.started {
		err := b.Start()
		if err != nil {
			return &backuppb.DeleteBackupResponse{
				Status: &backuppb.Status{
					StatusCode: backuppb.StatusCode_ConnectFailed,
				},
			}, nil
		}
	}

	resp := &backuppb.DeleteBackupResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_UnexpectedError,
		},
	}

	if request.GetBackupName() == "" {
		resp.Status.Reason = "empty backup name"
		return resp, nil
	}

	err := b.storageClient.RemoveWithPrefix(ctx, b.backupBucketName, BackupDirPath(b.backupRootPath, request.GetBackupName()))

	if err != nil {
		log.Error("Fail to delete backup", zap.String("backupName", request.GetBackupName()), zap.Error(err))
		return nil, err
	}

	return &backuppb.DeleteBackupResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_Success,
		},
	}, nil
}

func (b BackupContext) LoadBackup(ctx context.Context, request *backuppb.LoadBackupRequest) (*backuppb.LoadBackupResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.started {
		err := b.Start()
		if err != nil {
			return &backuppb.LoadBackupResponse{
				Status: &backuppb.Status{
					StatusCode: backuppb.StatusCode_ConnectFailed,
				},
			}, nil
		}
	}

	resp := &backuppb.LoadBackupResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_UnexpectedError,
		},
	}

	// 1, get and validate
	if request.GetCollectionSuffix() != "" {
		err := utils.ValidateType(request.GetCollectionSuffix(), COLLECTION_RENAME_SUFFIX)
		if err != nil {
			log.Error("illegal collection rename suffix", zap.Error(err))
			resp.Status.Reason = err.Error()
			return resp, nil
		}
	}

	getResp, err := b.GetBackup(ctx, &backuppb.GetBackupRequest{
		BackupName: request.GetBackupName(),
	})
	if err != nil {
		log.Error("fail to get backup", zap.String("backupName", request.GetBackupName()), zap.Error(err))
		resp.Status.Reason = "fail to get backup"
		return resp, err
	}
	if getResp.GetBackupInfo() == nil {
		log.Error("backup doesn't exist", zap.String("backupName", request.GetBackupName()))
		resp.Status.Reason = "backup doesn't exist"
		return resp, nil
	}

	backup := getResp.GetBackupInfo()
	resp.BackupInfo = backup
	log.Info("successfully get the backup to load", zap.String("backupName", backup.GetName()))

	// 2, initial loadCollectionTasks
	toLoadCollectionBackups := make([]*backuppb.CollectionBackupInfo, 0)
	if len(request.GetCollectionNames()) == 0 {
		toLoadCollectionBackups = backup.GetCollectionBackups()
	} else {
		collectionNameDict := make(map[string]bool)
		for _, collectName := range request.GetCollectionNames() {
			collectionNameDict[collectName] = true
		}
		for _, collectionBackup := range backup.GetCollectionBackups() {
			if collectionNameDict[collectionBackup.GetCollectionName()] {
				toLoadCollectionBackups = append(toLoadCollectionBackups, collectionBackup)
			}
		}
	}
	log.Info("Collections to load", zap.Int("totalNum", len(toLoadCollectionBackups)))

	loadCollectionTasks := make([]*backuppb.LoadCollectionTask, 0)
	for _, loadCollection := range toLoadCollectionBackups {
		backupCollectionName := loadCollection.GetSchema().GetName()
		var targetCollectionName string
		// rename collection, rename map has higher poriority then suffix
		if len(request.GetCollectionRenames()) > 0 && request.GetCollectionRenames()[backupCollectionName] != "" {
			targetCollectionName = request.GetCollectionRenames()[backupCollectionName]
		} else if request.GetCollectionSuffix() != "" {
			targetCollectionName = backupCollectionName + request.GetCollectionSuffix()
		} else {
			targetCollectionName = backupCollectionName
		}

		exist, err := b.milvusClient.HasCollection(ctx, targetCollectionName)
		if err != nil {
			log.Error("fail to check whether the collection is exist", zap.Error(err), zap.String("collection_name", targetCollectionName))
			resp.Status.Reason = err.Error()
			return resp, nil
		}
		if exist {
			log.Error("The collection to load already exists",
				zap.String("backupCollectName", backupCollectionName),
				zap.String("targetCollectionName", targetCollectionName))
			resp.Status.Reason = fmt.Sprintf("load target collection already exists in the cluster: %s", targetCollectionName)
			return resp, nil
		}

		task := &backuppb.LoadCollectionTask{
			LoadState: &backuppb.LoadTaskState{
				Code: backuppb.LoadTaskStateCode_LOAD_INITIAL,
			},
			CollBackup:           loadCollection,
			TargetCollectionName: targetCollectionName,
			PartitionLoadTasks:   []*backuppb.LoadPartitionTask{},
		}
		loadCollectionTasks = append(loadCollectionTasks, task)
	}
	resp.CollectionLoadTasks = loadCollectionTasks

	// 3, execute load loadCollectionTasks
	for _, task := range loadCollectionTasks {
		err := b.executeLoadTask(ctx, backup.GetName(), task)
		if err != nil {
			task.LoadState.ErrorMessage = err.Error()
			task.LoadState.Code = backuppb.LoadTaskStateCode_LOAD_FAIL
			resp.Status.Reason = err.Error()
			return resp, nil
		}
		task.LoadState.Code = backuppb.LoadTaskStateCode_LOAD_SUCCESS
	}

	resp.Status.StatusCode = backuppb.StatusCode_Success
	return resp, nil
}

func (b BackupContext) executeLoadTask(ctx context.Context, backupName string, task *backuppb.LoadCollectionTask) error {
	targetCollectionName := task.GetTargetCollectionName()
	task.LoadState.Code = backuppb.LoadTaskStateCode_LOAD_EXECUTING
	log.With(zap.String("backupName", backupName))
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

	err := b.milvusClient.CreateCollection(
		ctx,
		collectionSchema,
		task.GetCollBackup().GetShardsNum(),
		gomilvus.WithConsistencyLevel(entity.ConsistencyLevel(task.GetCollBackup().GetConsistencyLevel())))

	if err != nil {
		log.Error("fail to create collection", zap.Error(err), zap.String("targetCollectionName", targetCollectionName))
		return err
	}

	for _, partitionBackup := range task.GetCollBackup().GetPartitionBackups() {
		exist, err := b.milvusClient.HasPartition(ctx, targetCollectionName, partitionBackup.GetPartitionName())
		if err != nil {
			log.Error("fail to check has partition", zap.Error(err))
			return err
		}
		if !exist {
			err = b.milvusClient.CreatePartition(ctx, targetCollectionName, partitionBackup.GetPartitionName())
			if err != nil {
				log.Error("fail to create partition", zap.Error(err))
				return err
			}
		}

		// bulkload
		// todo ts
		options := make(map[string]string)
		options["end_ts"] = fmt.Sprint(task.GetCollBackup().BackupTimestamp)
		options["backup"] = "true"
		files, err := b.getBackupPartitionPaths(ctx, backupName, partitionBackup)
		if err != nil {
			log.Error("fail to get partition backup binlog files",
				zap.Error(err),
				zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
				zap.String("targetCollectionName", targetCollectionName),
				zap.String("partition", partitionBackup.GetPartitionName()))
			return err
		}
		log.Debug("execute bulkload",
			zap.String("collection", targetCollectionName),
			zap.String("partition", partitionBackup.GetPartitionName()),
			zap.Strings("files", files))
		err = b.executeBulkload(ctx, targetCollectionName, partitionBackup.GetPartitionName(), files, options)
		if err != nil {
			log.Error("fail to bulkload to partition",
				zap.Error(err),
				zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
				zap.String("targetCollectionName", targetCollectionName),
				zap.String("partition", partitionBackup.GetPartitionName()))
			return err
		}
	}

	return nil
}

func (b BackupContext) executeBulkload(ctx context.Context, coll string, partition string, files []string, options map[string]string) error {
	taskIds, err := b.milvusClient.Bulkload(ctx, coll, partition, BACKUP_ROW_BASED, files, options)
	if err != nil {
		log.Error("fail to bulkload",
			zap.Error(err),
			zap.String("collectionName", coll),
			zap.String("partitionName", partition),
			zap.Strings("files", files))
		return err
	}
	for _, taskId := range taskIds {
		loadErr := b.watchBulkloadState(ctx, taskId, BULKLOAD_TIMEOUT, BULKLOAD_SLEEP_INTERVAL)
		if loadErr != nil {
			log.Error("fail or timeout to bulkload",
				zap.Error(err),
				zap.Int64("taskId", taskId),
				zap.String("targetCollectionName", coll),
				zap.String("partitionName", partition))
			return err
		}
	}
	return nil
}

func (b BackupContext) watchBulkloadState(ctx context.Context, taskId int64, timeout int64, sleepSeconds int) error {
	start := time.Now().Unix()
	for time.Now().Unix()-start < timeout {
		importTaskState, err := b.milvusClient.GetBulkloadState(ctx, taskId)
		log.Debug("bulkinsert task state", zap.Int64("id", taskId), zap.Any("state", importTaskState))
		switch importTaskState.State {
		case entity.BulkloadFailed:
			return err
		case entity.BulkloadCompleted:
			return nil
		default:
			time.Sleep(time.Second * time.Duration(sleepSeconds))
			continue
		}
	}
	return errors.New("import task timeout")
}

func (b BackupContext) getBackupPartitionPaths(ctx context.Context, backupName string, partition *backuppb.PartitionBackupInfo) ([]string, error) {
	insertPath := fmt.Sprintf("%s/%s/%s/%s/%v/%v/", b.backupRootPath, backupName, BINGLOG_DIR, INSERT_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())
	deltaPath := fmt.Sprintf("%s/%s/%s/%s/%v/%v/", b.backupRootPath, backupName, BINGLOG_DIR, DELTA_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())

	exist, err := b.storageClient.Exist(ctx, b.backupBucketName, deltaPath)
	if err != nil {
		log.Warn("check binlog exist fail", zap.Error(err))
		return []string{}, err
	}
	if !exist {
		return []string{insertPath, ""}, nil
	}
	return []string{insertPath, deltaPath}, nil
}

func (b BackupContext) readBackup(ctx context.Context, backupName string) (*backuppb.BackupInfo, error) {
	backupMetaDirPath := b.backupRootPath + SEPERATOR + backupName + SEPERATOR + META_PREFIX
	backupMetaPath := backupMetaDirPath + SEPERATOR + BACKUP_META_FILE
	collectionMetaPath := backupMetaDirPath + SEPERATOR + COLLECTION_META_FILE
	partitionMetaPath := backupMetaDirPath + SEPERATOR + PARTITION_META_FILE
	segmentMetaPath := backupMetaDirPath + SEPERATOR + SEGMENT_META_FILE

	exist, err := b.storageClient.Exist(ctx, b.backupBucketName, backupMetaPath)
	if err != nil {
		log.Error("check backup meta file failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	if !exist {
		log.Warn("read backup meta file not exist, you may need to create it first", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}

	backupMetaBytes, err := b.storageClient.Read(ctx, b.backupBucketName, backupMetaPath)
	if err != nil {
		log.Error("Read backup meta failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	collectionBackupMetaBytes, err := b.storageClient.Read(ctx, b.backupBucketName, collectionMetaPath)
	if err != nil {
		log.Error("Read collection meta failed", zap.String("path", collectionMetaPath), zap.Error(err))
		return nil, err
	}
	partitionBackupMetaBytes, err := b.storageClient.Read(ctx, b.backupBucketName, partitionMetaPath)
	if err != nil {
		log.Error("Read partition meta failed", zap.String("path", partitionMetaPath), zap.Error(err))
		return nil, err
	}
	segmentBackupMetaBytes, err := b.storageClient.Read(ctx, b.backupBucketName, segmentMetaPath)
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
		log.Error("Fail to deserialize backup info", zap.String("backupName", backupName), zap.Error(err))
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

	insertPath := fmt.Sprintf("%s/%s/%v/%v/%v/", b.params.MinioCfg.RootPath, "insert_log", collecitonID, partitionID, segmentID)
	log.Debug("insertPath", zap.String("insertPath", insertPath))
	fieldsLogDir, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, insertPath, false)
	log.Debug("fieldsLogDir", zap.Any("fieldsLogDir", fieldsLogDir))
	insertLogs := make([]*backuppb.FieldBinlog, 0)
	for _, fieldLogDir := range fieldsLogDir {
		binlogPaths, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, fieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(fieldLogDir, insertPath, "", 1), SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*backuppb.Binlog, 0)
		for _, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &backuppb.Binlog{
				LogPath: binlogPath,
			})
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
		binlogPaths, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, deltaFieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(deltaFieldLogDir, deltaLogPath, "", 1), SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*backuppb.Binlog, 0)
		for _, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &backuppb.Binlog{
				LogPath: binlogPath,
			})
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

	statsLogPath := fmt.Sprintf("%s/%s/%v/%v/%v/", b.params.MinioCfg.RootPath, "stats_log", collecitonID, partitionID, segmentID)
	statsFieldsLogDir, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, statsLogPath, false)
	statsLogs := make([]*backuppb.FieldBinlog, 0)
	for _, statsFieldLogDir := range statsFieldsLogDir {
		binlogPaths, _, _ := b.storageClient.ListWithPrefix(ctx, b.milvusBucketName, statsFieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(statsFieldLogDir, statsLogPath, "", 1), SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*backuppb.Binlog, 0)
		for _, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &backuppb.Binlog{
				LogPath: binlogPath,
			})
		}
		statsLogs = append(statsLogs, &backuppb.FieldBinlog{
			FieldID: fieldId,
			Binlogs: binlogs,
		})
	}

	segmentBackupInfo.Binlogs = insertLogs
	segmentBackupInfo.Deltalogs = deltaLogs
	segmentBackupInfo.Statslogs = statsLogs
	return &segmentBackupInfo, nil
}

//func (b *BackupContext) CopyBackup(ctx context.Context, request *backuppb.CopyBackupRequest) (*backuppb.CopyBackupResponse, error) {
//	b.storageClient.Copy(ctx)
//	backupPath := BackupDirPath(b.backupRootPath, request.GetBackupName())
//	milvusBackupPath := BackupDirPath(BACKUP_PREFIX, request.GetBackupName())
//	if request.GetCopyType() == backuppb.CopyType_Backup2Milvus {
//
//	}
//
//}
