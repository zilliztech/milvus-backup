package core

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	//dcc "github.com/zilliztech/milvus-backup/internal/distributed/datacoord/client"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/proto/commonpb"
	"github.com/zilliztech/milvus-backup/internal/proto/datapb"
	"github.com/zilliztech/milvus-backup/internal/proto/schemapb"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"github.com/zilliztech/milvus-backup/internal/util/typeutil"

	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"
)

const (
	BACKUP_ROW_BASED                = false // bulkload backup should be columned based
	BULKLOAD_SEGMENT_TIMEOUT        = 10 * 60
	BULKLOAD_SEGMENT_SLEEP_INTERVAL = 3
)

type Backup interface {
	// Create backuppb
	CreateBackup(context.Context, *backuppb.CreateBackupRequest) (*backuppb.CreateBackupResponse, error)
	// Get backuppb with the chosen name
	GetBackup(context.Context, *backuppb.GetBackupRequest) (*backuppb.GetBackupResponse, error)
	// List backups that contains the given collection name, if collection is not given, return all backups in the cluster
	ListBackups(context.Context, *backuppb.ListBackupsRequest) (*backuppb.ListBackupsResponse, error)
	// Delete backuppb by given backuppb name
	DeleteBackup(context.Context, *backuppb.DeleteBackupRequest) (*backuppb.DeleteBackupResponse, error)
	// Load backuppb to milvus, return backuppb load report
	LoadBackup(context.Context, *backuppb.LoadBackupRequest) (*backuppb.LoadBackupResponse, error)
}

// makes sure BackupContext implements `Backup`
var _ Backup = (*BackupContext)(nil)

type BackupContext struct {
	ctx          context.Context
	milvusSource *MilvusSource
	backupInfos  []backuppb.BackupInfo
	// lock to make sure only one backup is creating or loading
	mu sync.Mutex
	// milvus go sdk client
	milvusClient gomilvus.Client
	//milvusProxyClient     proxy.Client
	//milvusRootCoordClient *rcc.Client
	//milvusDataCoordClient *dcc.Client
	// milvus data storage client
	milvusStorageClient MilvusStorage
	started             bool
}

func (b *BackupContext) Start() error {
	// start milvus go SDK client
	c, err := gomilvus.NewGrpcClient(b.ctx, b.milvusSource.GetProxyAddr())
	if err != nil {
		log.Error("failed to connect to milvus", zap.Error(err))
		return err
	}
	b.milvusClient = c

	// start milvus datacoord client
	//dataCoordClient, err := dcc.NewClient(b.ctx, b.milvusSource.GetDatacoordAddr())
	//if err != nil {
	//	log.Error("failed to connect to milvus's datacoord", zap.Error(err))
	//	return err
	//}
	//b.milvusDataCoordClient = dataCoordClient
	//b.milvusDataCoordClient.Init()
	//b.milvusDataCoordClient.Start()

	// start milvus storage client
	var minioEndPoint string
	Params.Init()
	minioHost := Params.LoadWithDefault("minio.address", paramtable.DefaultMinioHost)
	if strings.Contains(minioHost, ":") {
		minioEndPoint = minioHost
	}
	port := Params.LoadWithDefault("minio.port", paramtable.DefaultMinioPort)
	minioEndPoint = minioHost + ":" + port

	bucketName, _ := Params.Load("minio.bucketName")
	accessKeyID, _ := Params.Load("minio.accessKeyID")
	secretAccessKey, _ := Params.Load("minio.secretAccessKey")
	useSSLStr, _ := Params.Load("minio.useSSL")
	useSSL, _ := strconv.ParseBool(useSSLStr)
	minioClient, err := NewMinioMilvusStorage(b.ctx,
		Address(minioEndPoint),
		AccessKeyID(accessKeyID),
		SecretAccessKeyID(secretAccessKey),
		UseSSL(useSSL),
		BucketName(bucketName),
		UseIAM(false),
		IAMEndpoint(""),
		CreateBucket(true),
	)
	b.milvusStorageClient = minioClient

	//backupDirExist, err := b.milvusStorageClient.Exist(BACKUP_PREFIX)
	//if err != nil {
	//	log.Error("failed to check backup dir exist", zap.Error(err))
	//	return err
	//}
	//if !backupDirExist {
	//	err = b.milvusStorageClient.Write(BACKUP_PREFIX, nil)
	//	if err != nil {
	//		log.Error("failed to create backup dir", zap.Error(err))
	//		return err
	//	}
	//}
	//rootCoordClient, err := rcc.NewClient(b.ctx)
	//if err != nil {
	//	log.Error("failed to connect to milvus's rootcoord", zap.Error(err))
	//	return err
	//}
	//b.milvusRootCoordClient = rootCoordClient

	b.started = true
	return nil
}

func (b *BackupContext) Close() error {
	b.started = false
	err := b.milvusClient.Close()
	//err = b.milvusRootCoordClient.Stop()
	//err = b.milvusDataCoordClient.Stop()
	return err
}

func (b *BackupContext) GetMilvusSource() *MilvusSource {
	return b.milvusSource
}

func CreateBackupContext(ctx context.Context, params paramtable.ComponentParam) *BackupContext {
	var Params paramtable.GrpcServerConfig
	Params.InitOnce(typeutil.ProxyRole)
	milvusAddr := Params.GetAddress()

	//var Params2 paramtable.GrpcServerConfig
	//Params2.InitOnce(typeutil.DataCoordRole)
	//milvusDatacoordAddr := Params2.GetAddress()

	return &BackupContext{
		ctx: ctx,
		milvusSource: &MilvusSource{
			params:    params,
			proxyAddr: milvusAddr,
			//datacoordAddr: milvusDatacoordAddr,
		},
	}
}

// todo refine error handle
// todo support get create backup progress
func (b BackupContext) CreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest) (*backuppb.CreateBackupResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.started {
		err := b.Start()
		if err != nil {
			return &backuppb.CreateBackupResponse{
				Status: &backuppb.Status{StatusCode: backuppb.StatusCode_ConnectFailed},
			}, nil
		}
	}

	errorResp := &backuppb.CreateBackupResponse{
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

	// 4, get segment level meta
	// get segment infos by milvus SDK
	// todo: make sure the Binlog filed is not needed: timestampTo, timestampFrom, EntriesNum, LogSize
	segmentBackupInfos := make([]*backuppb.SegmentBackupInfo, 0)
	for _, collection := range toBackupCollections {
		segments, err := b.milvusClient.GetPersistentSegmentInfo(ctx, collection.Name)
		if err != nil {
			errorResp.Status.Reason = err.Error()
			return errorResp, nil
		}
		for _, segment := range segments {
			segmentInfo, err := b.readSegmentInfo(ctx, segment.CollectionID, segment.ParititionID, segment.ID, segment.NumRows)
			if err != nil {
				errorResp.Status.Reason = err.Error()
				return errorResp, nil
			}
			segmentBackupInfos = append(segmentBackupInfos, segmentInfo)
		}
	}
	log.Info(fmt.Sprintf("Get segment num %d", len(segmentBackupInfos)))

	// get segment infos by datacoord client
	//for _, part := range partitionBackupInfos {
	//	collectionID := part.GetCollectionId()
	//	partitionID := part.GetPartitionId()
	//	errorResp, err := b.milvusDataCoordClient.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{
	//		CollectionID: collectionID,
	//		PartitionID:  partitionID,
	//	})
	//	if err != nil || errorResp.Status.ErrorCode != commonpb.ErrorCode_Success {
	//		return nil, err
	//	}
	//	for _, binlogs := range errorResp.GetBinlogs() {
	//		segmentBackupInfos = append(segmentBackupInfos, &backuppb.SegmentBackupInfo{
	//			SegmentId:    binlogs.GetSegmentID(),
	//			CollectionId: collectionID,
	//			PartitionId:  partitionID,
	//			NumOfRows:    binlogs.GetNumOfRows(),
	//			Binlogs:      binlogs.GetFieldBinlogs(),
	//			Deltalogs:    binlogs.GetDeltalogs(),
	//			Statslogs:    binlogs.GetStatslogs(),
	//		})
	//	}
	//}

	leveledBackupInfo.segmentLevel = &backuppb.SegmentLevelBackupInfo{
		Infos: segmentBackupInfos,
	}

	// 5, wrap meta
	completeBackupInfo, err := levelToTree(leveledBackupInfo)
	if err != nil {
		errorResp.Status.Reason = err.Error()
		return errorResp, nil
	}
	completeBackupInfo.BackupStatus = backuppb.StatusCode_Success
	completeBackupInfo.BackupTimestamp = uint64(time.Now().Unix())
	if request.GetBackupName() == "" {
		completeBackupInfo.Name = "backup_" + fmt.Sprint(time.Now().Unix())
	} else {
		completeBackupInfo.Name = request.BackupName
	}
	// todo generate ID
	completeBackupInfo.Id = 0

	// 6, copy data
	for _, segment := range segmentBackupInfos {
		// insert log
		for _, binlogs := range segment.GetBinlogs() {
			for _, binlog := range binlogs.GetBinlogs() {
				targetPath := strings.Replace(binlog.GetLogPath(), "files", DataDirPath(completeBackupInfo), 1)
				if targetPath == binlog.GetLogPath() {
					log.Error("wrong target path",
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}

				err = b.milvusStorageClient.Copy(binlog.GetLogPath(), targetPath)
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
		// delta log
		for _, binlogs := range segment.GetDeltalogs() {
			for _, binlog := range binlogs.GetBinlogs() {
				targetPath := strings.Replace(binlog.GetLogPath(), "files", DataDirPath(completeBackupInfo), 1)
				if targetPath == binlog.GetLogPath() {
					log.Error("wrong target path",
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}

				err = b.milvusStorageClient.Copy(binlog.GetLogPath(), targetPath)
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

		// stats log
		for _, binlogs := range segment.GetStatslogs() {
			for _, binlog := range binlogs.GetBinlogs() {
				targetPath := strings.Replace(binlog.GetLogPath(), "files", DataDirPath(completeBackupInfo), 1)
				if targetPath == binlog.GetLogPath() {
					log.Error("wrong target path",
						zap.String("from", binlog.GetLogPath()),
						zap.String("to", targetPath))
					errorResp.Status.Reason = err.Error()
					return errorResp, nil
				}

				err = b.milvusStorageClient.Copy(binlog.GetLogPath(), targetPath)
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
	log.Info(string(output.BackupMetaBytes))
	log.Info(string(output.CollectionMetaBytes))
	log.Info(string(output.PartitionMetaBytes))
	log.Info(string(output.SegmentMetaBytes))

	b.milvusStorageClient.Write(BackupMetaPath(completeBackupInfo), output.BackupMetaBytes)
	b.milvusStorageClient.Write(CollectionMetaPath(completeBackupInfo), output.CollectionMetaBytes)
	b.milvusStorageClient.Write(PartitionMetaPath(completeBackupInfo), output.PartitionMetaBytes)
	b.milvusStorageClient.Write(SegmentMetaPath(completeBackupInfo), output.SegmentMetaBytes)

	return &backuppb.CreateBackupResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_Success,
		},
		BackupInfo: completeBackupInfo,
	}, nil
}

func (b BackupContext) GetBackup(ctx context.Context, request *backuppb.GetBackupRequest) (*backuppb.GetBackupResponse, error) {
	// 1, trigger inner sync to get the newest backup list in the milvus cluster
	if !b.started {
		err := b.Start()
		if err != nil {
			return &backuppb.GetBackupResponse{
				Status: &backuppb.Status{
					StatusCode: backuppb.StatusCode_ConnectFailed,
				},
			}, nil
		}
	}

	resp := &backuppb.GetBackupResponse{
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
		log.Error("Fail to read backup", zap.String("backupName", request.GetBackupName()), zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	return &backuppb.GetBackupResponse{
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
	backupPaths, _, err := b.milvusStorageClient.ListWithPrefix(BACKUP_PREFIX+SEPERATOR, false)
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
			BackupName: BackupPathToName(backupPath),
		})
		if err != nil {
			log.Warn("Fail to read backup",
				zap.String("path", backupPath),
				zap.Error(err))
			resp.Status.Reason = err.Error()
			failBackups = append(failBackups, BackupPathToName(backupPath))
			//return resp, nil
		}
		if backupResp.GetStatus().StatusCode != backuppb.StatusCode_Success {
			log.Warn("Fail to read backup",
				zap.String("path", backupPath),
				zap.String("error", backupResp.GetStatus().GetReason()))
			resp.Status.Reason = backupResp.GetStatus().GetReason()
			failBackups = append(failBackups, BackupPathToName(backupPath))
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

	err := b.milvusStorageClient.RemoveWithPrefix(generateBackupDirPath(request.GetBackupName()))

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

	resp := &backuppb.LoadBackupResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_UnexpectedError,
		},
	}

	// 1, validate
	getResp, err := b.GetBackup(ctx, &backuppb.GetBackupRequest{
		BackupName: request.GetBackupName(),
	})
	if err != nil {
		log.Error("Fail to get backup", zap.String("backupName", request.GetBackupName()), zap.Error(err))
		return nil, err
	}
	if getResp.GetBackupInfo() == nil {
		resp.Status.Reason = "backup doesn't exist"
		return resp, nil
	}

	backup := getResp.GetBackupInfo()
	log.Info("Successfully get the backup meta to laod", zap.String("backupName", backup.GetName()))

	// todo: check healthy

	toLoadCollectionBackups := make([]*backuppb.CollectionBackupInfo, 0)
	if len(request.GetCollectionNames()) == 0 {
		toLoadCollectionBackups = backup.GetCollectionBackups()
	} else {
		collectionNameDict := make(map[string]bool)
		for _, collectName := range request.GetCollectionNames() {
			collectionNameDict[collectName] = true
		}
		for _, collectionBackup := range getResp.GetBackupInfo().GetCollectionBackups() {
			if collectionNameDict[collectionBackup.GetCollectionName()] {
				toLoadCollectionBackups = append(toLoadCollectionBackups, collectionBackup)
				log.Info("To load all collections in backup", zap.String("backupName", backup.GetName()))
			}
		}
	}

	log.Info("Collections to load", zap.Int("totalNum", len(toLoadCollectionBackups)))

	tasks := make([]*backuppb.LoadCollectionBackupTask, 0)

	// 2, load
	for _, loadCollection := range toLoadCollectionBackups {
		backupCollectionName := loadCollection.GetSchema().GetName()
		var targetCollectionName string
		// rename collection, rename map has higher poriority then suffix
		if len(request.GetCollectionRenames()) > 0 && request.GetCollectionRenames()[backupCollectionName] != "" {
			targetCollectionName = request.GetCollectionRenames()[backupCollectionName]
		} else if request.GetCollectionSuffix() != "" {
			targetCollectionName = backupCollectionName + request.GetCollectionSuffix()
		}

		task := &backuppb.LoadCollectionBackupTask{
			State:                backuppb.LoadState_INTI,
			CollBackup:           loadCollection,
			TargetCollectionName: targetCollectionName,
		}
		tasks = append(tasks, task)
	}
	resp.CollectionBackupTasks = tasks

	for _, task := range tasks {
		b.executeTask(ctx, task)
	}

	return resp, nil
}

func (b BackupContext) executeTask(ctx context.Context, task *backuppb.LoadCollectionBackupTask) *backuppb.LoadCollectionBackupTask {
	backupCollectionName := task.GetCollBackup().GetCollectionName()
	targetCollectionName := task.GetTargetCollectionName()
	task.State = backuppb.LoadState_EXECUTING
	// check exist
	exist, err := b.milvusClient.HasCollection(ctx, targetCollectionName)
	if err != nil {
		log.Warn("Fail to check whether the collection is exist", zap.Error(err))
		task.ErrorMessage = err.Error()
		task.State = backuppb.LoadState_FAIL
		return task
	}
	if exist {
		log.Warn("The collection to load is already exist",
			zap.String("backupCollectName", backupCollectionName),
			zap.String("targetCollectionName", targetCollectionName))
		task.ErrorMessage = "load target collection already exist in the cluster"
		task.State = backuppb.LoadState_FAIL
		return task
	}

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

	err = b.milvusClient.CreateCollection(
		ctx,
		collectionSchema,
		task.GetCollBackup().GetShardsNum(),
		gomilvus.WithConsistencyLevel(entity.ConsistencyLevel(task.GetCollBackup().GetConsistencyLevel())))

	if err != nil {
		log.Error("Fail to create collection", zap.Error(err), zap.String("targetCollectionName", targetCollectionName))
		task.ErrorMessage = err.Error()
		task.State = backuppb.LoadState_FAIL
		return task
	}

	//
	for _, partitionBackup := range task.GetCollBackup().GetPartitionBackups() {
		// todo: handle default partition
		err = b.milvusClient.CreatePartition(ctx, targetCollectionName, partitionBackup.GetPartitionName())
		if err != nil {
			log.Error("Fail to create partition", zap.Error(err))
			task.ErrorMessage = err.Error()
			task.State = backuppb.LoadState_FAIL
			return task
		}
		for _, segment := range partitionBackup.GetSegmentBackups() {
			// bulkload
			taskIds, err := b.milvusClient.Bulkload(ctx, targetCollectionName, partitionBackup.GetPartitionName(), BACKUP_ROW_BASED, getSegmentFiles(segment), nil)
			if err != nil {
				log.Error("Fail to bulkload the segment",
					zap.Error(err),
					zap.String("targetCollectionName", task.GetCollBackup().GetCollectionName()),
					zap.String("partitionName", partitionBackup.GetPartitionName()),
					zap.Int64("partitionId", segment.GetPartitionId()),
					zap.Int64("segmentId", segment.GetSegmentId()))
				task.ErrorMessage = err.Error()
				task.State = backuppb.LoadState_FAIL
				return task
			}
			for _, taskId := range taskIds {
				timeout, loadErr := b.watchImportState(ctx, taskId, BULKLOAD_SEGMENT_TIMEOUT, BULKLOAD_SEGMENT_SLEEP_INTERVAL)
				if loadErr != nil {
					log.Error("Fail or timeout to bulkload the segment",
						zap.Error(err),
						zap.Int64("taskId", taskId),
						zap.String("targetCollectionName", task.GetCollBackup().GetCollectionName()),
						zap.String("partitionName", partitionBackup.GetPartitionName()),
						zap.Int64("partitionId", segment.GetPartitionId()),
						zap.Int64("segmentId", segment.GetSegmentId()))
					task.ErrorMessage = err.Error()
					if timeout {
						task.State = backuppb.LoadState_TIMEOUT
					} else {
						task.State = backuppb.LoadState_FAIL
					}
					return task
				}
			}
		}
	}

	task.State = backuppb.LoadState_SUCCESS
	return task
}

func (b BackupContext) watchImportState(ctx context.Context, taskId int64, timeout int64, sleepSeconds int) (bool, error) {
	start := time.Now().Unix()
	for time.Now().Unix()-start < timeout {
		importTaskState, err := b.milvusClient.GetBulkloadState(ctx, taskId)
		switch importTaskState.State {
		case entity.BulkloadFailed:
			return false, err
		case entity.BulkloadPersisted:
		case entity.BulkloadCompleted:
			return false, nil
		default:
			time.Sleep(time.Second * time.Duration(sleepSeconds))
			continue
		}
	}
	return true, errors.New("import task timeout")
}

// todo: to complete after bulkload interface
func getSegmentFiles(segment *backuppb.SegmentBackupInfo) []string {

	return []string{}
}

func (b BackupContext) readBackup(ctx context.Context, backupName string) (*backuppb.BackupInfo, error) {
	backupMetaDirPath := BACKUP_PREFIX + SEPERATOR + backupName + SEPERATOR + META_PREFIX
	backupMetaPath := backupMetaDirPath + SEPERATOR + BACKUP_META_FILE
	collectionMetaPath := backupMetaDirPath + SEPERATOR + COLLECTION_META_FILE
	partitionMetaPath := backupMetaDirPath + SEPERATOR + PARTITION_META_FILE
	segmentMetaPath := backupMetaDirPath + SEPERATOR + SEGMENT_META_FILE

	backupMetaBytes, err := b.milvusStorageClient.Read(backupMetaPath)
	if err != nil {
		log.Error("Read backup meta failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	collectionBackupMetaBytes, err := b.milvusStorageClient.Read(collectionMetaPath)
	if err != nil {
		log.Error("Read collection meta failed", zap.String("path", collectionMetaPath), zap.Error(err))
		return nil, err
	}
	partitionBackupMetaBytes, err := b.milvusStorageClient.Read(partitionMetaPath)
	if err != nil {
		log.Error("Read partition meta failed", zap.String("path", partitionMetaPath), zap.Error(err))
		return nil, err
	}
	segmentBackupMetaBytes, err := b.milvusStorageClient.Read(segmentMetaPath)
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

func generateBackupDirPath(backupName string) string {
	return BACKUP_PREFIX + SEPERATOR + backupName + SEPERATOR
}

func (b BackupContext) readSegmentInfo(ctx context.Context, collecitonID int64, partitionID int64, segmentID int64, numOfRows int64) (*backuppb.SegmentBackupInfo, error) {
	segmentBackupInfo := backuppb.SegmentBackupInfo{
		SegmentId:    segmentID,
		CollectionId: collecitonID,
		PartitionId:  partitionID,
		NumOfRows:    numOfRows,
	}

	insertPath := fmt.Sprintf("%s/%v/%v/%v/", "files/insert_log", collecitonID, partitionID, segmentID)
	fieldsLogDir, _, _ := b.milvusStorageClient.ListWithPrefix(insertPath, false)
	insertLogs := make([]*datapb.FieldBinlog, 0)
	for _, fieldLogDir := range fieldsLogDir {
		binlogPaths, _, _ := b.milvusStorageClient.ListWithPrefix(fieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(fieldLogDir, insertPath, "", 1), SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*datapb.Binlog, 0)
		for _, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &datapb.Binlog{
				LogPath: binlogPath,
			})
		}
		insertLogs = append(insertLogs, &datapb.FieldBinlog{
			FieldID: fieldId,
			Binlogs: binlogs,
		})
	}

	deltaLogPath := fmt.Sprintf("%s/%v/%v/%v/", "files/delta_log", collecitonID, partitionID, segmentID)
	deltaFieldsLogDir, _, _ := b.milvusStorageClient.ListWithPrefix(deltaLogPath, false)
	deltaLogs := make([]*datapb.FieldBinlog, 0)
	for _, deltaFieldLogDir := range deltaFieldsLogDir {
		binlogPaths, _, _ := b.milvusStorageClient.ListWithPrefix(deltaFieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(deltaFieldLogDir, deltaLogPath, "", 1), SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*datapb.Binlog, 0)
		for _, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &datapb.Binlog{
				LogPath: binlogPath,
			})
		}
		deltaLogs = append(deltaLogs, &datapb.FieldBinlog{
			FieldID: fieldId,
			Binlogs: binlogs,
		})
	}
	if len(deltaLogs) == 0 {
		deltaLogs = append(deltaLogs, &datapb.FieldBinlog{
			FieldID: 0,
		})
	}

	statsLogPath := fmt.Sprintf("%s/%v/%v/%v/", "files/stats_log", collecitonID, partitionID, segmentID)
	statsFieldsLogDir, _, _ := b.milvusStorageClient.ListWithPrefix(statsLogPath, false)
	statsLogs := make([]*datapb.FieldBinlog, 0)
	for _, statsFieldLogDir := range statsFieldsLogDir {
		binlogPaths, _, _ := b.milvusStorageClient.ListWithPrefix(statsFieldLogDir, false)
		fieldIdStr := strings.Replace(strings.Replace(statsFieldLogDir, statsLogPath, "", 1), SEPERATOR, "", -1)
		fieldId, _ := strconv.ParseInt(fieldIdStr, 10, 64)
		binlogs := make([]*datapb.Binlog, 0)
		for _, binlogPath := range binlogPaths {
			binlogs = append(binlogs, &datapb.Binlog{
				LogPath: binlogPath,
			})
		}
		statsLogs = append(statsLogs, &datapb.FieldBinlog{
			FieldID: fieldId,
			Binlogs: binlogs,
		})
	}

	segmentBackupInfo.Binlogs = insertLogs
	segmentBackupInfo.Deltalogs = deltaLogs
	segmentBackupInfo.Statslogs = statsLogs
	return &segmentBackupInfo, nil
}
