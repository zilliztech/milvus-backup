package core

import (
	"context"
	"github.com/zilliztech/milvus-backup/internal/proto/datapb"
	"github.com/zilliztech/milvus-backup/internal/util/typeutil"
	"strconv"
	"strings"
	"sync"
	"time"

	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	backuppb "github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/proto/commonpb"
	"github.com/zilliztech/milvus-backup/internal/proto/schemapb"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"

	dcc "github.com/zilliztech/milvus-backup/internal/distributed/datacoord/client"
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
	milvusDataCoordClient *dcc.Client
	// milvus data storage client
	milvusStorageClient MilvusStorage
	started             bool
}

const (
	BACKUP_PREFIX = "backup"
)

func (b *BackupContext) Start() error {
	// start milvus go SDK client
	c, err := gomilvus.NewGrpcClient(b.ctx, b.milvusSource.GetProxyAddr())
	if err != nil {
		log.Error("failed to connect to milvus", zap.Error(err))
		return err
	}
	b.milvusClient = c

	// start milvus datacoord client
	dataCoordClient, err := dcc.NewClient(b.ctx, b.milvusSource.GetDatacoordAddr())
	if err != nil {
		log.Error("failed to connect to milvus's datacoord", zap.Error(err))
		return err
	}
	b.milvusDataCoordClient = dataCoordClient
	b.milvusDataCoordClient.Init()
	b.milvusDataCoordClient.Start()

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

	backupDirExist, err := b.milvusStorageClient.Exist(BACKUP_PREFIX)
	if err != nil {
		log.Error("failed to check backup dir exist", zap.Error(err))
		return err
	}
	if !backupDirExist {
		err = b.milvusStorageClient.Write(BACKUP_PREFIX, nil)
		if err != nil {
			log.Error("failed to create backup dir", zap.Error(err))
			return err
		}
	}
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
	err := b.milvusClient.Close()
	//err = b.milvusRootCoordClient.Stop()
	err = b.milvusDataCoordClient.Stop()
	return err
}

func (b *BackupContext) GetMilvusSource() *MilvusSource {
	return b.milvusSource
}

func CreateBackupContext(ctx context.Context, params paramtable.ComponentParam) *BackupContext {
	var Params paramtable.GrpcServerConfig
	Params.InitOnce(typeutil.ProxyRole)
	milvusAddr := Params.GetAddress()

	var Params2 paramtable.GrpcServerConfig
	Params2.InitOnce(typeutil.DataCoordRole)
	milvusDatacoordAddr := Params2.GetAddress()

	return &BackupContext{
		ctx: ctx,
		milvusSource: &MilvusSource{
			params:        params,
			proxyAddr:     milvusAddr,
			datacoordAddr: milvusDatacoordAddr,
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

	leveledBackupInfo := &LeveledBackupInfo{}

	// 1, get collection level meta
	collections, err := b.milvusClient.ListCollections(b.ctx)
	if err != nil {
		log.Error("Fail in ListCollections", zap.Error(err))
		return &backuppb.CreateBackupResponse{
			Status: &backuppb.Status{
				StatusCode: backuppb.StatusCode_UnexpectedError,
				Reason:     err.Error(),
			},
		}, nil
	}

	toBackupCollections := func(collections []*entity.Collection, collectionNames []string) []*entity.Collection {
		if collections == nil || len(collectionNames) == 0 {
			return collections
		}
		res := make([]*entity.Collection, len(collections))
		collectionDict := make(map[string]bool, len(collectionNames))
		for _, collectionName := range collectionNames {
			collectionDict[collectionName] = true
		}
		for _, collection := range collections {
			if collectionDict[collection.Name] == true {
				res = append(res, collection)
			}
		}
		return res
	}(collections, request.GetCollectionNames())

	log.Info("collections to backup", zap.Any("collections", toBackupCollections))

	collectionBackupInfos := make([]*backuppb.CollectionBackupInfo, 0)
	for _, collection := range toBackupCollections {
		// list collection result is not complete
		completeCollection, err := b.milvusClient.DescribeCollection(b.ctx, collection.Name)
		if err != nil {
			return nil, err
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
			return nil, err
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
	// todo go sdk 没有ShowSegments方法
	segmentBackupInfos := make([]*backuppb.SegmentBackupInfo, 0)
	for _, part := range partitionBackupInfos {
		collectionID := part.GetCollectionId()
		partitionID := part.GetPartitionId()
		resp, err := b.milvusDataCoordClient.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{
			CollectionID: collectionID,
			PartitionID:  partitionID,
		})
		if err != nil || resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, err
		}
		for _, binlogs := range resp.GetBinlogs() {
			segmentBackupInfos = append(segmentBackupInfos, &backuppb.SegmentBackupInfo{
				SegmentId:    binlogs.GetSegmentID(),
				CollectionId: collectionID,
				PartitionId:  partitionID,
				NumOfRows:    binlogs.GetNumOfRows(),
				Binlogs:      binlogs.GetFieldBinlogs(),
				Deltalogs:    binlogs.GetDeltalogs(),
				Statslogs:    binlogs.GetStatslogs(),
			})
		}
	}
	leveledBackupInfo.segmentLevel = &backuppb.SegmentLevelBackupInfo{
		Infos: segmentBackupInfos,
	}

	// 5, copy data

	// 6,wrap meta
	completeBackupInfo, err := levelToTree(leveledBackupInfo)
	if err != nil {
		return nil, err
	}

	completeBackupInfo.BackupStatus = backuppb.StatusCode_Success
	completeBackupInfo.BackupTimestamp = uint64(time.Now().Unix())
	completeBackupInfo.Name = request.BackupName
	// todo generate ID
	completeBackupInfo.Id = 0

	output, _ := serialize(completeBackupInfo)
	log.Info(string(output.BackupMetaBytes))
	log.Info(string(output.CollectionMetaBytes))
	log.Info(string(output.PartitionMetaBytes))
	log.Info(string(output.SegmentMetaBytes))

	return &backuppb.CreateBackupResponse{
		Status: &backuppb.Status{
			StatusCode: backuppb.StatusCode_Success,
		},
		BackupInfo: completeBackupInfo,
	}, nil
}

func (b BackupContext) GetBackup(ctx context.Context, request *backuppb.GetBackupRequest) (*backuppb.GetBackupResponse, error) {
	// 1, trigger inner sync to get the newest backup list in the milvus cluster

	// 2, get wanted backup
	panic("implement me")
}

func (b BackupContext) ListBackups(ctx context.Context, request *backuppb.ListBackupsRequest) (*backuppb.ListBackupsResponse, error) {
	// 1, trigger inner sync to get the newest backup list in the milvus cluster

	// 2, list wanted backup
	panic("implement me")
}

func (b BackupContext) DeleteBackup(ctx context.Context, request *backuppb.DeleteBackupRequest) (*backuppb.DeleteBackupResponse, error) {
	// 1, delete the backup
	panic("implement me")
}

func (b BackupContext) LoadBackup(ctx context.Context, request *backuppb.LoadBackupRequest) (*backuppb.LoadBackupResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 1, validate

	// 2, create collections

	// 3, execute bulkload

	// 4, collect stats and return report
	panic("implement me")
}
