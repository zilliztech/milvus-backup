package core

import (
	"context"
	"sync"

	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	backuppb "github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/proto/commonpb"
	"github.com/zilliztech/milvus-backup/internal/proto/schemapb"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
)

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
	//metaClient   etcdclient
	//storageClient minioclient
}

func (b *BackupContext) startConnection() error {
	milvusAddr := b.milvusSource.GetParams().ProxyCfg.NetworkAddress
	c, err := gomilvus.NewGrpcClient(b.ctx, milvusAddr)
	if err != nil {
		log.Error("failed to connect to milvus", zap.Error(err))
		return err
	}
	b.milvusClient = c
	//dataCoordClient, err := dcc.NewClient(b.ctx)
	//if err != nil {
	//	log.Error("failed to connect to milvus's datacoord", zap.Error(err))
	//	return err
	//}
	//b.milvusDataCoordClient = dataCoordClient
	//
	//rootCoordClient, err := rcc.NewClient(b.ctx)
	//if err != nil {
	//	log.Error("failed to connect to milvus's rootcoord", zap.Error(err))
	//	return err
	//}
	//b.milvusRootCoordClient = rootCoordClient
	return nil
}

func (b *BackupContext) closeConnection() error {
	err := b.milvusClient.Close()
	//err = b.milvusRootCoordClient.Stop()
	//err = b.milvusDataCoordClient.Stop()
	return err
}

func (b *BackupContext) GetMilvusSource() *MilvusSource {
	return b.milvusSource
}

func CreateBackupContext(ctx context.Context, params paramtable.ComponentParam) *BackupContext {
	return &BackupContext{
		ctx: ctx,
		milvusSource: &MilvusSource{
			params: params,
		},
	}
}

// todo refine error handle
// todo support get create backup progress
func (b BackupContext) CreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest) (*backuppb.CreateBackupResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	err := b.startConnection()
	if err != nil {
		return &backuppb.CreateBackupResponse{
			Status: &backuppb.Status{StatusCode: backuppb.StatusCode_ConnectFailed},
		}, nil
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

	collectionBackupInfos := make([]*backuppb.CollectionBackupInfo, len(toBackupCollections))
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
	paritionBackupInfos := make([]*backuppb.PartitionBackupInfo, len(toBackupCollections))
	for _, collection := range toBackupCollections {
		paritions, err := b.milvusClient.ShowPartitions(b.ctx, collection.Name)
		if err != nil {
			return nil, err
		}
		for _, partition := range paritions {
			paritionBackupInfos = append(paritionBackupInfos, &backuppb.PartitionBackupInfo{
				PartitionId:   partition.ID,
				PartitionName: partition.Name,
				CollectionId:  collection.ID,
				//SegmentBackups is now empty,
				//will fullfill after getting segments info and rearrange from level structure to tree structure
			})
		}
	}
	leveledBackupInfo.partitionLevel = &backuppb.PartitionLevelBackupInfo{
		Infos: paritionBackupInfos,
	}

	// 3, Flush

	// 4, get segment level meta
	// todo go sdk 没有ShowSegments方法

	// 5, wrap meta
	completeBackupInfo, err := levelToTree(leveledBackupInfo)
	if err != nil {
		return nil, err
	}
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
