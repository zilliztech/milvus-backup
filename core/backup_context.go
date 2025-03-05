package core

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
)

const (
	BackupName                    = "BACKUP_NAME"
	CollectionRenameSuffix        = "COLLECTION_RENAME_SUFFIX"
	RPS                           = 1000
	BackupSegmentGroupMaxSizeInMB = 256
	GcWarnMessage                 = "This warn won't fail the backup process. Pause GC can protect data not to be GCed during backup, it is necessary to backup very large data(cost more than a hour)."
)

// makes sure BackupContext implements `Backup`
var _ Backup = (*BackupContext)(nil)

type BackupContext struct {
	ctx context.Context
	// lock to make sure only one backup is creating or restoring
	mu      sync.Mutex
	started bool
	params  *paramtable.BackupParams

	// milvus client
	grpcClient client.Grpc

	// restful client
	restfulClient client.RestfulBulkInsert

	// data storage client
	milvusStorageClient storage.ChunkManager
	backupStorageClient storage.ChunkManager
	backupCopier        *storage.Copier
	restoreCopier       *storage.Copier

	milvusBucketName string
	backupBucketName string
	milvusRootPath   string
	backupRootPath   string

	meta *meta.MetaManager

	backupCollectionWorkerPool *common.WorkerPool
	backupCopyDataWorkerPool   *common.WorkerPool

	bulkinsertWorkerPools sync.Map
}

func CreateGrpcClient(params *paramtable.BackupParams) (client.Grpc, error) {
	cli, err := client.NewGrpc(&params.MilvusCfg)
	if err != nil {
		log.Error("failed to create milvus client", zap.Error(err))
		return nil, fmt.Errorf("failed to create milvus client: %w", err)
	}
	return cli, nil
}

func CreateRestfulClient(params *paramtable.BackupParams) (client.RestfulBulkInsert, error) {
	cli, err := client.NewRestful(&params.MilvusCfg)
	if err != nil {
		log.Error("failed to create restful client", zap.Error(err))
		return nil, fmt.Errorf("failed to create restful client: %w", err)
	}
	return cli, nil
}

func (b *BackupContext) Start() error {
	b.started = true
	log.Info(fmt.Sprintf("%+v", b.params.BackupCfg))
	log.Info(fmt.Sprintf("%+v", b.params.HTTPCfg))
	return nil
}

func (b *BackupContext) Close() error {
	b.started = false
	if b.grpcClient != nil {
		err := b.getMilvusClient().Close()
		return err
	}
	return nil
}

func CreateBackupContext(ctx context.Context, params *paramtable.BackupParams) *BackupContext {
	return &BackupContext{
		ctx:                   ctx,
		params:                params,
		milvusBucketName:      params.MinioCfg.BucketName,
		backupBucketName:      params.MinioCfg.BackupBucketName,
		milvusRootPath:        params.MinioCfg.RootPath,
		backupRootPath:        params.MinioCfg.BackupRootPath,
		bulkinsertWorkerPools: sync.Map{},
		meta:                  meta.NewMetaManager(),
	}
}

func (b *BackupContext) getMilvusClient() client.Grpc {
	if b.grpcClient == nil {
		milvusClient, err := CreateGrpcClient(b.params)
		if err != nil {
			log.Error("failed to initial milvus client", zap.Error(err))
			panic(err)
		}
		b.grpcClient = milvusClient
	}
	return b.grpcClient
}

func (b *BackupContext) getRestfulClient() client.RestfulBulkInsert {
	if b.restfulClient == nil {
		restfulClient, err := CreateRestfulClient(b.params)
		if err != nil {
			log.Error("failed to initial restful client", zap.Error(err))
			panic(err)
		}
		b.restfulClient = restfulClient
	}
	return b.restfulClient
}

func (b *BackupContext) getMilvusStorageClient() storage.ChunkManager {
	if b.milvusStorageClient == nil {
		minioEndPoint := b.params.MinioCfg.Address + ":" + b.params.MinioCfg.Port
		log.Debug("create milvus storage client",
			zap.String("address", minioEndPoint),
			zap.String("bucket", b.params.MinioCfg.BucketName),
			zap.String("backupBucket", b.params.MinioCfg.BackupBucketName))

		storageConfig := &storage.StorageConfig{
			StorageType:       b.params.MinioCfg.StorageType,
			Address:           minioEndPoint,
			BucketName:        b.params.MinioCfg.BucketName,
			AccessKeyID:       b.params.MinioCfg.AccessKeyID,
			SecretAccessKeyID: b.params.MinioCfg.SecretAccessKey,
			GcpCredentialJSON: b.params.MinioCfg.GcpCredentialJSON,
			UseSSL:            b.params.MinioCfg.UseSSL,
			UseIAM:            b.params.MinioCfg.UseIAM,
			IAMEndpoint:       b.params.MinioCfg.IAMEndpoint,
			RootPath:          b.params.MinioCfg.RootPath,
			CreateBucket:      true,
		}

		storageClient, err := storage.NewChunkManager(b.ctx, b.params, storageConfig)
		if err != nil {
			log.Error("failed to initial storage client", zap.Error(err))
			panic(err)
		}
		b.milvusStorageClient = storageClient
	}
	return b.milvusStorageClient
}

func (b *BackupContext) getBackupStorageClient() storage.ChunkManager {
	if b.backupStorageClient == nil {
		minioEndPoint := b.params.MinioCfg.BackupAddress + ":" + b.params.MinioCfg.BackupPort
		log.Debug("create backup storage client",
			zap.String("address", minioEndPoint),
			zap.String("bucket", b.params.MinioCfg.BucketName),
			zap.String("backupBucket", b.params.MinioCfg.BackupBucketName))

		storageConfig := &storage.StorageConfig{
			StorageType:       b.params.MinioCfg.BackupStorageType,
			Address:           minioEndPoint,
			BucketName:        b.params.MinioCfg.BackupBucketName,
			AccessKeyID:       b.params.MinioCfg.BackupAccessKeyID,
			SecretAccessKeyID: b.params.MinioCfg.BackupSecretAccessKey,
			GcpCredentialJSON: b.params.MinioCfg.BackupGcpCredentialJSON,
			UseSSL:            b.params.MinioCfg.BackupUseSSL,
			UseIAM:            b.params.MinioCfg.BackupUseIAM,
			IAMEndpoint:       b.params.MinioCfg.BackupIAMEndpoint,
			RootPath:          b.params.MinioCfg.BackupRootPath,
			CreateBucket:      true,
		}

		storageClient, err := storage.NewChunkManager(b.ctx, b.params, storageConfig)
		if err != nil {
			log.Error("failed to initial storage client", zap.Error(err))
			panic(err)
		}
		b.backupStorageClient = storageClient
	}
	return b.backupStorageClient
}

func (b *BackupContext) getBackupCopier() *storage.Copier {
	crossStorage := b.params.MinioCfg.CrossStorage
	if b.getBackupStorageClient().Config().StorageType != b.getMilvusStorageClient().Config().StorageType {
		crossStorage = true
	}
	if b.backupCopier == nil {
		b.backupCopier = storage.NewCopier(
			b.getMilvusStorageClient(),
			b.getBackupStorageClient(),
			storage.CopyOption{
				WorkerNum:    b.params.BackupCfg.BackupCopyDataParallelism,
				RPS:          RPS,
				CopyByServer: crossStorage,
			})
	}
	return b.backupCopier
}

func (b *BackupContext) getRestoreCopier() *storage.Copier {
	crossStorage := b.params.MinioCfg.CrossStorage
	// force set copyByServer is true if two storage type is different
	if b.getBackupStorageClient().Config().StorageType != b.getMilvusStorageClient().Config().StorageType {
		crossStorage = true
	}
	if b.restoreCopier == nil {
		b.restoreCopier = storage.NewCopier(
			b.getBackupStorageClient(),
			b.getMilvusStorageClient(),
			storage.CopyOption{
				WorkerNum:    b.params.BackupCfg.BackupCopyDataParallelism,
				RPS:          RPS,
				CopyByServer: crossStorage,
			})
	}
	return b.restoreCopier
}

func (b *BackupContext) getBackupCollectionWorkerPool() *common.WorkerPool {
	if b.backupCollectionWorkerPool == nil {
		wp, err := common.NewWorkerPool(b.ctx, b.params.BackupCfg.BackupCollectionParallelism, RPS)
		if err != nil {
			log.Error("failed to initial collection backup worker pool", zap.Error(err))
			panic(err)
		}
		b.backupCollectionWorkerPool = wp
		b.backupCollectionWorkerPool.Start()
	}
	return b.backupCollectionWorkerPool
}

func (b *BackupContext) getCopyDataWorkerPool() *common.WorkerPool {
	if b.backupCopyDataWorkerPool == nil {
		wp, err := common.NewWorkerPool(b.ctx, b.params.BackupCfg.BackupCopyDataParallelism, RPS)
		if err != nil {
			log.Error("failed to initial copy data worker pool", zap.Error(err))
			panic(err)
		}
		b.backupCopyDataWorkerPool = wp
		b.backupCopyDataWorkerPool.Start()
	}
	return b.backupCopyDataWorkerPool
}

func (b *BackupContext) getRestoreWorkerPool(id string) *common.WorkerPool {
	// TODO DO NOT use pool of worker pool
	if pool, exist := b.bulkinsertWorkerPools.Load(id); exist {
		return pool.(*common.WorkerPool)
	} else {
		wp, err := common.NewWorkerPool(b.ctx, b.params.BackupCfg.RestoreParallelism, RPS)
		if err != nil {
			log.Error("failed to initial copy data worker pool", zap.Error(err))
			panic(err)
		}
		wp.Start()
		b.bulkinsertWorkerPools.Store(id, wp)

		return wp
	}
}

func (b *BackupContext) cleanRestoreWorkerPool(id string) {
	// TODO DO NOT use pool of worker pool
	if _, exist := b.bulkinsertWorkerPools.Load(id); exist {
		b.bulkinsertWorkerPools.Delete(id)
	}
}

func (b *BackupContext) GetBackup(ctx context.Context, request *backuppb.GetBackupRequest) *backuppb.BackupInfoResponse {
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
		}
	}

	if request.GetBackupId() == "" && request.GetBackupName() == "" {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "empty backup name and backup id, please set a backup name or id"
	} else if request.GetBackupId() != "" {
		backupInfo := b.meta.GetFullMeta(request.GetBackupId())
		resp.Code = backuppb.ResponseCode_Success
		resp.Msg = "success"
		resp.Data = backupInfo
	} else if request.GetBackupName() != "" {
		backupInfo := b.meta.GetBackupByName(request.GetBackupName())
		if backupInfo != nil {
			fullBackupInfo := b.meta.GetFullMeta(backupInfo.Id)
			resp.Code = backuppb.ResponseCode_Success
			resp.Msg = "success"
			resp.Data = fullBackupInfo
		} else {
			var backupBucketName string
			var backupPath string
			if request.GetBucketName() == "" || request.GetPath() == "" {
				backupBucketName = b.backupBucketName
				backupPath = b.backupRootPath + meta.SEPERATOR + request.GetBackupName()
			} else {
				backupBucketName = request.GetBucketName()
				backupPath = request.GetPath() + meta.SEPERATOR + request.GetBackupName()
			}
			backup, err := b.readBackupV2(ctx, backupBucketName, backupPath)
			if err != nil {
				log.Warn("Fail to read backup",
					zap.String("backupBucketName", backupBucketName),
					zap.String("backupPath", backupPath),
					zap.Error(err))
				resp.Code = backuppb.ResponseCode_Fail
				resp.Msg = err.Error()
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

	if request.WithoutDetail {
		resp = meta.SimpleBackupResponse(resp)
	}

	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("finish GetBackupRequest",
			zap.String("requestId", request.GetRequestId()),
			zap.String("backupName", request.GetBackupName()),
			zap.String("backupId", request.GetBackupId()),
			zap.String("bucketName", request.GetBucketName()),
			zap.String("path", request.GetPath()))
	} else {
		log.Info("finish GetBackupRequest",
			zap.String("requestId", request.GetRequestId()),
			zap.String("backupName", request.GetBackupName()),
			zap.String("backupId", request.GetBackupId()),
			zap.String("bucketName", request.GetBucketName()),
			zap.String("path", request.GetPath()))
	}

	return resp
}

func (b *BackupContext) ListBackups(ctx context.Context, request *backuppb.ListBackupsRequest) *backuppb.ListBackupsResponse {
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
	backupPaths, _, err := b.getBackupStorageClient().ListWithPrefix(ctx, b.backupBucketName, b.backupRootPath+meta.SEPERATOR, false)
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
			BackupName: meta.BackupPathToName(b.backupRootPath, backupPath),
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

func (b *BackupContext) DeleteBackup(ctx context.Context, request *backuppb.DeleteBackupRequest) *backuppb.DeleteBackupResponse {
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
	// always trigger a remove to make sure it is deleted
	err := b.getBackupStorageClient().RemoveWithPrefix(ctx, b.backupBucketName, meta.BackupDirPath(b.backupRootPath, request.GetBackupName()))

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

	if err != nil {
		log.Error("Fail to delete backup", zap.String("backupName", request.GetBackupName()), zap.Error(err))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = getResp.GetMsg()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	log.Info("return DeleteBackupResponse",
		zap.String("requestId", resp.GetRequestId()),
		zap.Int32("code", int32(resp.GetCode())))
	return resp
}

// read backup
// 1. first read backup from full meta
// 2. if full meta not exist, which means backup is a very old version, read from seperate files
func (b *BackupContext) readBackupV2(ctx context.Context, bucketName string, backupPath string) (*backuppb.BackupInfo, error) {
	backupMetaDirPath := backupPath + meta.SEPERATOR + meta.META_PREFIX
	fullMetaPath := backupMetaDirPath + meta.SEPERATOR + meta.FULL_META_FILE
	exist, err := b.getBackupStorageClient().Exist(ctx, bucketName, fullMetaPath)
	if err != nil {
		log.Error("check full meta file failed", zap.String("path", fullMetaPath), zap.Error(err))
		return nil, err
	}
	if exist {
		backupMetaBytes, err := b.getBackupStorageClient().Read(ctx, bucketName, fullMetaPath)
		if err != nil {
			log.Error("Read backup meta failed", zap.String("path", fullMetaPath), zap.Error(err))
			return nil, err
		}
		backupInfo := &backuppb.BackupInfo{}
		err = json.Unmarshal(backupMetaBytes, backupInfo)
		if err != nil {
			log.Error("Read backup meta failed", zap.String("path", fullMetaPath), zap.Error(err))
			return nil, err
		}
		return backupInfo, nil
	} else {
		return b.readBackup(ctx, bucketName, backupPath)
	}
}

// read backup from seperated meta files
func (b *BackupContext) readBackup(ctx context.Context, bucketName string, backupPath string) (*backuppb.BackupInfo, error) {
	backupMetaDirPath := backupPath + meta.SEPERATOR + meta.META_PREFIX
	backupMetaPath := backupMetaDirPath + meta.SEPERATOR + meta.BACKUP_META_FILE
	collectionMetaPath := backupMetaDirPath + meta.SEPERATOR + meta.COLLECTION_META_FILE
	partitionMetaPath := backupMetaDirPath + meta.SEPERATOR + meta.PARTITION_META_FILE
	segmentMetaPath := backupMetaDirPath + meta.SEPERATOR + meta.SEGMENT_META_FILE

	exist, err := b.getBackupStorageClient().Exist(ctx, bucketName, backupMetaPath)
	if err != nil {
		log.Error("check backup meta file failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	if !exist {
		log.Warn("read backup meta file not exist", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}

	backupMetaBytes, err := b.getBackupStorageClient().Read(ctx, bucketName, backupMetaPath)
	if err != nil {
		log.Error("Read backup meta failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	collectionBackupMetaBytes, err := b.getBackupStorageClient().Read(ctx, bucketName, collectionMetaPath)
	if err != nil {
		log.Error("Read collection meta failed", zap.String("path", collectionMetaPath), zap.Error(err))
		return nil, err
	}
	partitionBackupMetaBytes, err := b.getBackupStorageClient().Read(ctx, bucketName, partitionMetaPath)
	if err != nil {
		log.Error("Read partition meta failed", zap.String("path", partitionMetaPath), zap.Error(err))
		return nil, err
	}
	segmentBackupMetaBytes, err := b.getBackupStorageClient().Read(ctx, bucketName, segmentMetaPath)
	if err != nil {
		log.Error("Read segment meta failed", zap.String("path", segmentMetaPath), zap.Error(err))
		return nil, err
	}

	completeBackupMetas := &meta.BackupMetaBytes{
		BackupMetaBytes:     backupMetaBytes,
		CollectionMetaBytes: collectionBackupMetaBytes,
		PartitionMetaBytes:  partitionBackupMetaBytes,
		SegmentMetaBytes:    segmentBackupMetaBytes,
	}

	backupInfo, err := meta.Deserialize(completeBackupMetas)
	if err != nil {
		log.Error("Fail to deserialize backup info", zap.String("backupPath", backupPath), zap.Error(err))
		return nil, err
	}

	return backupInfo, nil
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

	task := b.meta.GetRestoreTask(request.GetId())
	if task != nil {
		progress := int32(float32(task.GetRestoredSize()) * 100 / float32(task.GetToRestoreSize()))
		// don't return zero
		if progress == 0 {
			progress = 1
		}
		task.Progress = progress
		resp.Code = backuppb.ResponseCode_Success
		resp.Msg = "success"
		resp.Data = task
		return resp
	} else {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "restore id not exist in context"
		return resp
	}
}

func (b *BackupContext) Check(ctx context.Context) string {
	version, err := b.getMilvusClient().GetVersion(ctx)
	if err != nil {
		return "Failed to connect to milvus " + err.Error()
	}

	info := fmt.Sprintf(
		"Milvus version: %s\n"+
			"Storage:\n"+
			"milvus-bucket: %s\n"+
			"milvus-rootpath: %s\n"+
			"backup-bucket: %s\n"+
			"backup-rootpath: %s\n",
		version, b.milvusBucketName, b.milvusRootPath, b.backupBucketName, b.backupRootPath)

	milvusFiles, _, err := b.getMilvusStorageClient().ListWithPrefix(ctx, b.milvusBucketName, b.milvusRootPath+meta.SEPERATOR, false)
	if err != nil {
		return "Failed to connect to storage milvus path\n" + info + err.Error()
	}

	if len(milvusFiles) == 0 {
		return "Milvus storage is empty. Please verify whether your cluster is really empty. If not, the configs(minio address, port, bucket, rootPath) may be wrong\n" + info
	}

	_, _, err = b.getBackupStorageClient().ListWithPrefix(ctx, b.backupBucketName, b.backupRootPath+meta.SEPERATOR, false)
	if err != nil {
		return "Failed to connect to storage backup path " + info + err.Error()
	}

	checkSrcPath := path.Join(b.milvusRootPath, "milvus_backup_check_src_"+fmt.Sprint(time.Now().Unix()))
	checkDstPath := path.Join(b.backupRootPath, "milvus_backup_check_dst_"+fmt.Sprint(time.Now().Unix()))

	err = b.getMilvusStorageClient().Write(ctx, b.milvusBucketName, checkSrcPath, []byte{1})
	if err != nil {
		return "Failed to connect to storage milvus path\n" + info + err.Error()
	}
	defer func() {
		b.getMilvusStorageClient().Remove(ctx, b.milvusBucketName, checkSrcPath)
	}()

	log.Debug("check copy", zap.String("srcBucket", b.milvusBucketName), zap.String("destBucket", b.backupBucketName), zap.String("key", checkSrcPath), zap.String("destKey", checkDstPath))
	err = b.getBackupCopier().Copy(ctx, checkSrcPath, checkDstPath, b.milvusBucketName, b.backupBucketName)
	if err != nil {
		return "Failed to copy file from milvus storage to backup storage\n" + info + err.Error()
	}
	defer func() {
		b.getBackupStorageClient().Remove(ctx, b.backupBucketName, checkDstPath)
	}()

	return "Succeed to connect to milvus and storage.\n" + info
}
