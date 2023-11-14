package core

import (
	"context"
	"errors"
	"fmt"
	"sync"

	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
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
	milvusClient *MilvusClient

	// data storage client
	storageClient    *storage.ChunkManager
	milvusBucketName string
	backupBucketName string
	milvusRootPath   string
	backupRootPath   string

	backupNameIdDict sync.Map //map[string]string
	backupTasks      sync.Map //map[string]*backuppb.BackupInfo

	restoreTasks map[string]*backuppb.RestoreBackupTask

	bulkinsertWorkerPool *common.WorkerPool
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
	minioClient, err := storage.NewChunkManager(ctx, params)
	return minioClient, err
}

func (b *BackupContext) Start() error {
	b.backupTasks = sync.Map{}
	b.backupNameIdDict = sync.Map{}
	b.restoreTasks = make(map[string]*backuppb.RestoreBackupTask)
	b.started = true
	log.Info(fmt.Sprintf("%+v", b.params.BackupCfg))
	log.Info(fmt.Sprintf("%+v", b.params.HTTPCfg))
	return nil
}

func (b *BackupContext) Close() error {
	b.started = false
	if b.milvusClient != nil {
		err := b.getMilvusClient().Close()
		return err
	}
	return nil
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

func (b *BackupContext) getMilvusClient() *MilvusClient {
	if b.milvusClient == nil {
		milvusClient, err := CreateMilvusClient(b.ctx, b.params)
		if err != nil {
			log.Error("failed to initial milvus client", zap.Error(err))
			panic(err)
		}
		b.milvusClient = &MilvusClient{
			client: milvusClient,
		}
	}
	return b.milvusClient
}

func (b *BackupContext) getStorageClient() storage.ChunkManager {
	if b.storageClient == nil {
		storageClient, err := CreateStorageClient(b.ctx, b.params)
		if err != nil {
			log.Error("failed to initial storage client", zap.Error(err))
			panic(err)
		}
		b.storageClient = &storageClient
	}
	return *b.storageClient
}

func (b *BackupContext) getRestoreWorkerPool() *common.WorkerPool {
	if b.bulkinsertWorkerPool == nil {
		wp, err := common.NewWorkerPool(b.ctx, b.params.BackupCfg.RestoreParallelism, RPS)
		if err != nil {
			log.Error("failed to initial copy data woker pool", zap.Error(err))
			panic(err)
		}
		b.bulkinsertWorkerPool = wp
		b.bulkinsertWorkerPool.Start()
	}
	return b.bulkinsertWorkerPool
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
		resp.Msg = "empty backup name and backup id"
	} else if request.GetBackupId() != "" {
		if value, ok := b.backupTasks.Load(request.GetBackupId()); ok {
			resp.Code = backuppb.ResponseCode_Success
			resp.Msg = "success"
			resp.Data = value.(*backuppb.BackupInfo)
		}
	} else if request.GetBackupName() != "" {
		if id, ok := b.backupNameIdDict.Load(request.GetBackupName()); ok {
			resp.Code = backuppb.ResponseCode_Success
			resp.Msg = "success"
			backup, ok := b.backupTasks.Load(id)
			if ok {
				resp.Data = backup.(*backuppb.BackupInfo)
			}
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
		resp = SimpleBackupResponse(resp)
	}

	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("finish GetBackupRequest",
			zap.String("requestId", request.GetRequestId()),
			zap.String("backupName", request.GetBackupName()),
			zap.String("backupId", request.GetBackupId()),
			zap.String("bucketName", request.GetBucketName()),
			zap.String("path", request.GetPath()),
			zap.Any("resp", resp))
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
	backupPaths, _, err := b.getStorageClient().ListWithPrefix(ctx, b.backupBucketName, b.backupRootPath+SEPERATOR, false)
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

	err := b.getStorageClient().RemoveWithPrefix(ctx, b.backupBucketName, BackupDirPath(b.backupRootPath, request.GetBackupName()))

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

func (b *BackupContext) readBackup(ctx context.Context, bucketName string, backupPath string) (*backuppb.BackupInfo, error) {
	backupMetaDirPath := backupPath + SEPERATOR + META_PREFIX
	backupMetaPath := backupMetaDirPath + SEPERATOR + BACKUP_META_FILE
	collectionMetaPath := backupMetaDirPath + SEPERATOR + COLLECTION_META_FILE
	partitionMetaPath := backupMetaDirPath + SEPERATOR + PARTITION_META_FILE
	segmentMetaPath := backupMetaDirPath + SEPERATOR + SEGMENT_META_FILE

	exist, err := b.getStorageClient().Exist(ctx, bucketName, backupMetaPath)
	if err != nil {
		log.Error("check backup meta file failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	if !exist {
		log.Warn("read backup meta file not exist", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}

	backupMetaBytes, err := b.getStorageClient().Read(ctx, bucketName, backupMetaPath)
	if err != nil {
		log.Error("Read backup meta failed", zap.String("path", backupMetaPath), zap.Error(err))
		return nil, err
	}
	collectionBackupMetaBytes, err := b.getStorageClient().Read(ctx, bucketName, collectionMetaPath)
	if err != nil {
		log.Error("Read collection meta failed", zap.String("path", collectionMetaPath), zap.Error(err))
		return nil, err
	}
	partitionBackupMetaBytes, err := b.getStorageClient().Read(ctx, bucketName, partitionMetaPath)
	if err != nil {
		log.Error("Read partition meta failed", zap.String("path", partitionMetaPath), zap.Error(err))
		return nil, err
	}
	segmentBackupMetaBytes, err := b.getStorageClient().Read(ctx, bucketName, segmentMetaPath)
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

	paths, _, err := b.getStorageClient().ListWithPrefix(ctx, b.milvusBucketName, b.milvusRootPath+SEPERATOR, false)
	if err != nil {
		return "Failed to connect to storage milvus path\n" + info + err.Error()
	}

	if len(paths) == 0 {
		return "Milvus storage is empty. Please verify whether your cluster is really empty. If not, the configs(minio address, port, bucket, rootPath) may be wrong\n" + info
	}

	paths, _, err = b.getStorageClient().ListWithPrefix(ctx, b.backupBucketName, b.backupRootPath+SEPERATOR, false)
	if err != nil {
		return "Failed to connect to storage backup path " + info + err.Error()
	}

	CHECK_PATH := ".milvus_backup_check"

	err = b.getStorageClient().Write(ctx, b.milvusBucketName, b.milvusRootPath+SEPERATOR+CHECK_PATH, []byte{1})
	if err != nil {
		return "Failed to connect to storage milvus path\n" + info + err.Error()
	}
	defer func() {
		b.getStorageClient().Remove(ctx, b.milvusBucketName, b.milvusRootPath+SEPERATOR+CHECK_PATH)
	}()

	err = b.getStorageClient().Copy(ctx, b.milvusBucketName, b.backupBucketName, b.milvusRootPath+SEPERATOR+CHECK_PATH, b.backupRootPath+SEPERATOR+CHECK_PATH)
	if err != nil {
		return "Failed to copy file from milvus storage to backup storage\n" + info + err.Error()
	}
	defer func() {
		b.getStorageClient().Remove(ctx, b.backupBucketName, b.backupRootPath+SEPERATOR+CHECK_PATH)
	}()

	return "Succeed to connect to milvus and storage.\n" + info
}
