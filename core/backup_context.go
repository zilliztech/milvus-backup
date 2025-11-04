package core

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const (
	BackupName = "BACKUP_NAME"
)

type BackupContext struct {
	ctx context.Context
	// lock to make sure only one backup is creating or restoring
	mu      sync.Mutex
	started bool
	params  *paramtable.BackupParams

	// milvus client
	grpcClient    milvus.Grpc
	restfulClient milvus.Restful

	// data storage client
	milvusStorageClient storage.Client
	backupStorageClient storage.Client

	milvusBucketName string
	backupBucketName string
	milvusRootPath   string
	backupRootPath   string

	meta *meta.MetaManager
}

func CreateGrpcClient(params *paramtable.BackupParams) (milvus.Grpc, error) {
	cli, err := milvus.NewGrpc(&params.MilvusCfg)
	if err != nil {
		log.Error("failed to create milvus client", zap.Error(err))
		return nil, fmt.Errorf("failed to create milvus client: %w", err)
	}
	return cli, nil
}

func CreateRestfulClient(params *paramtable.BackupParams) (milvus.Restful, error) {
	cli, err := milvus.NewRestful(&params.MilvusCfg)
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
		ctx:              ctx,
		params:           params,
		milvusBucketName: params.MinioCfg.BucketName,
		backupBucketName: params.MinioCfg.BackupBucketName,
		milvusRootPath:   params.MinioCfg.RootPath,
		backupRootPath:   params.MinioCfg.BackupRootPath,
		meta:             meta.NewMetaManager(),
	}
}

func (b *BackupContext) getMilvusClient() milvus.Grpc {
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

func (b *BackupContext) getRestfulClient() milvus.Restful {
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

func (b *BackupContext) getMilvusStorageClient() storage.Client {
	if b.milvusStorageClient == nil {
		cli, err := storage.NewMilvusStorage(b.ctx, &b.params.MinioCfg)
		if err != nil {
			log.Error("failed to initial milvus storage client", zap.Error(err))
			panic(err)
		}

		b.milvusStorageClient = cli
	}
	return b.milvusStorageClient
}

func (b *BackupContext) getBackupStorageClient() storage.Client {
	if b.backupStorageClient == nil {
		cli, err := storage.NewBackupStorage(b.ctx, &b.params.MinioCfg)
		if err != nil {
			log.Error("failed to initial backup storage client", zap.Error(err))
			panic(err)
		}

		b.backupStorageClient = cli
	}
	return b.backupStorageClient
}

func (b *BackupContext) GetBackup(ctx context.Context, request *backuppb.GetBackupRequest) *backuppb.BackupInfoResponse {
	if request.GetRequestId() == "" {
		request.RequestId = uuid.NewString()
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
			if resp.Data.GetStateCode() == backuppb.BackupTaskStateCode_BACKUP_SUCCESS {
				metaDir := mpath.MetaDir(b.backupRootPath, request.GetBackupName())
				_, sizes, err := storage.ListPrefixFlat(ctx, b.getBackupStorageClient(), metaDir, true)
				if err != nil {
					log.Warn("Fail to list backup directory", zap.Error(err))
					resp.Code = backuppb.ResponseCode_Fail
					resp.Msg = err.Error()
				} else {
					resp.Data.MetaSize = lo.Sum(sizes)
				}
			}
		} else {
			backupDir := mpath.BackupDir(b.backupRootPath, request.GetBackupName())
			backup, err := meta.Read(ctx, b.getBackupStorageClient(), backupDir)
			if err != nil {
				log.Warn("Fail to read backup",
					zap.String("backupBucketName", b.params.MinioCfg.BackupBucketName),
					zap.String("backup_dir", backupDir),
					zap.Error(err))
				resp.Code = backuppb.ResponseCode_Fail
				resp.Msg = err.Error()
			}

			resp.Data = backup
			if backup == nil {
				resp.Code = backuppb.ResponseCode_Request_Object_Not_Found
				resp.Msg = "not found"
			} else {
				metaDir := mpath.MetaDir(b.backupRootPath, request.GetBackupName())
				_, sizes, err := storage.ListPrefixFlat(ctx, b.getBackupStorageClient(), metaDir, true)
				if err != nil {
					log.Warn("Fail to list backup directory", zap.Error(err))
					resp.Code = backuppb.ResponseCode_Fail
					resp.Msg = err.Error()
				} else {
					resp.Data.MetaSize = lo.Sum(sizes)
				}
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

func (b *BackupContext) GetRestore(ctx context.Context, request *backuppb.GetRestoreStateRequest) *backuppb.RestoreBackupResponse {
	if request.GetRequestId() == "" {
		request.RequestId = uuid.NewString()
	}
	log.Info("receive GetRestoreStateRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("task_id", request.GetId()))

	resp := &backuppb.RestoreBackupResponse{RequestId: request.GetRequestId()}

	if request.GetId() == "" {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "empty restore id"
		return resp
	}

	taskView, err := taskmgr.DefaultMgr.GetRestoreTask(request.GetId())
	log.Warn("get restore task", zap.String("task_id", request.GetId()), zap.Error(err))
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "restore id not exist in context"
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.RestoreTaskViewToResp(taskView)
	return resp
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

	milvusFiles, _, err := storage.ListPrefixFlat(ctx, b.getMilvusStorageClient(), b.milvusRootPath+meta.SEPERATOR, false)
	if err != nil {
		return "Failed to connect to storage milvus path\n" + info + err.Error()
	}

	if len(milvusFiles) == 0 {
		return "Milvus storage is empty. Please verify whether your cluster is really empty. If not, the configs(minio address, port, bucket, rootPath) may be wrong\n" + info
	}

	_, _, err = storage.ListPrefixFlat(ctx, b.getBackupStorageClient(), b.backupRootPath+meta.SEPERATOR, false)
	if err != nil {
		return "Failed to connect to storage backup path " + info + err.Error()
	}

	checkSrcPath := path.Join(b.milvusRootPath, "milvus_backup_check_src_"+fmt.Sprint(time.Now().Unix()))
	checkDstPath := path.Join(b.backupRootPath, "milvus_backup_check_dst_"+fmt.Sprint(time.Now().Unix()))

	if err = storage.Write(ctx, b.getMilvusStorageClient(), checkSrcPath, []byte{1}); err != nil {
		return "Failed to connect to storage milvus path\n" + info + err.Error()
	}
	defer func() {
		if err := storage.DeletePrefix(ctx, b.getMilvusStorageClient(), checkSrcPath); err != nil {
			log.Error("Failed to delete check file", zap.String("path", checkSrcPath), zap.Error(err))
		}
	}()

	log.Debug("check copy", zap.String("srcBucket", b.milvusBucketName), zap.String("destBucket", b.backupBucketName), zap.String("key", checkSrcPath), zap.String("destKey", checkDstPath))
	crossStorage := b.params.MinioCfg.CrossStorage
	if b.getBackupStorageClient().Config().Provider != b.getMilvusStorageClient().Config().Provider {
		crossStorage = true
	}
	opt := storage.CopyPrefixOpt{
		Src:          b.getMilvusStorageClient(),
		Dest:         b.getBackupStorageClient(),
		SrcPrefix:    checkSrcPath,
		DestPrefix:   checkDstPath,
		Sem:          semaphore.NewWeighted(1),
		CopyByServer: crossStorage,
	}
	task := storage.NewCopyPrefixTask(opt)
	if err := task.Execute(ctx); err != nil {
		return "Failed to copy file from milvus storage to backup storage\n" + info + err.Error()
	}

	defer func() {
		if err := storage.DeletePrefix(ctx, b.getBackupStorageClient(), checkDstPath); err != nil {
			log.Error("Failed to delete check file", zap.String("path", checkDstPath), zap.Error(err))
		}
	}()

	return "Succeed to connect to milvus and storage.\n" + info
}
