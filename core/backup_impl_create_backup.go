package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

func (b *BackupContext) CreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest) *backuppb.BackupInfoResponse {
	if request.GetRequestId() == "" {
		request.RequestId = uuid.NewString()
	}
	log.Info("receive CreateBackupRequest", zap.Any("request", request))

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

	info := &backuppb.BackupInfo{
		Id:            request.GetRequestId(),
		StateCode:     backuppb.BackupTaskStateCode_BACKUP_INITIAL,
		StartTime:     time.Now().UnixNano() / int64(time.Millisecond),
		Name:          request.GetBackupName(),
		MilvusVersion: milvusVersion,
	}
	b.meta.AddBackup(info.GetId(), info)

	if request.Async {
		go b.executeCreateBackup(ctx, request, info)
		asyncResp := &backuppb.BackupInfoResponse{
			RequestId: request.GetRequestId(),
			Code:      backuppb.ResponseCode_Success,
			Msg:       "create backup is executing asynchronously",
			Data:      info,
		}
		return asyncResp
	} else {
		err := b.executeCreateBackup(ctx, request, info)
		resp.Data = b.meta.GetBackup(info.GetId())
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

func (b *BackupContext) executeCreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest, backupInfo *backuppb.BackupInfo) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.meta.UpdateBackup(backupInfo.GetId(), meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_EXECUTING))

	backupRootPath := b.params.MinioCfg.BackupRootPath
	if request.GetBackupRootPath() != "" {
		log.Info("use backup root path from request", zap.String("path", request.GetBackupRootPath()))
		backupRootPath = request.GetBackupRootPath()
	}
	exist, err := storage.Exist(ctx, b.getBackupStorageClient(), mpath.BackupDir(backupRootPath, request.GetBackupName()))
	if err != nil {
		return fmt.Errorf("fail to check whether exist backup with name: %s", request.GetBackupName())
	}
	if exist {
		return fmt.Errorf("backup with name %s already exist", request.GetBackupName())
	}

	var addr string
	if len(request.GetGcPauseAddress()) != 0 {
		addr = request.GetGcPauseAddress()
	} else if len(b.params.BackupCfg.GcPauseAddress) != 0 {
		addr = b.params.BackupCfg.GcPauseAddress
	} else {
		return errors.New("enable gc pause but no address provided")
	}
	manage := milvus.NewManage(addr)

	args := backup.TaskArgs{
		TaskID:        backupInfo.GetId(),
		MilvusStorage: b.getMilvusStorageClient(),
		BackupStorage: b.getBackupStorageClient(),
		BackupDir:     mpath.BackupDir(backupRootPath, request.GetBackupName()),
		Request:       request,
		Params:        b.params,
		Grpc:          b.getMilvusClient(),
		Restful:       b.getRestfulClient(),
		Manage:        manage,
		Meta:          b.meta,
	}

	task, err := backup.NewTask(args)
	if err != nil {
		b.meta.UpdateBackup(backupInfo.GetId(), meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
		log.Error("fail to create backup task", zap.Error(err))
		return fmt.Errorf("fail to create backup task: %w", err)
	}

	if err := task.Execute(ctx); err != nil {
		b.meta.UpdateBackup(backupInfo.GetId(), meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
		log.Error("fail to execute backup task", zap.Error(err))
		return fmt.Errorf("fail to execute backup task: %w", err)
	}

	log.Info("finish backup all info",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.Bool("async", request.GetAsync()))

	b.meta.UpdateBackup(backupInfo.GetId(), meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_SUCCESS),
		meta.SetEndTime(time.Now().UnixMilli()))
	return nil
}
