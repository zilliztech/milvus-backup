package core

import (
	"context"
	"fmt"
	"path"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func (b *BackupContext) RestoreBackup(ctx context.Context, request *backuppb.RestoreBackupRequest) *backuppb.RestoreBackupResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive RestoreBackupRequest", zap.Any("request", request))

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

	// 1, get and validate
	if request.GetCollectionSuffix() != "" {
		err := utils.ValidateType(request.GetCollectionSuffix(), CollectionRenameSuffix)
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
		backupPath = path.Join(b.backupRootPath, request.GetBackupName())
	} else {
		backupBucketName = request.GetBucketName()
		backupPath = path.Join(request.GetPath(), request.GetBackupName())
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

	// overwrite taskID
	if request.GetId() == "" {
		taskID := "restore_" + fmt.Sprint(time.Now().UTC().Format("2006_01_02_15_04_05_")) + fmt.Sprint(time.Now().Nanosecond())
		request.Id = taskID
	}

	if request.Async {
		go b.executeRestoreBackupTask(ctx, backupBucketName, backupPath, backup, request)
		task := b.meta.GetRestoreTask(request.GetId())
		asyncResp := &backuppb.RestoreBackupResponse{
			RequestId: request.GetRequestId(),
			Code:      backuppb.ResponseCode_Success,
			Msg:       "restore backup is executing asynchronously",
			Data:      task,
		}
		return asyncResp
	} else {
		taskPB, err := b.executeRestoreBackupTask(ctx, backupBucketName, backupPath, backup, request)
		resp.Data = taskPB
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

func (b *BackupContext) executeRestoreBackupTask(ctx context.Context, backupBucketName, backupPath string, backup *backuppb.BackupInfo, request *backuppb.RestoreBackupRequest) (*backuppb.RestoreBackupTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	restoreBackupTask := restore.NewTask(
		request,
		backupPath,
		backupBucketName,
		b.params,
		backup,
		b.meta,
		b.getBackupStorageClient(),
		b.getMilvusStorageClient(),
		b.getMilvusClient(),
		b.getRestfulClient())

	if err := restoreBackupTask.Execute(ctx); err != nil {
		return nil, fmt.Errorf("backup: execute restore collection fail, err: %w", err)
	}

	taskPB := b.meta.GetRestoreTask(request.GetId())

	return taskPB, nil
}
