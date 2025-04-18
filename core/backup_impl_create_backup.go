package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func (b *BackupContext) CreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest) *backuppb.BackupInfoResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive CreateBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.String("databaseCollections", utils.GetDBCollections(request.GetDbCollections())),
		zap.Bool("async", request.GetAsync()),
		zap.Bool("force", request.GetForce()),
		zap.Bool("metaOnly", request.GetMetaOnly()))

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
	if request.GetBackupName() != "" {
		exist, err := b.getBackupStorageClient().Exist(b.ctx, b.backupBucketName, b.backupRootPath+meta.SEPERATOR+request.GetBackupName())
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
	b.meta.AddBackup(info)

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

	task := backup.NewTask(backupInfo.GetId(),
		b.getMilvusStorageClient(),
		b.getBackupStorageClient(),
		request, b.params,
		b.getMilvusClient(),
		b.getRestfulClient(),
		b.meta)
	if err := task.Execute(ctx); err != nil {
		b.meta.UpdateBackup(backupInfo.GetId(), meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL))
		log.Error("fail to execute backup task", zap.Error(err))
		return fmt.Errorf("fail to execute backup task: %w", err)
	}

	if err := b.writeBackupInfoMeta(ctx, backupInfo.GetId()); err != nil {
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL),
			meta.SetErrorMessage(err.Error()))
		return err
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

func (b *BackupContext) writeBackupInfoMeta(ctx context.Context, id string) error {
	// TODO: move to backup package
	backupInfo := b.meta.GetFullMeta(id)
	output, _ := meta.Serialize(backupInfo)

	collectionBackups := backupInfo.GetCollectionBackups()
	collectionPositions := make(map[string][]*backuppb.ChannelPosition)
	for _, collectionBackup := range collectionBackups {
		collectionCPs := make([]*backuppb.ChannelPosition, 0, len(collectionBackup.GetChannelCheckpoints()))
		for vCh, position := range collectionBackup.GetChannelCheckpoints() {
			collectionCPs = append(collectionCPs, &backuppb.ChannelPosition{
				Name:     vCh,
				Position: position,
			})
		}
		collectionPositions[collectionBackup.GetCollectionName()] = collectionCPs
	}
	channelCPsBytes, err := json.Marshal(collectionPositions)
	if err != nil {
		return err
	}
	log.Debug("channel cp meta", zap.String("value", string(channelCPsBytes)))

	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.BackupMetaPath(b.backupRootPath, backupInfo.GetName()), output.BackupMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.CollectionMetaPath(b.backupRootPath, backupInfo.GetName()), output.CollectionMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.PartitionMetaPath(b.backupRootPath, backupInfo.GetName()), output.PartitionMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.SegmentMetaPath(b.backupRootPath, backupInfo.GetName()), output.SegmentMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.FullMetaPath(b.backupRootPath, backupInfo.GetName()), output.FullMetaBytes)
	b.getBackupStorageClient().Write(ctx, b.backupBucketName, meta.ChannelCPMetaPath(b.backupRootPath, backupInfo.GetName()), channelCPsBytes)

	log.Info("finish writeBackupInfoMeta",
		zap.String("path", meta.BackupDirPath(b.backupRootPath, backupInfo.GetName())),
		zap.String("backupName", backupInfo.GetName()),
		zap.String("backup meta", string(output.BackupMetaBytes)))
	return nil
}
