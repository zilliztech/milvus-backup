package core

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/meta"
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

	var taskID string
	if request.GetId() != "" {
		taskID = request.GetId()
	} else {
		taskID = "restore_" + fmt.Sprint(time.Now().UTC().Format("2006_01_02_15_04_05_")) + fmt.Sprint(time.Now().Nanosecond())
	}

	task := &backuppb.RestoreBackupTask{
		Id:        taskID,
		StateCode: backuppb.RestoreTaskStateCode_INITIAL,
		StartTime: time.Now().Unix(),
		Progress:  0,
	}
	// clean thread pool
	defer func() {
		b.cleanRestoreWorkerPool(taskID)
	}()

	// restore rbac
	if request.GetRbac() {
		rt := restore.NewRBACTask(b.getMilvusClient(), backup.GetRbacMeta())
		if err := rt.Execute(ctx); err != nil {
			log.Error("fail to restore RBAC", zap.Error(err))
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = fmt.Sprintf("fail to restore RBAC, err: %s", err)
			return resp
		}
	} else {
		log.Info("skip restore RBAC")
	}

	// 2, initial restoreCollectionTasks
	toRestoreCollectionBackups := make([]*backuppb.CollectionBackupInfo, 0)

	dbCollectionsStr := utils.GetRestoreDBCollections(request)
	if dbCollectionsStr != "" {
		var dbCollections meta.DbCollections
		err := jsoniter.UnmarshalFromString(dbCollectionsStr, &dbCollections)
		if err != nil {
			log.Error("fail in unmarshal dbCollections in RestoreBackupRequest", zap.String("dbCollections", dbCollectionsStr), zap.Error(err))
			errorMsg := fmt.Sprintf("fail in unmarshal dbCollections in RestoreBackupRequest， dbCollections: %s, err: %s", request.GetDbCollections(), err)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
		}
		for db, collections := range dbCollections {
			if len(collections) == 0 {
				for _, collectionBackup := range backup.GetCollectionBackups() {
					if collectionBackup.GetDbName() == "" {
						collectionBackup.DbName = "default"
					}
					if collectionBackup.GetDbName() == db {
						toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
					}
				}
			} else {
				for _, coll := range collections {
					for _, collectionBackup := range backup.GetCollectionBackups() {
						if collectionBackup.GetDbName() == "" {
							collectionBackup.DbName = "default"
						}
						if collectionBackup.GetDbName() == db && collectionBackup.CollectionName == coll {
							toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
						}
					}
				}
			}
		}
	} else if len(request.GetCollectionNames()) == 0 {
		for _, collectionBackup := range backup.GetCollectionBackups() {
			if collectionBackup.GetDbName() == "" {
				collectionBackup.DbName = "default"
			}
			toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
		}
	} else {
		collectionNameDict := make(map[string]bool)
		for _, collectionName := range request.GetCollectionNames() {
			var fullCollectionName string
			if strings.Contains(collectionName, ".") {
				fullCollectionName = collectionName
			} else {
				fullCollectionName = "default." + collectionName
			}
			collectionNameDict[fullCollectionName] = true
		}
		for _, collectionBackup := range backup.GetCollectionBackups() {
			if collectionBackup.GetDbName() == "" {
				collectionBackup.DbName = "default"
			}
			fullCollectionName := collectionBackup.GetDbName() + "." + collectionBackup.GetCollectionName()
			if collectionNameDict[fullCollectionName] {
				toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
			}
		}
	}
	log.Info("Collections to restore", zap.Int("collection_num", len(toRestoreCollectionBackups)))

	// add default db in collection_renames if not set
	collectionRenames := make(map[string]string)
	dbRenames := make(map[string]string)
	for oldname, newName := range request.GetCollectionRenames() {
		if strings.HasSuffix(oldname, ".*") && strings.HasSuffix(newName, ".*") {
			dbRenames[strings.Split(oldname, ".*")[0]] = strings.Split(newName, ".*")[0]
		}

		var fullCollectionName string
		if strings.Contains(oldname, ".") {
			fullCollectionName = oldname
		} else {
			fullCollectionName = "default." + oldname
		}
		var fullCollectionNewName string
		if strings.Contains(newName, ".") {
			fullCollectionNewName = newName
		} else {
			fullCollectionNewName = "default." + newName
		}
		collectionRenames[fullCollectionName] = fullCollectionNewName
	}

	restoreCollectionTasks := make([]*backuppb.RestoreCollectionTask, 0)
	for _, restoreCollection := range toRestoreCollectionBackups {
		backupDBCollectionName := restoreCollection.DbName + "." + restoreCollection.GetSchema().GetName()
		targetDBName := restoreCollection.DbName
		targetCollectionName := restoreCollection.GetSchema().GetName()
		if value, ok := dbRenames[restoreCollection.DbName]; ok {
			targetDBName = value
		}
		// rename collection, rename map has higher priority then suffix
		if len(request.GetCollectionRenames()) > 0 && collectionRenames[backupDBCollectionName] != "" {
			targetDBName = strings.Split(collectionRenames[backupDBCollectionName], ".")[0]
			targetCollectionName = strings.Split(collectionRenames[backupDBCollectionName], ".")[1]
		} else if request.GetCollectionSuffix() != "" {
			targetCollectionName = targetCollectionName + request.GetCollectionSuffix()
		}

		var toRestoreSize int64 = 0
		for _, partitionBackup := range restoreCollection.GetPartitionBackups() {
			toRestoreSize += partitionBackup.GetSize()
		}
		id := utils.UUID()

		restoreCollectionTask := &backuppb.RestoreCollectionTask{
			Id:                    id,
			StateCode:             backuppb.RestoreTaskStateCode_INITIAL,
			StartTime:             time.Now().Unix(),
			CollBackup:            restoreCollection,
			TargetDbName:          targetDBName,
			TargetCollectionName:  targetCollectionName,
			PartitionRestoreTasks: []*backuppb.RestorePartitionTask{},
			ToRestoreSize:         toRestoreSize,
			MetaOnly:              request.GetMetaOnly(),
			RestoreIndex:          request.GetRestoreIndex(),
			UseAutoIndex:          request.GetUseAutoIndex(),
			DropExistCollection:   request.GetDropExistCollection(),
			DropExistIndex:        request.GetDropExistIndex(),
			SkipCreateCollection:  request.GetSkipCreateCollection(),
			MaxShardNum:           request.GetMaxShardNum(),
			SkipParams:            request.GetSkipParams(),
			UseV2Restore:          request.GetUseV2Restore(),
			TruncateBinlogByTs:    request.GetTruncateBinlogByTs(),
		}
		restoreCollectionTasks = append(restoreCollectionTasks, restoreCollectionTask)
		task.CollectionRestoreTasks = restoreCollectionTasks
		task.ToRestoreSize = task.GetToRestoreSize() + toRestoreSize
	}
	b.meta.AddRestoreTask(task)

	if request.Async {
		go b.executeRestoreBackupTask(ctx, backupBucketName, backupPath, backup, task)
		asyncResp := &backuppb.RestoreBackupResponse{
			RequestId: request.GetRequestId(),
			Code:      backuppb.ResponseCode_Success,
			Msg:       "restore backup is executing asynchronously",
			Data:      task,
		}
		return asyncResp
	} else {
		err := b.executeRestoreBackupTask(ctx, backupBucketName, backupPath, backup, task)
		resp.Data = task
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

func (b *BackupContext) executeRestoreBackupTask(ctx context.Context, backupBucketName, backupPath string, backup *backuppb.BackupInfo, task *backuppb.RestoreBackupTask) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	restoreBackupTask := restore.NewTask(task,
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
		return fmt.Errorf("backup: execute restore collection fail, err: %w", err)
	}

	return nil
}
