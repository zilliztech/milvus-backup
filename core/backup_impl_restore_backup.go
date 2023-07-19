package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	jsoniter "github.com/json-iterator/go"
	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

func (b *BackupContext) RestoreBackup(ctx context.Context, request *backuppb.RestoreBackupRequest) *backuppb.RestoreBackupResponse {
	if request.GetRequestId() == "" {
		request.RequestId = utils.UUID()
	}
	log.Info("receive RestoreBackupRequest",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.String("CollectionSuffix", request.GetCollectionSuffix()),
		zap.Any("CollectionRenames", request.GetCollectionRenames()),
		zap.Bool("async", request.GetAsync()),
		zap.String("bucketName", request.GetBucketName()),
		zap.String("path", request.GetPath()),
		zap.String("databaseCollections", request.GetDbCollections()))

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
		err := utils.ValidateType(request.GetCollectionSuffix(), COLLECTION_RENAME_SUFFIX)
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
		backupPath = b.backupRootPath + SEPERATOR + request.GetBackupName()
	} else {
		backupBucketName = request.GetBucketName()
		backupPath = request.GetPath() + SEPERATOR + request.GetBackupName()
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

	id := utils.UUID()

	task := &backuppb.RestoreBackupTask{
		Id:        id,
		StateCode: backuppb.RestoreTaskStateCode_INITIAL,
		StartTime: time.Now().Unix(),
		Progress:  0,
	}

	// 2, initial restoreCollectionTasks
	toRestoreCollectionBackups := make([]*backuppb.CollectionBackupInfo, 0)

	if request.GetDbCollections() != "" {
		var dbCollections DbCollections
		err := jsoniter.UnmarshalFromString(request.GetDbCollections(), &dbCollections)
		if err != nil {
			log.Error("fail in unmarshal dbCollections in RestoreBackupRequest", zap.String("dbCollections", request.GetDbCollections()), zap.Error(err))
			errorMsg := fmt.Sprintf("fail in unmarshal dbCollections in RestoreBackupRequest， dbCollections: %s, err: %s", request.GetDbCollections(), err)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
		}
		for db, collections := range dbCollections {
			if len(collections) == 0 {
				for _, collectionBackup := range backup.GetCollectionBackups() {
					if collectionBackup.GetDbName() == db {
						toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
					}
				}
			} else {
				for _, coll := range collections {
					for _, collectionBackup := range backup.GetCollectionBackups() {
						if collectionBackup.GetDbName() == db && collectionBackup.CollectionName == coll {
							toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
						}
					}
				}
			}
		}
	} else if len(request.GetCollectionNames()) == 0 {
		toRestoreCollectionBackups = backup.GetCollectionBackups()
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
			dbName := "default"
			if collectionBackup.GetDbName() != "" {
				dbName = collectionBackup.GetDbName()
			}
			fullCollectionName := dbName + "." + collectionBackup.GetCollectionName()
			collectionBackup.GetCollectionName()
			if collectionNameDict[fullCollectionName] {
				toRestoreCollectionBackups = append(toRestoreCollectionBackups, collectionBackup)
			}
		}
	}
	log.Info("Collections to restore", zap.Int("collection_num", len(toRestoreCollectionBackups)))

	restoreCollectionTasks := make([]*backuppb.RestoreCollectionTask, 0)
	for _, restoreCollection := range toRestoreCollectionBackups {
		backupCollectionName := restoreCollection.GetSchema().GetName()
		var targetCollectionName string
		// rename collection, rename map has higher priority then suffix
		if len(request.GetCollectionRenames()) > 0 && request.GetCollectionRenames()[backupCollectionName] != "" {
			targetCollectionName = request.GetCollectionRenames()[backupCollectionName]
		} else if request.GetCollectionSuffix() != "" {
			targetCollectionName = backupCollectionName + request.GetCollectionSuffix()
		} else {
			targetCollectionName = backupCollectionName
		}

		// check if the database exist, if not, create it first
		dbs, err := b.getMilvusClient().ListDatabases(ctx)
		if err != nil {
			errorMsg := fmt.Sprintf("fail to list databases, err: %s", err)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
		}
		var hasDatabase = false
		for _, db := range dbs {
			if db.Name == restoreCollection.DbName {
				hasDatabase = true
				break
			}
		}
		if !hasDatabase {
			err := b.getMilvusClient().CreateDatabase(ctx, restoreCollection.DbName)
			if err != nil {
				errorMsg := fmt.Sprintf("fail to create database %s, err: %s", restoreCollection.DbName, err)
				log.Error(errorMsg)
				resp.Code = backuppb.ResponseCode_Fail
				resp.Msg = errorMsg
				return resp
			}
			log.Info("create database", zap.String("database", restoreCollection.DbName))
		}
		err = b.getMilvusClient().UsingDatabase(ctx, restoreCollection.DbName)
		if err != nil {
			errorMsg := fmt.Sprintf("fail to switch database %s, err: %s", restoreCollection.DbName, err)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
		}

		// check if the collection exist, if exist, will not restore
		exist, err := b.getMilvusClient().HasCollection(ctx, targetCollectionName)
		if err != nil {
			errorMsg := fmt.Sprintf("fail to check whether the collection is exist, collection_name: %s, err: %s", targetCollectionName, err)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
		}
		if exist {
			errorMsg := fmt.Sprintf("The collection to restore already exists, backupCollectName: %s, targetCollectionName: %s", backupCollectionName, targetCollectionName)
			log.Error(errorMsg)
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = errorMsg
			return resp
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
			TargetCollectionName:  targetCollectionName,
			PartitionRestoreTasks: []*backuppb.RestorePartitionTask{},
			ToRestoreSize:         toRestoreSize,
			RestoredSize:          0,
			Progress:              0,
		}
		restoreCollectionTasks = append(restoreCollectionTasks, restoreCollectionTask)
		task.CollectionRestoreTasks = restoreCollectionTasks
		task.ToRestoreSize = task.GetToRestoreSize() + toRestoreSize
	}

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
		endTask, err := b.executeRestoreBackupTask(ctx, backupBucketName, backupPath, backup, task)
		resp.Data = endTask
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

func (b *BackupContext) executeRestoreBackupTask(ctx context.Context, backupBucketName string, backupPath string, backup *backuppb.BackupInfo, task *backuppb.RestoreBackupTask) (*backuppb.RestoreBackupTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := task.GetId()
	b.restoreTasks[id] = task
	task.StateCode = backuppb.RestoreTaskStateCode_EXECUTING

	log.Info("executeRestoreBackupTask start",
		zap.String("backup_name", backup.GetName()),
		zap.String("backupBucketName", backupBucketName),
		zap.String("backupPath", backupPath))
	updateRestoreTaskFunc := func(id string, task *backuppb.RestoreBackupTask) {
		b.restoreTasks[id] = task
	}
	defer updateRestoreTaskFunc(id, task)

	restoreCollectionTasks := task.GetCollectionRestoreTasks()

	// 3, execute restoreCollectionTasks
	for _, restoreCollectionTask := range restoreCollectionTasks {
		endTask, err := b.executeRestoreCollectionTask(ctx, backupBucketName, backupPath, restoreCollectionTask, id)
		if err != nil {
			log.Error("executeRestoreCollectionTask failed",
				zap.String("TargetCollectionName", restoreCollectionTask.GetTargetCollectionName()),
				zap.Error(err))
			return task, err
		}
		log.Info("finish restore collection", zap.String("collection_name", restoreCollectionTask.GetTargetCollectionName()))
		restoreCollectionTask.StateCode = backuppb.RestoreTaskStateCode_SUCCESS
		task.RestoredSize += endTask.RestoredSize
		if task.GetToRestoreSize() == 0 {
			task.Progress = 100
		} else {
			task.Progress = int32(100 * task.GetRestoredSize() / task.GetToRestoreSize())
		}
		updateRestoreTaskFunc(id, task)
	}

	task.StateCode = backuppb.RestoreTaskStateCode_SUCCESS
	task.EndTime = time.Now().Unix()
	return task, nil
}

func (b *BackupContext) executeRestoreCollectionTask(ctx context.Context, backupBucketName string, backupPath string, task *backuppb.RestoreCollectionTask, parentTaskID string) (*backuppb.RestoreCollectionTask, error) {
	targetCollectionName := task.GetTargetCollectionName()
	task.StateCode = backuppb.RestoreTaskStateCode_EXECUTING
	log.Info("start restore",
		zap.String("collection_name", task.GetTargetCollectionName()),
		zap.String("backupBucketName", backupBucketName),
		zap.String("backupPath", backupPath))
	// create collection
	fields := make([]*entity.Field, 0)
	hasPartitionKey := false
	for _, field := range task.GetCollBackup().GetSchema().GetFields() {
		fields = append(fields, &entity.Field{
			ID:             field.GetFieldID(),
			Name:           field.GetName(),
			PrimaryKey:     field.GetIsPrimaryKey(),
			AutoID:         field.GetAutoID(),
			Description:    field.GetDescription(),
			DataType:       entity.FieldType(field.GetDataType()),
			TypeParams:     utils.KvPairsMap(field.GetTypeParams()),
			IndexParams:    utils.KvPairsMap(field.GetIndexParams()),
			IsDynamic:      field.GetIsDynamic(),
			IsPartitionKey: field.GetIsPartitionKey(),
		})
		if field.GetIsPartitionKey() {
			hasPartitionKey = true
		}
	}

	log.Info("collection schema", zap.Any("fields", fields))

	collectionSchema := &entity.Schema{
		CollectionName:     targetCollectionName,
		Description:        task.GetCollBackup().GetSchema().GetDescription(),
		AutoID:             task.GetCollBackup().GetSchema().GetAutoID(),
		Fields:             fields,
		EnableDynamicField: task.GetCollBackup().GetSchema().GetEnableDynamicField(),
	}

	dbName := task.GetCollBackup().GetDbName()
	if dbName == "" {
		dbName = "default"
	}
	b.getMilvusClient().UsingDatabase(ctx, dbName)

	err := retry.Do(ctx, func() error {
		if hasPartitionKey {
			partitionNum := len(task.GetCollBackup().GetPartitionBackups())
			return b.getMilvusClient().CreateCollection(
				ctx,
				collectionSchema,
				task.GetCollBackup().GetShardsNum(),
				gomilvus.WithConsistencyLevel(entity.ConsistencyLevel(task.GetCollBackup().GetConsistencyLevel())),
				gomilvus.WithPartitionNum(int64(partitionNum)))
		}
		return b.getMilvusClient().CreateCollection(
			ctx,
			collectionSchema,
			task.GetCollBackup().GetShardsNum(),
			gomilvus.WithConsistencyLevel(entity.ConsistencyLevel(task.GetCollBackup().GetConsistencyLevel())))
	}, retry.Attempts(10), retry.Sleep(1*time.Second))
	if err != nil {
		errorMsg := fmt.Sprintf("fail to create collection, targetCollectionName: %s err: %s", targetCollectionName, err)
		log.Error(errorMsg)
		task.StateCode = backuppb.RestoreTaskStateCode_FAIL
		task.ErrorMessage = errorMsg
		return task, err
	}
	log.Info("create collection", zap.String("database", dbName), zap.String("collectionName", targetCollectionName), zap.Bool("hasPartitionKey", hasPartitionKey))

	tempDir := "restore-temp-" + parentTaskID + SEPERATOR
	isSameBucket := b.milvusBucketName == backupBucketName
	// clean the temporary file
	defer func() {
		if !isSameBucket {
			log.Info("Delete temporary file", zap.String("dir", tempDir))
			err := b.getStorageClient().RemoveWithPrefix(ctx, b.milvusBucketName, tempDir)
			if err != nil {
				log.Warn("Delete temporary file failed", zap.Error(err))
			}
		}
	}()

	for _, partitionBackup := range task.GetCollBackup().GetPartitionBackups() {
		exist, err := b.getMilvusClient().HasPartition(ctx, targetCollectionName, partitionBackup.GetPartitionName())
		if err != nil {
			log.Error("fail to check has partition", zap.Error(err))
			return task, err
		}
		if !exist {
			err = retry.Do(ctx, func() error {
				return b.getMilvusClient().CreatePartition(ctx, targetCollectionName, partitionBackup.GetPartitionName())
			}, retry.Attempts(10), retry.Sleep(1*time.Second))
			if err != nil {
				log.Error("fail to create partition", zap.Error(err))
				return task, err
			}
		}
		log.Info("create partition",
			zap.String("collectionName", targetCollectionName),
			zap.String("partitionName", partitionBackup.GetPartitionName()))

		// bulk insert
		copyAndBulkInsert := func(files []string) error {
			realFiles := make([]string, len(files))
			// if milvus bucket and backup bucket are not the same, should copy the data first
			if !isSameBucket {
				log.Info("milvus bucket and backup bucket are not the same, copy the data first", zap.Strings("files", files))
				for i, file := range files {
					// empty delta file, no need to copy
					if file == "" {
						realFiles[i] = file
					} else {
						err := b.getStorageClient().Copy(ctx, backupBucketName, b.milvusBucketName, file, tempDir+file)
						if err != nil {
							log.Error("fail to copy backup date from backup bucket to restore target milvus bucket", zap.Error(err))
							return err
						}
						realFiles[i] = tempDir + file
					}
				}
			} else {
				realFiles = files
			}

			err = b.executeBulkInsert(ctx, targetCollectionName, partitionBackup.GetPartitionName(), realFiles, int64(task.GetCollBackup().BackupTimestamp))
			if err != nil {
				log.Error("fail to bulk insert to partition",
					zap.Error(err),
					zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
					zap.String("targetCollectionName", targetCollectionName),
					zap.String("partition", partitionBackup.GetPartitionName()))
				return err
			}
			return nil
		}

		groupIds := collectGroupIdsFromSegments(partitionBackup.GetSegmentBackups())
		if len(groupIds) == 1 && groupIds[0] == 0 {
			// backward compatible old backup without group id
			files, err := b.getBackupPartitionPaths(ctx, backupBucketName, backupPath, partitionBackup)
			if err != nil {
				log.Error("fail to get partition backup binlog files",
					zap.Error(err),
					zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
					zap.String("targetCollectionName", targetCollectionName),
					zap.String("partition", partitionBackup.GetPartitionName()))
				return task, err
			}
			err = copyAndBulkInsert(files)
			if err != nil {
				log.Error("fail to (copy and) bulkinsert data",
					zap.Error(err),
					zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
					zap.String("targetCollectionName", targetCollectionName),
					zap.String("partition", partitionBackup.GetPartitionName()))
				return task, err
			}
		} else {
			// bulk insert by segment groups
			for _, groupId := range groupIds {
				files, err := b.getBackupPartitionPathsWithGroupID(ctx, backupBucketName, backupPath, partitionBackup, groupId)
				if err != nil {
					log.Error("fail to get partition backup binlog files",
						zap.Error(err),
						zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
						zap.String("targetCollectionName", targetCollectionName),
						zap.String("partition", partitionBackup.GetPartitionName()))
					return task, err
				}
				err = copyAndBulkInsert(files)
				if err != nil {
					log.Error("fail to (copy and) bulkinsert data",
						zap.Error(err),
						zap.String("backupCollectionName", task.GetCollBackup().GetCollectionName()),
						zap.String("targetCollectionName", targetCollectionName),
						zap.String("partition", partitionBackup.GetPartitionName()))
					return task, err
				}
			}
		}
		task.RestoredSize = task.RestoredSize + partitionBackup.GetSize()
		if task.ToRestoreSize == 0 {
			task.Progress = 100
		} else {
			task.Progress = int32(100 * task.RestoredSize / task.ToRestoreSize)
		}
	}

	return task, err
}

func collectGroupIdsFromSegments(segments []*backuppb.SegmentBackupInfo) []int64 {
	dict := make(map[int64]bool)
	res := make([]int64, 0)
	for _, seg := range segments {
		if _, ok := dict[seg.GetGroupId()]; !ok {
			dict[seg.GetGroupId()] = true
			res = append(res, seg.GetGroupId())
		}
	}
	return res
}

func (b *BackupContext) executeBulkInsert(ctx context.Context, coll string, partition string, files []string, endTime int64) error {
	log.Debug("execute bulk insert",
		zap.String("collection", coll),
		zap.String("partition", partition),
		zap.Strings("files", files),
		zap.Int64("endTime", endTime))
	taskId, err := b.getMilvusClient().BulkInsert(ctx, coll, partition, files, gomilvus.IsBackup(), gomilvus.WithEndTs(endTime))
	if err != nil {
		log.Error("fail to bulk insert",
			zap.Error(err),
			zap.String("collectionName", coll),
			zap.String("partitionName", partition),
			zap.Strings("files", files))
		return err
	}
	err = b.watchBulkInsertState(ctx, taskId, BULKINSERT_TIMEOUT, BULKINSERT_SLEEP_INTERVAL)
	if err != nil {
		log.Error("fail or timeout to bulk insert",
			zap.Error(err),
			zap.Int64("taskId", taskId),
			zap.String("targetCollectionName", coll),
			zap.String("partitionName", partition))
		return err
	}
	return nil
}

func (b *BackupContext) watchBulkInsertState(ctx context.Context, taskId int64, timeout int64, sleepSeconds int) error {
	lastProgress := 0
	lastUpdateTime := time.Now().Unix()
	for {
		importTaskState, err := b.getMilvusClient().GetBulkInsertState(ctx, taskId)
		currentTimestamp := time.Now().Unix()
		if err != nil {
			return err
		}
		log.Info("bulkinsert task state",
			zap.Int64("id", taskId),
			zap.Int32("state", int32(importTaskState.State)),
			zap.Any("state", importTaskState),
			zap.Int("progress", importTaskState.Progress()),
			zap.Int64("currentTimestamp", currentTimestamp),
			zap.Int64("lastUpdateTime", lastUpdateTime))
		switch importTaskState.State {
		case entity.BulkInsertFailed:
			if value, ok := importTaskState.Infos["failed_reason"]; ok {
				return errors.New("bulk insert fail, info: " + value)
			} else {
				return errors.New("bulk insert fail")
			}
		case entity.BulkInsertCompleted:
			return nil
		default:
			currentProgress := importTaskState.Progress()
			if currentProgress > lastProgress {
				lastUpdateTime = time.Now().Unix()
			} else if (currentTimestamp - lastUpdateTime) >= timeout {
				log.Warn(fmt.Sprintf("bulkinsert task state progress hang for more than %d s", timeout))
				return errors.New("import task timeout")
			}
			time.Sleep(time.Second * time.Duration(sleepSeconds))
			continue
		}
	}
	return errors.New("import task timeout")
}

func (b *BackupContext) getBackupPartitionPaths(ctx context.Context, bucketName string, backupPath string, partition *backuppb.PartitionBackupInfo) ([]string, error) {
	log.Info("getBackupPartitionPaths",
		zap.String("bucketName", bucketName),
		zap.String("backupPath", backupPath),
		zap.Int64("partitionID", partition.PartitionId))

	insertPath := fmt.Sprintf("%s/%s/%s/%v/%v/", backupPath, BINGLOG_DIR, INSERT_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())
	deltaPath := fmt.Sprintf("%s/%s/%s/%v/%v/", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())

	exist, err := b.getStorageClient().Exist(ctx, bucketName, deltaPath)
	if err != nil {
		log.Warn("check binlog exist fail", zap.Error(err))
		return []string{}, err
	}
	if !exist {
		return []string{insertPath, ""}, nil
	}
	return []string{insertPath, deltaPath}, nil
}

func (b *BackupContext) getBackupPartitionPathsWithGroupID(ctx context.Context, bucketName string, backupPath string, partition *backuppb.PartitionBackupInfo, groupId int64) ([]string, error) {
	log.Info("getBackupPartitionPaths",
		zap.String("bucketName", bucketName),
		zap.String("backupPath", backupPath),
		zap.Int64("partitionID", partition.GetPartitionId()),
		zap.Int64("groupId", groupId))

	insertPath := fmt.Sprintf("%s/%s/%s/%v/%v/%d/", backupPath, BINGLOG_DIR, INSERT_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId(), groupId)
	deltaPath := fmt.Sprintf("%s/%s/%s/%v/%v/%d/", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId(), groupId)

	exist, err := b.getStorageClient().Exist(ctx, bucketName, deltaPath)
	if err != nil {
		log.Warn("check binlog exist fail", zap.Error(err))
		return []string{}, err
	}
	if !exist {
		return []string{insertPath, ""}, nil
	}
	return []string{insertPath, deltaPath}, nil
}
