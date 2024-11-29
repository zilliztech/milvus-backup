package core

import (
	"context"
	"fmt"

	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	entityV2 "github.com/milvus-io/milvus/client/v2/entity"
	indexV2 "github.com/milvus-io/milvus/client/v2/index"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/common"
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
		zap.Bool("onlyMeta", request.GetMetaOnly()),
		zap.Bool("restoreIndex", request.GetRestoreIndex()),
		zap.Bool("useAutoIndex", request.GetUseAutoIndex()),
		zap.Bool("dropExistCollection", request.GetDropExistCollection()),
		zap.Bool("dropExistIndex", request.GetDropExistIndex()),
		zap.Bool("skipCreateCollection", request.GetSkipCreateCollection()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.String("CollectionSuffix", request.GetCollectionSuffix()),
		zap.Any("CollectionRenames", request.GetCollectionRenames()),
		zap.Bool("async", request.GetAsync()),
		zap.String("bucketName", request.GetBucketName()),
		zap.String("path", request.GetPath()),
		zap.String("databaseCollections", utils.GetRestoreDBCollections(request)),
		zap.Bool("skipDiskQuotaCheck", request.GetSkipImportDiskQuotaCheck()),
		zap.Int32("maxShardNum", request.GetMaxShardNum()))

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
		err := b.restoreRBAC(ctx, backup)
		if err != nil {
			log.Error("fail to restore RBAC", zap.Error(err))
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = fmt.Sprintf("fail to restore RBAC, err: %s", err)
			return resp
		}
	}

	// 2, initial restoreCollectionTasks
	toRestoreCollectionBackups := make([]*backuppb.CollectionBackupInfo, 0)

	dbCollectionsStr := utils.GetRestoreDBCollections(request)
	if dbCollectionsStr != "" {
		var dbCollections DbCollections
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
		targetDBCollectionName := targetDBName + "." + targetCollectionName

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
			if db.Name == targetDBName {
				hasDatabase = true
				break
			}
		}
		if !hasDatabase {
			err := b.getMilvusClient().CreateDatabase(ctx, targetDBName)
			if err != nil {
				errorMsg := fmt.Sprintf("fail to create database %s, err: %s", targetDBName, err)
				log.Error(errorMsg)
				resp.Code = backuppb.ResponseCode_Fail
				resp.Msg = errorMsg
				return resp
			}
			log.Info("create database", zap.String("database", targetDBName))
		}

		// check if the collection exist, if exist, will not restore
		if !request.GetSkipCreateCollection() {
			exist, err := b.getMilvusClient().HasCollection(ctx, targetDBName, targetCollectionName)
			if err != nil {
				errorMsg := fmt.Sprintf("fail to check whether the collection is exist, collection_name: %s, err: %s", targetDBCollectionName, err)
				log.Error(errorMsg)
				resp.Code = backuppb.ResponseCode_Fail
				resp.Msg = errorMsg
				return resp
			}
			if exist {
				errorMsg := fmt.Sprintf("The collection to restore already exists, backupCollectName: %s, targetCollectionName: %s", backupDBCollectionName, targetDBCollectionName)
				log.Error(errorMsg)
				resp.Code = backuppb.ResponseCode_Fail
				resp.Msg = errorMsg
				return resp
			}
		} else {
			log.Info("skip check collection exist")
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
			RestoredSize:          0,
			Progress:              0,
			MetaOnly:              request.GetMetaOnly(),
			RestoreIndex:          request.GetRestoreIndex(),
			UseAutoIndex:          request.GetUseAutoIndex(),
			DropExistCollection:   request.GetDropExistCollection(),
			DropExistIndex:        request.GetDropExistIndex(),
			SkipCreateCollection:  request.GetSkipCreateCollection(),
			SkipDiskQuotaCheck:    request.GetSkipImportDiskQuotaCheck(),
			MaxShardNum:           request.GetMaxShardNum(),
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

	wp, err := common.NewWorkerPool(ctx, b.params.BackupCfg.RestoreParallelism, RPS)
	if err != nil {
		return task, err
	}
	wp.Start()
	log.Info("Start collection level restore pool", zap.Int("parallelism", b.params.BackupCfg.RestoreParallelism))

	id := task.GetId()
	b.meta.UpdateRestoreTask(id, setRestoreStateCode(backuppb.RestoreTaskStateCode_EXECUTING))
	log.Info("executeRestoreBackupTask start",
		zap.String("backup_name", backup.GetName()),
		zap.String("backupBucketName", backupBucketName),
		zap.String("backupPath", backupPath))

	restoreCollectionTasks := task.GetCollectionRestoreTasks()

	// 3, execute restoreCollectionTasks
	for _, restoreCollectionTask := range restoreCollectionTasks {
		restoreCollectionTaskClone := restoreCollectionTask
		job := func(ctx context.Context) error {
			endTask, err := b.executeRestoreCollectionTask(ctx, backupBucketName, backupPath, restoreCollectionTaskClone, id)
			if err != nil {
				log.Error("executeRestoreCollectionTask failed",
					zap.String("TargetDBName", restoreCollectionTaskClone.GetTargetDbName()),
					zap.String("TargetCollectionName", restoreCollectionTaskClone.GetTargetCollectionName()),
					zap.Error(err))
				return err
			}
			restoreCollectionTaskClone.StateCode = backuppb.RestoreTaskStateCode_SUCCESS
			log.Info("finish restore collection",
				zap.String("db_name", restoreCollectionTaskClone.GetTargetDbName()),
				zap.String("collection_name", restoreCollectionTaskClone.GetTargetCollectionName()),
				zap.Int64("size", endTask.RestoredSize))
			return nil
		}
		wp.Submit(job)
	}
	wp.Done()
	if err := wp.Wait(); err != nil {
		return task, err
	}

	endTime := time.Now().Unix()
	task.EndTime = endTime
	b.meta.UpdateRestoreTask(id, setRestoreStateCode(backuppb.RestoreTaskStateCode_SUCCESS), setRestoreEndTime(endTime))

	log.Info("finish restore all collections",
		zap.String("backupName", backup.GetName()),
		zap.Int("collections", len(backup.GetCollectionBackups())),
		zap.String("taskID", task.GetId()),
		zap.Int64("duration in seconds", task.GetEndTime()-task.GetStartTime()))
	return task, nil
}

func (b *BackupContext) executeRestoreCollectionTask(ctx context.Context, backupBucketName string, backupPath string, task *backuppb.RestoreCollectionTask, parentTaskID string) (*backuppb.RestoreCollectionTask, error) {
	targetDBName := task.GetTargetDbName()
	targetCollectionName := task.GetTargetCollectionName()
	task.StateCode = backuppb.RestoreTaskStateCode_EXECUTING
	log := log.With(
		zap.String("backup_db_name", task.GetCollBackup().DbName),
		zap.String("backup_collection_name", task.GetCollBackup().DbName),
		zap.String("target_db_name", targetDBName),
		zap.String("target_collection_name", targetCollectionName),
		zap.Bool("skipDiskQuotaCheck", task.GetSkipDiskQuotaCheck()),
		zap.Int32("maxShardNum", task.GetMaxShardNum()))
	log.Info("start restore",
		zap.String("backupBucketName", backupBucketName),
		zap.String("backupPath", backupPath))
	// create collection
	fields := make([]*entityV2.Field, 0)
	hasPartitionKey := false
	for _, field := range task.GetCollBackup().GetSchema().GetFields() {
		fieldRestore := &entityV2.Field{
			ID:             field.GetFieldID(),
			Name:           field.GetName(),
			PrimaryKey:     field.GetIsPrimaryKey(),
			AutoID:         field.GetAutoID(),
			Description:    field.GetDescription(),
			DataType:       entityV2.FieldType(field.GetDataType()),
			TypeParams:     utils.KvPairsMap(field.GetTypeParams()),
			IndexParams:    utils.KvPairsMap(field.GetIndexParams()),
			IsDynamic:      field.GetIsDynamic(),
			IsPartitionKey: field.GetIsPartitionKey(),
			Nullable:       field.GetNullable(),
			ElementType:    entityV2.FieldType(field.GetElementType()),
		}
		if field.DefaultValueProto != "" {
			defaultValue := &schemapb.ValueField{}
			err := proto.Unmarshal([]byte(field.DefaultValueProto), defaultValue)
			if err != nil {
				return nil, err
			}
			fieldRestore.DefaultValue = defaultValue
		}

		fields = append(fields, fieldRestore)
		if field.GetIsPartitionKey() {
			hasPartitionKey = true
		}
	}

	log.Info("collection schema", zap.Any("fields", fields))

	collectionSchema := &entityV2.Schema{
		CollectionName:     targetCollectionName,
		Description:        task.GetCollBackup().GetSchema().GetDescription(),
		AutoID:             task.GetCollBackup().GetSchema().GetAutoID(),
		Fields:             fields,
		EnableDynamicField: task.GetCollBackup().GetSchema().GetEnableDynamicField(),
	}

	if task.GetDropExistCollection() {
		//check if the collection exist, if collection exist, will drop it
		exist, err := b.milvusClient.HasCollection(ctx, targetDBName, targetCollectionName)
		if err != nil {
			errorMsg := fmt.Sprintf("fail to check whether the collection is exist, collection_name: %s, err: %s", targetCollectionName, err)
			log.Error(errorMsg)
		}
		if exist {
			err := b.milvusClient.DropCollection(ctx, targetDBName, targetCollectionName)
			if err != nil {
				errorMsg := fmt.Sprintf("fail to drop collection, CollectionName: %s.%s err: %s", targetDBName, targetCollectionName, err)
				log.Error(errorMsg)
				task.StateCode = backuppb.RestoreTaskStateCode_FAIL
				task.ErrorMessage = errorMsg
				return task, err
			}
		}
	}
	//in function RestoreBackup, before executing the restore,
	//the SkipCreateCollection has been checked,
	//so here it is necessary to be compatible with the situation where SkipCreateCollection and DropExistCollection are enabled at the same time.
	if !task.GetSkipCreateCollection() || task.GetDropExistCollection() {
		err := retry.Do(ctx, func() error {
			// overwrite shardNum by request parameter
			shardNum := task.GetCollBackup().GetShardsNum()
			if shardNum > task.GetMaxShardNum() && task.GetMaxShardNum() != 0 {
				shardNum = task.GetMaxShardNum()
				log.Info("overwrite shardNum by request parameter", zap.Int32("oldShardNum", task.GetCollBackup().GetShardsNum()), zap.Int32("newShardNum", shardNum))

			}

			if hasPartitionKey {
				partitionNum := len(task.GetCollBackup().GetPartitionBackups())
				return b.getMilvusClient().CreateCollectionV2(ctx, targetDBName, collectionSchema, shardNum, entityV2.ConsistencyLevel(task.GetCollBackup().GetConsistencyLevel()), int64(partitionNum))
			}
			return b.getMilvusClient().CreateCollectionV2(ctx, targetDBName, collectionSchema, shardNum, entityV2.ConsistencyLevel(task.GetCollBackup().GetConsistencyLevel()), 0)
		}, retry.Attempts(10), retry.Sleep(1*time.Second))
		if err != nil {
			errorMsg := fmt.Sprintf("fail to create collection, targetCollectionName: %s err: %s", targetCollectionName, err)
			log.Error(errorMsg)
			task.StateCode = backuppb.RestoreTaskStateCode_FAIL
			task.ErrorMessage = errorMsg
			return task, err
		}
		log.Info("create collection",
			zap.Bool("hasPartitionKey", hasPartitionKey))
	} else {
		log.Info("skip create collection",
			zap.Bool("hasPartitionKey", hasPartitionKey))
	}

	if task.GetDropExistIndex() {
		for _, field := range task.CollBackup.Schema.Fields {
			fieldIndexs, err := b.getMilvusClient().DescribeIndex(b.ctx, targetDBName, targetCollectionName, field.Name)
			if err != nil {
				if strings.Contains(err.Error(), "index not found") ||
					strings.HasPrefix(err.Error(), "index doesn't exist") {
					// todo
					log.Info("field has no index",
						zap.String("field_name", field.Name))
					continue
				} else {
					log.Error("fail in DescribeIndex", zap.Error(err))
					return task, err
				}
			}
			for _, fieldIndex := range fieldIndexs {
				err = b.milvusClient.DropIndex(ctx, targetDBName, targetCollectionName, fieldIndex.Name())
				if err != nil {
					log.Warn("Fail to drop index",
						zap.Error(err))
					return task, err
				}
				log.Info("drop index",
					zap.String("field_name", field.Name),
					zap.String("index_name", fieldIndex.Name()))
			}
		}
	}

	if task.GetRestoreIndex() {
		vectorFields := make(map[string]bool, 0)
		for _, field := range collectionSchema.Fields {
			if strings.HasSuffix(strings.ToLower(field.DataType.Name()), "vector") {
				vectorFields[field.Name] = true
			}
		}
		indexes := task.GetCollBackup().GetIndexInfos()
		for _, index := range indexes {
			var idx indexV2.Index
			log.Info("source index",
				zap.String("indexName", index.GetIndexName()),
				zap.String("indexType", index.GetIndexType()),
				zap.Any("params", index.GetParams()))
			if _, ok := vectorFields[index.GetFieldName()]; ok && task.GetUseAutoIndex() {
				log.Info("use auto index")
				params := make(map[string]string, 0)
				// auto index only support index_type and metric_type in params
				params["index_type"] = "AUTOINDEX"
				params["metric_type"] = index.GetParams()["metric_type"]
				// v1
				//idx = entity.NewGenericIndex(index.GetIndexName(), entity.AUTOINDEX, params)
				// v2
				idx = indexV2.NewGenericIndex(index.GetIndexName(), params)
			} else {
				log.Info("not auto index")
				indexType := index.GetIndexType()
				if indexType == "marisa-trie" {
					indexType = "Trie"
				}
				params := index.GetParams()
				if params["index_type"] == "marisa-trie" {
					params["index_type"] = "Trie"
				}
				// v1
				//idx = entityV2.NewGenericIndex(index.GetIndexName(), entity.IndexType(indexType), index.GetParams())
				// v2
				idx = indexV2.NewGenericIndex(index.GetIndexName(), params)
			}
			err := b.getMilvusClient().CreateIndexV2(ctx, targetDBName, targetCollectionName, index.GetFieldName(), idx, true)
			if err != nil {
				log.Warn("Fail to restore index", zap.Error(err))
				return task, err
			}
		}
	}

	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s%s", parentTaskID, task.TargetDbName, task.TargetCollectionName, SEPERATOR)
	isSameBucket := b.milvusBucketName == backupBucketName
	isSameStorage := b.getMilvusStorageClient().Config().StorageType == b.getBackupStorageClient().Config().StorageType
	// clean the temporary file
	defer func() {
		if !isSameBucket && !b.params.BackupCfg.KeepTempFiles {
			log.Info("Delete temporary file", zap.String("dir", tempDir))
			err := b.getMilvusStorageClient().RemoveWithPrefix(ctx, b.milvusBucketName, tempDir)
			if err != nil {
				log.Warn("Delete temporary file failed", zap.Error(err))
			}
		}
	}()

	// bulk insert
	copyAndBulkInsert := func(dbName, collectionName, partitionName string, files []string, isL0 bool, skipDiskQuotaCheck bool) error {
		realFiles := make([]string, len(files))
		// if milvus bucket and backup bucket are not the same, should copy the data first
		if !isSameBucket || !isSameStorage {
			log.Info("milvus and backup store in different bucket, copy the data first", zap.Strings("files", files), zap.String("copyDataPath", tempDir))
			for i, file := range files {
				// empty delta file, no need to copy
				if file == "" {
					realFiles[i] = file
				} else {
					tempFileKey := path.Join(tempDir, strings.Replace(file, b.params.MinioCfg.BackupRootPath, "", 1)) + SEPERATOR
					log.Debug("Copy temporary restore file", zap.String("from", file), zap.String("to", tempFileKey))
					err := retry.Do(ctx, func() error {
						return b.getRestoreCopier().Copy(ctx, file, tempFileKey, backupBucketName, b.milvusBucketName)
					}, retry.Sleep(2*time.Second), retry.Attempts(5))
					if err != nil {
						log.Error("fail to copy backup date from backup bucket to restore target milvus bucket after retry", zap.Error(err))
						return err
					}
					realFiles[i] = tempFileKey
				}
			}
		} else {
			realFiles = files
		}

		err := b.executeBulkInsert(ctx, dbName, collectionName, partitionName, realFiles, int64(task.GetCollBackup().BackupTimestamp), isL0, skipDiskQuotaCheck)
		if err != nil {
			log.Error("fail to bulk insert to partition",
				zap.String("partition", partitionName),
				zap.Error(err))
			return err
		}
		return nil
	}

	jobIds := make([]int64, 0)
	restoredSize := atomic.Int64{}

	type partitionL0Segment struct {
		collectionID  int64
		partitionName string
		partitionID   int64
		segmentID     int64
	}
	partitionL0Segments := make([]partitionL0Segment, 0)
	for _, v := range task.GetCollBackup().GetPartitionBackups() {
		partitionBackup := v
		log.Info("start restore partition", zap.String("partition", partitionBackup.GetPartitionName()))
		// pre-check whether partition exist, if not create it
		exist, err := b.getMilvusClient().HasPartition(ctx, targetDBName, targetCollectionName, partitionBackup.GetPartitionName())
		if err != nil {
			log.Error("fail to check has partition", zap.Error(err))
			return task, err
		}
		if !exist {
			log.Info("create partition", zap.String("partitionName", partitionBackup.GetPartitionName()))
			err = retry.Do(ctx, func() error {
				return b.getMilvusClient().CreatePartition(ctx, targetDBName, targetCollectionName, partitionBackup.GetPartitionName())
			}, retry.Attempts(10), retry.Sleep(1*time.Second))
			if err != nil {
				log.Error("fail to create partition", zap.Error(err))
				return task, err
			}
		}

		type restoreGroup struct {
			files []string
			size  int64
		}
		restoreFileGroups := make([]restoreGroup, 0)

		var l0Segments []*backuppb.SegmentBackupInfo = lo.Filter(partitionBackup.GetSegmentBackups(), func(segment *backuppb.SegmentBackupInfo, _ int) bool {
			return segment.IsL0
		})
		notl0Segments := lo.Filter(partitionBackup.GetSegmentBackups(), func(segment *backuppb.SegmentBackupInfo, _ int) bool {
			return !segment.IsL0
		})
		groupIds := collectGroupIdsFromSegments(notl0Segments)
		if len(groupIds) == 1 && groupIds[0] == 0 {
			// backward compatible old backup without group id
			files, size, err := b.getBackupPartitionPaths(ctx, backupBucketName, backupPath, partitionBackup)
			if err != nil {
				log.Error("fail to get partition backup binlog files",
					zap.Error(err),
					zap.String("partition", partitionBackup.GetPartitionName()))
				return task, err
			}
			restoreFileGroups = append(restoreFileGroups, restoreGroup{files: files, size: size})
		} else {
			// bulk insert by segment groups
			for _, groupId := range groupIds {
				files, size, err := b.getBackupPartitionPathsWithGroupID(ctx, backupBucketName, backupPath, partitionBackup, groupId)
				if err != nil {
					log.Error("fail to get partition backup binlog files",
						zap.Error(err),
						zap.String("partition", partitionBackup.GetPartitionName()))
					return task, err
				}
				restoreFileGroups = append(restoreFileGroups, restoreGroup{files: files, size: size})
			}
		}

		for _, value := range restoreFileGroups {
			group := value
			job := func(ctx context.Context) error {
				err := copyAndBulkInsert(targetDBName, targetCollectionName, partitionBackup.GetPartitionName(), group.files, false, task.GetSkipDiskQuotaCheck())
				if err != nil {
					return err
				} else {
					b.meta.UpdateRestoreTask(parentTaskID, addCollectionRestoredSize(task.GetCollBackup().GetCollectionId(), group.size))
					restoredSize.Add(group.size)
					task.RestoredSize = restoredSize.Load()
					return nil
				}
			}
			jobId := b.getRestoreWorkerPool(parentTaskID).SubmitWithId(job)
			jobIds = append(jobIds, jobId)
		}

		if len(l0Segments) > 0 {
			for _, segment := range l0Segments {
				partitionL0Segments = append(partitionL0Segments, partitionL0Segment{
					collectionID:  segment.CollectionId,
					partitionName: partitionBackup.GetPartitionName(),
					partitionID:   segment.GetPartitionId(),
					segmentID:     segment.GetSegmentId(),
				})
			}
		}
	}

	err := b.getRestoreWorkerPool(parentTaskID).WaitJobs(jobIds)
	if err != nil {
		return task, err
	}

	// restore l0 segments
	l0JobIds := make([]int64, 0)
	log.Info("start restore l0 segments", zap.Int("global_l0_segment_num", len(task.GetCollBackup().GetL0Segments())), zap.Int("partition_l0_segment_num", len(partitionL0Segments)))
	for _, v := range partitionL0Segments {
		segmentBackup := v
		job := func(ctx context.Context) error {
			l0Files := fmt.Sprintf("%s/%s/%s/%d/%d/%d", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, segmentBackup.collectionID, segmentBackup.partitionID, segmentBackup.segmentID)
			log.Info("restore l0 segment ", zap.String("files", l0Files))
			return copyAndBulkInsert(targetDBName, targetCollectionName, segmentBackup.partitionName, []string{l0Files}, true, task.GetSkipDiskQuotaCheck())
		}
		jobId := b.getRestoreWorkerPool(parentTaskID).SubmitWithId(job)
		l0JobIds = append(l0JobIds, jobId)
	}

	if len(task.GetCollBackup().GetL0Segments()) > 0 {
		for _, v := range task.GetCollBackup().GetL0Segments() {
			segment := v
			job := func(ctx context.Context) error {
				l0Files := fmt.Sprintf("%s/%s/%s/%d/%d/%d", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, task.CollBackup.CollectionId, -1, segment.GetSegmentId())
				log.Info("restore l0 segment ", zap.String("files", l0Files))
				return copyAndBulkInsert(targetDBName, targetCollectionName, "", []string{l0Files}, true, task.GetSkipDiskQuotaCheck())
			}
			jobId := b.getRestoreWorkerPool(parentTaskID).SubmitWithId(job)
			l0JobIds = append(l0JobIds, jobId)
		}
	}
	err = b.getRestoreWorkerPool(parentTaskID).WaitJobs(l0JobIds)

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

func (b *BackupContext) executeBulkInsert(ctx context.Context, db, coll string, partition string, files []string, endTime int64, isL0 bool, skipDiskQuotaCheck bool) error {
	log.Info("execute bulk insert",
		zap.String("db", db),
		zap.String("collection", coll),
		zap.String("partition", partition),
		zap.Strings("files", files),
		zap.Int64("endTime", endTime))
	var taskId int64
	var err error
	if endTime == 0 {
		if isL0 {
			taskId, err = b.getMilvusClient().BulkInsert(ctx, db, coll, partition, files, gomilvus.IsL0(isL0), gomilvus.SkipDiskQuotaCheck(skipDiskQuotaCheck))
		} else {
			taskId, err = b.getMilvusClient().BulkInsert(ctx, db, coll, partition, files, gomilvus.IsBackup(), gomilvus.SkipDiskQuotaCheck(skipDiskQuotaCheck))
		}
	} else {
		if isL0 {
			taskId, err = b.getMilvusClient().BulkInsert(ctx, db, coll, partition, files, gomilvus.IsL0(isL0), gomilvus.SkipDiskQuotaCheck(skipDiskQuotaCheck), gomilvus.WithEndTs(endTime))
		} else {
			taskId, err = b.getMilvusClient().BulkInsert(ctx, db, coll, partition, files, gomilvus.IsBackup(), gomilvus.SkipDiskQuotaCheck(skipDiskQuotaCheck), gomilvus.WithEndTs(endTime))
		}
	}
	if err != nil {
		log.Error("fail to bulk insert",
			zap.String("db", db),
			zap.String("collectionName", coll),
			zap.String("partitionName", partition),
			zap.Strings("files", files),
			zap.Error(err))
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

func (b *BackupContext) getBackupPartitionPaths(ctx context.Context, bucketName string, backupPath string, partition *backuppb.PartitionBackupInfo) ([]string, int64, error) {
	log.Info("getBackupPartitionPaths",
		zap.String("bucketName", bucketName),
		zap.String("backupPath", backupPath),
		zap.Int64("partitionID", partition.PartitionId))

	insertPath := fmt.Sprintf("%s/%s/%s/%v/%v/", backupPath, BINGLOG_DIR, INSERT_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())
	deltaPath := fmt.Sprintf("%s/%s/%s/%v/%v/", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId())

	exist, err := b.getBackupStorageClient().Exist(ctx, bucketName, deltaPath)
	if err != nil {
		log.Warn("check binlog exist fail", zap.Error(err))
		return []string{}, 0, err
	}
	log.Debug("check delta log exist",
		zap.Int64("partitionID", partition.PartitionId),
		zap.String("deltaPath", deltaPath),
		zap.Bool("exist", exist))
	if !exist {
		return []string{insertPath, ""}, partition.GetSize(), nil
	}
	return []string{insertPath, deltaPath}, partition.GetSize(), nil
}

func (b *BackupContext) getBackupPartitionPathsWithGroupID(ctx context.Context, bucketName string, backupPath string, partition *backuppb.PartitionBackupInfo, groupId int64) ([]string, int64, error) {
	log.Info("getBackupPartitionPaths",
		zap.String("bucketName", bucketName),
		zap.String("backupPath", backupPath),
		zap.Int64("partitionID", partition.GetPartitionId()),
		zap.Int64("groupId", groupId))

	insertPath := fmt.Sprintf("%s/%s/%s/%v/%v/%d/", backupPath, BINGLOG_DIR, INSERT_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId(), groupId)
	deltaPath := fmt.Sprintf("%s/%s/%s/%v/%v/%d/", backupPath, BINGLOG_DIR, DELTA_LOG_DIR, partition.GetCollectionId(), partition.GetPartitionId(), groupId)

	var totalSize int64
	for _, seg := range partition.GetSegmentBackups() {
		if seg.GetGroupId() == groupId {
			totalSize += seg.GetSize()
		}
	}

	exist, err := b.getBackupStorageClient().Exist(ctx, bucketName, deltaPath)
	if err != nil {
		log.Warn("check binlog exist fail", zap.Error(err))
		return []string{}, 0, err
	}
	if !exist {
		return []string{insertPath, ""}, totalSize, nil
	}

	return []string{insertPath, deltaPath}, totalSize, nil
}

func (b *BackupContext) restoreRBAC(ctx context.Context, backupInfo *backuppb.BackupInfo) error {
	log.Info("restore RBAC")

	rbacBackup := backupInfo.GetRbacMeta()
	users := make([]*entity.UserInfo, 0)
	roles := make([]*entity.Role, 0)
	grants := make([]*entity.RoleGrants, 0)

	for _, user := range rbacBackup.GetUsers() {
		roles := lo.Map(user.GetRoles(), func(role *backuppb.RoleEntity, index int) string {
			return role.Name
		})
		userEntity := &entity.UserInfo{
			UserDescription: entity.UserDescription{
				Name:  user.GetUser(),
				Roles: roles,
			},
			Password: user.Password,
		}
		users = append(users, userEntity)
	}

	for _, role := range rbacBackup.GetRoles() {
		roleEntity := &entity.Role{
			Name: role.GetName(),
		}
		roles = append(roles, roleEntity)
	}

	for _, roleGrant := range rbacBackup.GetGrants() {
		roleGrantEntity := &entity.RoleGrants{
			Object:        roleGrant.Object.GetName(),
			ObjectName:    roleGrant.GetObjectName(),
			RoleName:      roleGrant.GetRole().GetName(),
			GrantorName:   roleGrant.GetGrantor().GetUser().GetName(),
			PrivilegeName: roleGrant.GetGrantor().GetPrivilege().GetName(),
			DbName:        roleGrant.GetDbName(),
		}
		grants = append(grants, roleGrantEntity)
	}

	rbacMeta := &entity.RBACMeta{
		Users:      users,
		Roles:      roles,
		RoleGrants: grants,
	}

	log.Info("restore RBAC", zap.Int("users", len(users)), zap.Int("roles", len(roles)), zap.Int("grants", len(grants)))
	err := b.getMilvusClient().RestoreRBAC(ctx, rbacMeta)
	return err
}
