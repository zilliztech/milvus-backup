package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/restore"

	jsoniter "github.com/json-iterator/go"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
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
		zap.Any("skipParams", request.GetSkipParams()),
		zap.Bool("useV2Restore", request.GetUseV2Restore()),
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
		backupPath = b.backupRootPath + meta.SEPERATOR + request.GetBackupName()
	} else {
		backupBucketName = request.GetBucketName()
		backupPath = request.GetPath() + meta.SEPERATOR + request.GetBackupName()
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
			if db == targetDBName {
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

		// check if the collection exist, if existed, will not restore
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
			SkipParams:            request.GetSkipParams(),
			UseV2Restore:          request.GetUseV2Restore(),
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
	b.meta.UpdateRestoreTask(id, meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_EXECUTING))
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
				b.meta.UpdateRestoreTask(id, meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_FAIL),
					meta.SetRestoreErrorMessage(endTask.ErrorMessage))
				b.meta.UpdateRestoreCollectionTask(id, endTask.Id,
					meta.SetRestoreCollectionStateCode(backuppb.RestoreTaskStateCode_FAIL),
					meta.SetRestoreCollectionErrorMessage(endTask.ErrorMessage))
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
	b.meta.UpdateRestoreTask(id, meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_SUCCESS), meta.SetRestoreEndTime(endTime))

	log.Info("finish restore all collections",
		zap.String("backupName", backup.GetName()),
		zap.Int("collections", len(backup.GetCollectionBackups())),
		zap.String("taskID", task.GetId()),
		zap.Int64("duration in seconds", task.GetEndTime()-task.GetStartTime()))
	return task, nil
}

func (b *BackupContext) executeRestoreCollectionTask(ctx context.Context, backupBucketName string, backupPath string, task *backuppb.RestoreCollectionTask, parentTaskID string) (*backuppb.RestoreCollectionTask, error) {
	collTask := restore.NewCollectionTask(task,
		b.meta,
		b.params,
		parentTaskID,
		backupBucketName,
		backupPath,
		b.getBackupStorageClient(),
		b.getMilvusStorageClient(),
		b.getMilvusClient(),
		b.getRestfulClient())

	err := collTask.Execute(ctx)

	return task, err
}

func (b *BackupContext) restoreRBAC(ctx context.Context, backupInfo *backuppb.BackupInfo) error {
	log.Info("restore RBAC")

	resp, err := b.getMilvusClient().BackupRBAC(ctx)
	if err != nil {
		return fmt.Errorf("fail to get current RBAC, err: %s", err)
	}
	curRBAC := resp.RBACMeta

	rbacBackup := backupInfo.GetRbacMeta()
	curUsers := lo.SliceToMap(curRBAC.Users, func(user *milvuspb.UserInfo) (string, struct{}) {
		return user.User, struct{}{}
	})
	users := make([]*milvuspb.UserInfo, 0, len(rbacBackup.GetUsers()))
	for _, user := range rbacBackup.GetUsers() {
		// skip if user already exist
		if _, ok := curUsers[user.GetUser()]; ok {
			continue
		}

		ur := lo.Map(user.GetRoles(), func(role *backuppb.RoleEntity, index int) *milvuspb.RoleEntity {
			return &milvuspb.RoleEntity{Name: role.Name}
		})
		userEntity := &milvuspb.UserInfo{
			User:     user.User,
			Password: user.Password,
			Roles:    ur,
		}
		users = append(users, userEntity)
	}

	curRoles := lo.SliceToMap(curRBAC.Roles, func(role *milvuspb.RoleEntity) (string, struct{}) {
		return role.Name, struct{}{}
	})
	roles := make([]*milvuspb.RoleEntity, 0, len(rbacBackup.GetRoles()))
	for _, role := range rbacBackup.GetRoles() {
		// skip if role already exist
		if _, ok := curRoles[role.GetName()]; ok {
			continue
		}

		roleEntity := &milvuspb.RoleEntity{
			Name: role.GetName(),
		}
		roles = append(roles, roleEntity)
	}

	grants := make([]*milvuspb.GrantEntity, 0, len(rbacBackup.GetGrants()))
	curGrants := lo.SliceToMap(curRBAC.Grants, func(grant *milvuspb.GrantEntity) (string, struct{}) {
		return fmt.Sprintf("%s/%s/%s/%s/%s/%s", grant.Object, grant.ObjectName, grant.Role.Name, grant.Grantor.User, grant.Grantor.Privilege.Name, grant.DbName), struct{}{}
	})
	for _, roleGrant := range rbacBackup.GetGrants() {
		key := fmt.Sprintf("%s/%s/%s/%s/%s/%s", roleGrant.Object.GetName(), roleGrant.GetObjectName(), roleGrant.GetRole().GetName(), roleGrant.GetGrantor().GetUser().GetName(), roleGrant.GetGrantor().GetPrivilege().GetName(), roleGrant.GetDbName())
		// skip if grant already exist
		if _, ok := curGrants[key]; ok {
			continue
		}
		roleGrantEntity := &milvuspb.GrantEntity{
			Object:     &milvuspb.ObjectEntity{Name: roleGrant.Object.Name},
			ObjectName: roleGrant.GetObjectName(),
			Role:       &milvuspb.RoleEntity{Name: roleGrant.Role.Name},
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: roleGrant.Grantor.User.Name},
				Privilege: &milvuspb.PrivilegeEntity{Name: roleGrant.Grantor.Privilege.Name},
			},
			DbName: roleGrant.GetDbName(),
		}
		grants = append(grants, roleGrantEntity)
	}

	rbacMeta := &milvuspb.RBACMeta{
		Users:  users,
		Roles:  roles,
		Grants: grants,
	}

	log.Info("restore RBAC", zap.Int("users", len(users)), zap.Int("roles", len(roles)), zap.Int("grants", len(grants)))
	err = b.getMilvusClient().RestoreRBAC(ctx, rbacMeta)
	return err
}
