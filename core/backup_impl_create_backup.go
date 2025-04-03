package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/common"
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
		zap.String("databaseCollections", utils.GetCreateDBCollections(request)),
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

	backup := &backuppb.BackupInfo{
		Id:            request.GetRequestId(),
		StateCode:     backuppb.BackupTaskStateCode_BACKUP_INITIAL,
		StartTime:     time.Now().UnixNano() / int64(time.Millisecond),
		Name:          request.BackupName,
		MilvusVersion: milvusVersion,
	}
	b.meta.AddBackup(backup)
	//levelBackupInfo := NewLeveledBackupInfo(backup)
	//b.backupTasksCache.Store(request.GetRequestId(), levelBackupInfo)
	//b.backupNameIdDict.Store(name, request.GetRequestId())

	if request.Async {
		go b.executeCreateBackup(ctx, request, backup)
		asyncResp := &backuppb.BackupInfoResponse{
			RequestId: request.GetRequestId(),
			Code:      backuppb.ResponseCode_Success,
			Msg:       "create backup is executing asynchronously",
			Data:      backup,
		}
		return asyncResp
	} else {
		err := b.executeCreateBackup(ctx, request, backup)
		resp.Data = b.meta.GetBackup(backup.GetId())
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

type collectionStruct struct {
	db             string
	collectionName string
}

// parse collections to backup
// For backward compatibility：
//
//	1，parse dbCollections first,
//	2，if dbCollections not set, use collectionNames
func (b *BackupContext) parseBackupCollections(request *backuppb.CreateBackupRequest) ([]collectionStruct, error) {
	log.Debug("Request collection names",
		zap.Strings("request_collection_names", request.GetCollectionNames()),
		zap.String("request_db_collections", utils.GetCreateDBCollections(request)),
		zap.Int("length", len(request.GetCollectionNames())))
	var toBackupCollections []collectionStruct

	dbCollectionsStr := utils.GetCreateDBCollections(request)
	// first priority: dbCollections
	if dbCollectionsStr != "" {
		var dbCollections meta.DbCollections
		err := jsoniter.UnmarshalFromString(dbCollectionsStr, &dbCollections)
		if err != nil {
			log.Error("fail in unmarshal dbCollections in CreateBackupRequest", zap.String("dbCollections", dbCollectionsStr), zap.Error(err))
			return nil, err
		}
		for db, collections := range dbCollections {
			if len(collections) == 0 {
				resp, err := b.getMilvusClient().ListCollections(b.ctx, db)
				if err != nil {
					log.Error("fail in ListCollections", zap.Error(err))
					return nil, err
				}
				for _, coll := range resp.CollectionNames {
					log.Debug("Add collection to toBackupCollections", zap.String("db", db), zap.String("collection", coll))
					toBackupCollections = append(toBackupCollections, collectionStruct{db, coll})
				}
			} else {
				for _, coll := range collections {
					toBackupCollections = append(toBackupCollections, collectionStruct{db, coll})
				}
			}
		}
		log.Debug("Parsed backup collections from request.db_collections", zap.Int("length", len(toBackupCollections)))
		return toBackupCollections, nil
	}

	if request.GetCollectionNames() == nil || len(request.GetCollectionNames()) == 0 {
		dbs, err := b.getMilvusClient().ListDatabases(b.ctx)
		if err != nil {
			// compatible to milvus under v2.2.8 without database support
			if !b.getMilvusClient().SupportMultiDatabase() {
				// default database only
				resp, err := b.getMilvusClient().ListCollections(b.ctx, "default")
				if err != nil {
					log.Error("fail in ListCollections", zap.Error(err))
					return nil, err
				}
				for _, coll := range resp.CollectionNames {
					toBackupCollections = append(toBackupCollections, collectionStruct{"default", coll})
				}
			} else {
				log.Error("fail in ListDatabases", zap.Error(err))
				return nil, err
			}
		} else {
			for _, db := range dbs {
				resp, err := b.getMilvusClient().ListCollections(b.ctx, db)
				if err != nil {
					log.Error("fail in ListCollections", zap.Error(err))
					return nil, err
				}
				for _, coll := range resp.CollectionNames {
					toBackupCollections = append(toBackupCollections, collectionStruct{db, coll})
				}
			}
		}
		log.Debug(fmt.Sprintf("List %v collections", len(toBackupCollections)))
	} else {
		for _, collectionName := range request.GetCollectionNames() {
			var dbName = "default"
			if strings.Contains(collectionName, ".") {
				splits := strings.Split(collectionName, ".")
				dbName = splits[0]
				collectionName = splits[1]
			}

			exist, err := b.getMilvusClient().HasCollection(b.ctx, dbName, collectionName)
			if err != nil {
				log.Error("fail in HasCollection", zap.Error(err))
				return nil, err
			}
			if !exist {
				errMsg := fmt.Sprintf("request backup collection does not exist: %s.%s", dbName, collectionName)
				log.Error(errMsg)
				return nil, errors.New(errMsg)
			}
			toBackupCollections = append(toBackupCollections, collectionStruct{dbName, collectionName})
		}
	}

	return toBackupCollections, nil
}

func (b *BackupContext) pauseMilvusGC(ctx context.Context, gcAddress string, pauseSeconds int) {
	pauseAPI := "/management/datacoord/garbage_collection/pause"
	params := url.Values{}
	params.Add("pause_seconds", strconv.Itoa(pauseSeconds))
	fullURL := fmt.Sprintf("%s?%s", gcAddress+pauseAPI, params.Encode())
	response, err := http.Get(fullURL)

	if err != nil {
		log.Warn("Pause Milvus GC Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	defer response.Body.Close()
	// Read the response body
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Warn("Read response Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	log.Info("Pause Milvus GC response", zap.String("response", string(body)), zap.String("address", gcAddress), zap.Int("pauseSeconds", pauseSeconds))
}

func (b *BackupContext) resumeMilvusGC(ctx context.Context, gcAddress string) {
	pauseAPI := "/management/datacoord/garbage_collection/resume"
	fullURL := gcAddress + pauseAPI
	response, err := http.Get(fullURL)
	if err != nil {
		log.Warn("Resume Milvus GC Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	// Read the response body
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Warn("Read response Error:"+GcWarnMessage, zap.Error(err))
		return
	}
	log.Info("Resume Milvus GC response", zap.String("response", string(body)), zap.String("address", gcAddress))
}

func (b *BackupContext) executeCreateBackup(ctx context.Context, request *backuppb.CreateBackupRequest, backupInfo *backuppb.BackupInfo) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// set backup state
	b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_EXECUTING))

	// pause GC
	if request.GetGcPauseEnable() || b.params.BackupCfg.GcPauseEnable {
		var pause = 0
		if request.GetGcPauseSeconds() == 0 {
			pause = b.params.BackupCfg.GcPauseSeconds
		} else {
			pause = int(request.GetGcPauseSeconds())
		}
		var gcAddress string = ""
		if request.GetGcPauseAddress() == "" {
			gcAddress = b.params.BackupCfg.GcPauseAddress
		} else {
			gcAddress = request.GetGcPauseAddress()
		}
		b.pauseMilvusGC(ctx, gcAddress, pause)
		defer b.resumeMilvusGC(ctx, gcAddress)
	}

	// 1, get collection level meta
	toBackupCollections, err := b.parseBackupCollections(request)
	if err != nil {
		log.Error("parse backup collections from request failed", zap.Error(err))
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
		return err
	}
	collectionNames := make([]string, len(toBackupCollections))
	for i, coll := range toBackupCollections {
		collectionNames[i] = coll.db + "." + coll.collectionName
	}
	log.Info("collections to backup", zap.Strings("collections", collectionNames))

	wp, err := common.NewWorkerPool(ctx, b.params.BackupCfg.BackupCollectionParallelism, RPS)
	if err != nil {
		log.Error("create worker pool failed", zap.Error(err))
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
		return err
	}
	wp.Start()

	opt := b.newCollectionTaskOpt(request)
	for _, dbColl := range toBackupCollections {
		job := func(ctx context.Context) error {
			task := backup.NewCollectionTask(dbColl.db, dbColl.collectionName, opt)
			if err := task.Execute(ctx); err != nil {
				return fmt.Errorf("create: backup collection %s.%s failed, err: %w", dbColl.db, dbColl.collectionName, err)
			}

			return nil
		}
		wp.Submit(job)
	}
	wp.Done()
	if err := wp.Wait(); err != nil {
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL), meta.SetErrorMessage(err.Error()))
		return err
	}

	backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_SUCCESS
	b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_SUCCESS), meta.SetEndTime(time.Now().UnixNano()/int64(time.Millisecond)))

	if request.GetRbac() {
		err = b.backupRBAC(ctx, backupInfo)
		if err != nil {
			backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_FAIL
			backupInfo.ErrorMessage = err.Error()
			return err
		}
	}

	// 7, write meta data
	err = b.writeBackupInfoMeta(ctx, backupInfo.GetId())
	if err != nil {
		backupInfo.StateCode = backuppb.BackupTaskStateCode_BACKUP_FAIL
		backupInfo.ErrorMessage = err.Error()
		b.meta.UpdateBackup(backupInfo.Id, meta.SetStateCode(backuppb.BackupTaskStateCode_BACKUP_FAIL),
			meta.SetErrorMessage(err.Error()))
		return err
	}
	log.Info("finish backup all collections",
		zap.String("requestId", request.GetRequestId()),
		zap.String("backupName", request.GetBackupName()),
		zap.Strings("collections", request.GetCollectionNames()),
		zap.Bool("async", request.GetAsync()))
	return nil
}

func (b *BackupContext) writeBackupInfoMeta(ctx context.Context, id string) error {
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

func (b *BackupContext) newCollectionTaskOpt(request *backuppb.CreateBackupRequest) backup.CollectionOpt {
	backupPath := path.Join(b.params.MinioCfg.BackupRootPath, request.GetBackupName())

	return backup.CollectionOpt{
		BackupID:       request.GetRequestId(),
		MetaOnly:       request.GetMetaOnly(),
		SkipFlush:      request.GetForce(),
		MilvusStorage:  b.getMilvusStorageClient(),
		MilvusRootPath: b.params.MinioCfg.RootPath,
		MilvusBucket:   b.params.MinioCfg.BucketName,
		BackupBucket:   b.backupBucketName,
		BackupStorage:  b.getBackupStorageClient(),
		BackupPath:     backupPath,
		Meta:           b.meta,
		Grpc:           b.getMilvusClient(),
	}
}

func (b *BackupContext) backupRBAC(ctx context.Context, backupInfo *backuppb.BackupInfo) error {
	log.Info("backup RBAC")
	resp, err := b.getMilvusClient().BackupRBAC(ctx)
	if err != nil {
		log.Error("fail in BackupMeta", zap.Error(err))
		return err
	}
	rbacMeta := resp.RBACMeta

	users := make([]*backuppb.UserInfo, 0, len(rbacMeta.Users))
	roles := make([]*backuppb.RoleEntity, 0, len(rbacMeta.Roles))
	grants := make([]*backuppb.GrantEntity, 0, len(rbacMeta.Grants))
	for _, user := range rbacMeta.Users {
		rs := lo.Map(user.Roles, func(role *milvuspb.RoleEntity, index int) *backuppb.RoleEntity {
			return &backuppb.RoleEntity{Name: role.Name}
		})
		userP := &backuppb.UserInfo{
			User:     user.User,
			Password: user.Password,
			Roles:    rs,
		}
		users = append(users, userP)
	}

	for _, role := range rbacMeta.Roles {
		roleP := &backuppb.RoleEntity{
			Name: role.Name,
		}
		roles = append(roles, roleP)
	}

	for _, roleGrant := range rbacMeta.Grants {
		roleGrantP := &backuppb.GrantEntity{
			Role: &backuppb.RoleEntity{
				Name: roleGrant.Role.Name,
			},
			Object: &backuppb.ObjectEntity{
				Name: roleGrant.Object.Name,
			},
			ObjectName: roleGrant.ObjectName,
			Grantor: &backuppb.GrantorEntity{
				User: &backuppb.UserEntity{
					Name: roleGrant.Grantor.User.Name,
				},
				Privilege: &backuppb.PrivilegeEntity{
					Name: roleGrant.Grantor.Privilege.Name,
				},
			},
			DbName: roleGrant.DbName,
		}
		grants = append(grants, roleGrantP)
	}

	rbacPb := &backuppb.RBACMeta{
		Users:  users,
		Roles:  roles,
		Grants: grants,
	}

	log.Info("backup RBAC", zap.Int("users", len(users)), zap.Int("roles", len(roles)), zap.Int("grants", len(grants)))
	b.meta.UpdateBackup(backupInfo.Id, meta.SetRBACMeta(rbacPb))
	return nil
}
