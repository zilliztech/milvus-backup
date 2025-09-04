package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/meta/taskmgr"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/validate"
)

// RestoreBackup Restore interface
// @Summary Restore interface
// @Description Submit a request to restore the data from backup
// @Tags Restore
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body backuppb.RestoreBackupRequest   true  "RestoreBackupRequest JSON"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /restore [post]
func (s *Server) handleRestoreBackup(c *gin.Context) {
	var request backuppb.RestoreBackupRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request body: %s", err)})
		return
	}

	h := newRestoreHandler(&request, s.params)
	resp := h.run(context.Background())

	c.JSON(http.StatusOK, resp)
}

type restoreHandler struct {
	params  *paramtable.BackupParams
	request *backuppb.RestoreBackupRequest

	milvusClient  milvus.Grpc
	restfulClient milvus.Restful

	backupStorage  storage.Client
	backupRootPath string

	milvusStorage storage.Client
}

func newRestoreHandler(request *backuppb.RestoreBackupRequest, params *paramtable.BackupParams) *restoreHandler {
	return &restoreHandler{request: request, params: params}
}

func (h *restoreHandler) run(ctx context.Context) *backuppb.RestoreBackupResponse {
	h.complete()
	if err := h.validate(); err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Parameter_Error, Msg: err.Error()}
	}

	if err := h.initClient(); err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}

	backupDir := mpath.BackupDir(h.backupRootPath, h.request.GetBackupName())
	exist, err := h.checkExist(ctx, backupDir)
	if err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}
	if !exist {
		msg := fmt.Sprintf("backup %s not found", h.request.GetBackupName())
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Parameter_Error, Msg: msg}
	}

	task, err := h.newTask(ctx, backupDir)
	if err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}

	if h.request.GetAsync() {
		return h.runAsync(task)
	} else {
		return h.runSync(ctx, task)
	}
}

func (h *restoreHandler) validate() error {
	if len(h.request.GetBackupName()) == 0 {
		return errors.New("backup name is required")
	}

	if h.request.GetRestorePlan() != nil && len(h.request.GetCollectionSuffix()) != 0 {
		return errors.New("restore plan and collection suffix cannot be set at the same time")
	}

	if h.request.GetRestorePlan() != nil && len(h.request.GetCollectionRenames()) != 0 {
		return errors.New("restore plan and collection renames cannot be set at the same time")
	}

	if len(h.request.GetCollectionSuffix()) != 0 {
		if has := validate.HasSpecialChar(h.request.GetCollectionSuffix()); has {
			return errors.New("only alphanumeric characters and underscores are allowed in collection suffix")
		}
	}

	if h.request.GetDropExistCollection() && h.request.GetSkipCreateCollection() {
		return errors.New("drop_exist_collection and skip_create_collection cannot be true at the same time")
	}

	return nil
}

func (h *restoreHandler) complete() {
	if len(h.request.GetRequestId()) == 0 {
		h.request.RequestId = uuid.NewString()
	}

	if len(h.request.GetId()) == 0 {
		taskID := "restore_" + fmt.Sprint(time.Now().UTC().Format("2006_01_02_15_04_05_")) + fmt.Sprint(time.Now().Nanosecond())
		h.request.Id = taskID
	}
}

func (h *restoreHandler) initClient() error {
	milvusClient, err := milvus.NewGrpc(&h.params.MilvusCfg)
	if err != nil {
		return fmt.Errorf("server: create milvus client: %w", err)
	}

	restfulClient, err := milvus.NewRestful(&h.params.MilvusCfg)
	if err != nil {
		return fmt.Errorf("server: create restful client: %w", err)
	}

	backupParams := h.params.MinioCfg
	if h.request.GetBucketName() != "" {
		log.Info("use bucket name from request", zap.String("bucketName", h.request.GetBucketName()))
		backupParams.BackupBucketName = h.request.GetBucketName()
	}
	backupStorage, err := storage.NewBackupStorage(context.Background(), &backupParams)
	if err != nil {
		return fmt.Errorf("server: create backup storage: %w", err)
	}
	backupRootPath := h.params.MinioCfg.BackupRootPath
	if h.request.GetPath() != "" {
		log.Info("use path from request", zap.String("path", h.request.GetPath()))
		backupRootPath = h.request.GetPath()
	}

	milvusStorage, err := storage.NewMilvusStorage(context.Background(), &h.params.MinioCfg)
	if err != nil {
		return fmt.Errorf("server: create milvus storage: %w", err)
	}

	h.milvusClient = milvusClient
	h.restfulClient = restfulClient
	h.backupStorage = backupStorage
	h.milvusStorage = milvusStorage

	h.backupRootPath = backupRootPath

	return nil
}

func (h *restoreHandler) checkExist(ctx context.Context, backupDir string) (bool, error) {
	exist, err := meta.Exist(ctx, h.backupStorage, backupDir)
	if err != nil {
		return false, fmt.Errorf("server: check backup exist: %w", err)
	}

	return exist, nil
}

func (h *restoreHandler) newTask(ctx context.Context, backupDir string) (*restore.Task, error) {
	backup, err := meta.Read(ctx, h.backupStorage, backupDir)
	if err != nil {
		return nil, fmt.Errorf("server: read backup: %w", err)
	}

	plan, err := newPlanFromRequest(h.request)
	if err != nil {
		return nil, fmt.Errorf("server: create restore plan: %w", err)
	}

	args := restore.TaskArgs{
		TaskID:        h.request.GetId(),
		Backup:        backup,
		Plan:          plan,
		Option:        newOptionFromRequest(h.request),
		Params:        h.params,
		BackupDir:     backupDir,
		BackupStorage: h.backupStorage,
		MilvusStorage: h.milvusStorage,
		Grpc:          h.milvusClient,
		Restful:       h.restfulClient,
	}
	task, err := restore.NewTask(args)
	if err != nil {
		return nil, fmt.Errorf("backup: new restore task fail, err: %w", err)
	}

	if err := task.Prepare(ctx); err != nil {
		return nil, fmt.Errorf("backup: build restore collection task fail, err: %w", err)
	}

	return task, nil
}

func (h *restoreHandler) runSync(ctx context.Context, task *restore.Task) *backuppb.RestoreBackupResponse {
	if err := task.Execute(ctx); err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}

	resp := backuppb.RestoreBackupResponse{RequestId: h.request.GetRequestId()}
	taskView, err := taskmgr.DefaultMgr.GetRestoreTask(h.request.GetId())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		log.Error("get restore task fail", zap.String("taskId", h.request.GetId()), zap.Error(err))
		resp.Msg = err.Error()
		return &resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.RestoreTaskViewToResp(taskView)

	return &resp
}

func (h *restoreHandler) runAsync(task *restore.Task) *backuppb.RestoreBackupResponse {
	go func() {
		if err := task.Execute(context.Background()); err != nil {
			log.Error("restore backup task execute fail", zap.String("backupId", h.request.GetId()), zap.Error(err))
		}
	}()

	resp := backuppb.RestoreBackupResponse{RequestId: h.request.GetRequestId()}
	taskView, err := taskmgr.DefaultMgr.GetRestoreTask(h.request.GetId())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		log.Error("get restore task fail", zap.String("taskId", h.request.GetId()), zap.Error(err))
		resp.Msg = err.Error()
		return &resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "restore backup is executing asynchronously"
	resp.Data = pbconv.RestoreTaskViewToResp(taskView)
	return &resp
}

func newSkipParamsFromRequest(request *backuppb.RestoreBackupRequest) restore.SkipParams {
	return restore.SkipParams{
		CollectionProperties: request.GetSkipParams().GetCollectionProperties(),
		FieldIndexParams:     request.GetSkipParams().GetFieldIndexParams(),
		FieldTypeParams:      request.GetSkipParams().GetFieldTypeParams(),
		IndexParams:          request.GetSkipParams().GetIndexParams(),
	}
}

func newOptionFromRequest(request *backuppb.RestoreBackupRequest) *restore.Option {
	return &restore.Option{
		DropExistIndex:       request.GetDropExistIndex(),
		RebuildIndex:         request.GetRestoreIndex(),
		UseAutoIndex:         request.GetUseAutoIndex(),
		DropExistCollection:  request.GetDropExistCollection(),
		SkipCreateCollection: request.GetSkipCreateCollection(),
		MaxShardNum:          request.GetMaxShardNum(),
		SkipParams:           newSkipParamsFromRequest(request),
		MetaOnly:             request.GetMetaOnly(),
		UseV2Restore:         request.GetUseV2Restore(),
		TruncateBinlogByTs:   request.GetTruncateBinlogByTs(),
		RestoreRBAC:          request.GetRbac(),
	}
}

func newPlanFromRequest(request *backuppb.RestoreBackupRequest) (*restore.Plan, error) {
	dbFilter, err := newDBBackupFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create db backup filter: %w", err)
	}
	collFilter, err := newCollBackupFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create coll backup filter: %w", err)
	}

	dbMapper, err := newDBMapper(request.GetRestorePlan())
	if err != nil {
		return nil, fmt.Errorf("restore: create db mapper: %w", err)
	}
	collMapper, err := newCollMapper(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create coll mapper: %w", err)
	}

	dbTaskFilter, err := newDBTaskFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create db task filter: %w", err)
	}
	collTaskFilter, err := newCollTaskFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create coll task filter: %w", err)
	}

	return &restore.Plan{
		DBBackupFilter:   dbFilter,
		CollBackupFilter: collFilter,
		DBMapper:         dbMapper,
		CollMapper:       collMapper,
		DBTaskFilter:     dbTaskFilter,
		CollTaskFilter:   collTaskFilter,
	}, nil

}

// rename map format: key: oldName, value: newName
// rule 1. key: db1.* value: db2.*
// rule 2. key: db1.coll1 value: db2.coll2
// rule 3. key: coll1 value: coll2 , under default db
// rule 4. key: db1. value: db2.

var (
	_rule1Regex = regexp.MustCompile(`^(\w+)\.\*$`)
	_rule2Regex = regexp.MustCompile(`^(\w+)\.(\w+)$`)
	_rule3Regex = regexp.MustCompile(`^(\w+)$`)
	_rule4Regex = regexp.MustCompile(`^(\w+)\.$`)
)

func inferRuleType(k, v string) (int, error) {
	if _rule1Regex.MatchString(k) && _rule1Regex.MatchString(v) {
		return 1, nil
	}

	if _rule2Regex.MatchString(k) && _rule2Regex.MatchString(v) {
		return 2, nil
	}

	if _rule3Regex.MatchString(k) && _rule3Regex.MatchString(v) {
		return 3, nil
	}

	if _rule4Regex.MatchString(k) && _rule4Regex.MatchString(v) {
		return 4, nil
	}

	return 0, fmt.Errorf("restore: invalid rename rule: %s -> %s", k, v)
}

// newRenameGenerator creates a new mapRenamer with the given rename map.
func newTableMapperFromCollRename(collRename map[string]string) (*restore.TableMapper, error) {
	// add default db in collection_renames if not set
	nsMapping := make(map[string][]namespace.NS)
	dbWildcard := make(map[string]string)

	for k, v := range collRename {
		rule, err := inferRuleType(k, v)
		if err != nil {
			return nil, err
		}

		switch rule {
		case 1:
			dbWildcard[k[:len(k)-2]] = v[:len(v)-2]
		case 2, 3:
			oldNS, err := namespace.Parse(k)
			if err != nil {
				return nil, fmt.Errorf("restore: parse namespace %s %w", k, err)
			}
			newNS, err := namespace.Parse(v)
			if err != nil {
				return nil, fmt.Errorf("restore: parse namespace %s %w", v, err)
			}

			nsMapping[oldNS.String()] = append(nsMapping[oldNS.String()], newNS)
		case 4:
			// handle in db mapping
			continue
		}
	}

	return &restore.TableMapper{DBWildcard: dbWildcard, NSMapping: nsMapping}, nil
}

func newCollMapperFromPlan(plan *backuppb.RestorePlan) (restore.CollMapper, error) {
	nsMapping := make(map[string][]namespace.NS)
	for _, mapping := range plan.Mapping {
		if mapping.GetSource() == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.GetTarget() == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		for _, collMapping := range mapping.Colls {
			oldNS := namespace.New(mapping.GetSource(), collMapping.GetSource())
			newNS := namespace.New(mapping.GetTarget(), collMapping.GetTarget())
			nsMapping[oldNS.String()] = append(nsMapping[oldNS.String()], newNS)
		}
	}

	return &restore.TableMapper{NSMapping: nsMapping}, nil
}

func newCollMapper(request *backuppb.RestoreBackupRequest) (restore.CollMapper, error) {
	if request.GetRestorePlan() != nil {
		return newCollMapperFromPlan(request.GetRestorePlan())
	}

	if len(request.GetCollectionRenames()) != 0 {
		mapper, err := newTableMapperFromCollRename(request.GetCollectionRenames())
		if err != nil {
			return nil, fmt.Errorf("restore: create map renamer %w", err)
		}
		return mapper, nil
	}

	if len(request.GetCollectionSuffix()) != 0 {
		mapper := restore.NewSuffixMapper(request.GetCollectionSuffix())
		return mapper, nil
	}

	return restore.NewDefaultCollMapper(), nil
}

func newDBMapper(plan *backuppb.RestorePlan) (map[string][]restore.DBMapping, error) {
	if plan == nil {
		return nil, nil
	}

	dbMapper := make(map[string][]restore.DBMapping)
	for _, mapping := range plan.Mapping {
		if mapping.GetSource() == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.GetTarget() == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		mapper := restore.DBMapping{Target: mapping.GetTarget(), WithProp: mapping.GetWithProp()}
		dbMapper[mapping.GetSource()] = append(dbMapper[mapping.GetSource()], mapper)
	}

	return dbMapper, nil
}

func newDBFilterFromDBCollections(dbCollections string) (map[string]struct{}, error) {
	dbColl := meta.DbCollections{}
	if err := json.Unmarshal([]byte(dbCollections), &dbColl); err != nil {
		return nil, fmt.Errorf("restore: unmarshal dbCollections: %w", err)
	}

	dbFilter := make(map[string]struct{}, len(dbColl))
	for dbName := range dbColl {
		if dbName == "" {
			dbName = namespace.DefaultDBName
		}
		dbFilter[dbName] = struct{}{}
	}

	return dbFilter, nil
}

func newDBBackupFilter(request *backuppb.RestoreBackupRequest) (map[string]struct{}, error) {
	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollections())
	if dbCollectionsStr != "" {
		return newDBFilterFromDBCollections(dbCollectionsStr)
	}

	// from collection names
	if len(request.GetCollectionNames()) != 0 {
		dbFilter := make(map[string]struct{}, len(request.GetCollectionNames()))
		for _, ns := range request.GetCollectionNames() {
			dbName, err := namespace.Parse(ns)
			if err != nil {
				return nil, fmt.Errorf("restore: parse namespace %s: %w", ns, err)
			}
			dbFilter[dbName.DBName()] = struct{}{}
		}

		return dbFilter, nil
	}

	return nil, nil
}

func newCollFilterFromDBCollections(dbCollections string) (map[string]restore.CollFilter, error) {
	dbColls := meta.DbCollections{}
	if err := json.Unmarshal([]byte(dbCollections), &dbColls); err != nil {
		return nil, fmt.Errorf("restore: unmarshal dbCollections: %w", err)
	}

	collBackupFilter := make(map[string]restore.CollFilter, len(dbColls))
	for dbName, colls := range dbColls {
		if dbName == "" {
			dbName = namespace.DefaultDBName
		}

		if len(colls) == 0 {
			collBackupFilter[dbName] = restore.CollFilter{AllowAll: true}
		} else {
			collName := make(map[string]struct{}, len(colls))
			for _, coll := range colls {
				collName[coll] = struct{}{}
			}

			collBackupFilter[dbName] = restore.CollFilter{CollName: collName}
		}
	}

	return collBackupFilter, nil
}

func newCollFilterFromCollectionNames(collectionNames []string) (map[string]restore.CollFilter, error) {
	collBackupFilter := make(map[string]restore.CollFilter)
	for _, ns := range collectionNames {
		dbName, err := namespace.Parse(ns)
		if err != nil {
			return nil, fmt.Errorf("restore: parse namespace %s: %w", ns, err)
		}
		filter, ok := collBackupFilter[dbName.DBName()]
		if !ok {
			filter = restore.CollFilter{CollName: make(map[string]struct{})}
			collBackupFilter[dbName.DBName()] = filter
		}
		filter.CollName[dbName.CollName()] = struct{}{}
	}

	return collBackupFilter, nil
}

func newCollBackupFilter(request *backuppb.RestoreBackupRequest) (map[string]restore.CollFilter, error) {
	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollections())
	if dbCollectionsStr != "" {
		return newCollFilterFromDBCollections(dbCollectionsStr)
	}

	// from collection names
	if len(request.GetCollectionNames()) != 0 {
		return newCollFilterFromCollectionNames(request.GetCollectionNames())
	}

	return nil, nil
}

func newDBTaskFilterFromPlan(plan *backuppb.RestorePlan) (map[string]struct{}, error) {
	dbTaskFilter := make(map[string]struct{})
	for dbName := range plan.GetFilter() {
		dbTaskFilter[dbName] = struct{}{}
	}

	return dbTaskFilter, nil
}

func newDBTaskFilter(request *backuppb.RestoreBackupRequest) (map[string]struct{}, error) {
	// from restore plan
	if request.GetRestorePlan() != nil {
		return newDBTaskFilterFromPlan(request.GetRestorePlan())
	}

	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollectionsAfterRename())
	if dbCollectionsStr != "" {
		return newDBFilterFromDBCollections(dbCollectionsStr)
	}

	return nil, nil
}

func newCollTaskFilterFromPlan(plan *backuppb.RestorePlan) map[string]restore.CollFilter {
	collTaskFilter := make(map[string]restore.CollFilter)
	for dbName, filter := range plan.GetFilter() {
		if len(filter.GetColls()) == 1 && filter.GetColls()[0] == "*" {
			collTaskFilter[dbName] = restore.CollFilter{AllowAll: true}
			continue
		}

		collTaskFilter[dbName] = restore.CollFilter{CollName: make(map[string]struct{}, len(filter.GetColls()))}
		for _, coll := range filter.GetColls() {
			collTaskFilter[dbName].CollName[coll] = struct{}{}
		}
	}
	return collTaskFilter
}

func newCollTaskFilterFromDBCollections(dbCollections string) (map[string]restore.CollFilter, error) {
	dbColl := meta.DbCollections{}
	if err := json.Unmarshal([]byte(dbCollections), &dbColl); err != nil {
		return nil, fmt.Errorf("restore: unmarshal dbCollections: %w", err)
	}

	collTaskFilter := make(map[string]restore.CollFilter, len(dbColl))
	for dbName, colls := range dbColl {
		if dbName == "" {
			dbName = namespace.DefaultDBName
		}

		if len(colls) == 0 {
			collTaskFilter[dbName] = restore.CollFilter{AllowAll: true}
		} else {
			filter := restore.CollFilter{CollName: make(map[string]struct{}, len(colls))}
			for _, coll := range colls {
				filter.CollName[coll] = struct{}{}
			}
			collTaskFilter[dbName] = filter
		}
	}

	return collTaskFilter, nil
}

func newCollTaskFilter(request *backuppb.RestoreBackupRequest) (map[string]restore.CollFilter, error) {
	// from restore plan
	if request.GetRestorePlan() != nil {
		return newCollTaskFilterFromPlan(request.GetRestorePlan()), nil
	}

	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollectionsAfterRename())
	if dbCollectionsStr != "" {
		return newCollTaskFilterFromDBCollections(dbCollectionsStr)
	}

	return nil, nil
}
