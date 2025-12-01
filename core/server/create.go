package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// CreateBackup Create backup interface
// @Summary Create backup interface
// @Description Create a backup with the given name and collections
// @Tags Backup
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body backuppb.CreateBackupRequest   true  "CreateBackupRequest JSON"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /create [post]
func (s *Server) handleCreateBackup(c *gin.Context) {
	var requestBody backuppb.CreateBackupRequest
	if err := c.ShouldBindJSON(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	requestBody.RequestId = c.GetHeader("request_id")

	log.Info("receive create backup request", zap.Any("request", &requestBody))
	h := newCreateBackupHandler(&requestBody, s.params)
	resp := h.run(c.Request.Context())
	log.Info("response create backup response", zap.Any("resp", resp))

	c.JSON(http.StatusOK, resp)
}

type createBackupHandler struct {
	params *paramtable.BackupParams

	request *backuppb.CreateBackupRequest

	milvusClient  milvus.Grpc
	restfulClient milvus.Restful
	manageClient  milvus.Manage

	backupStorage storage.Client

	milvusStorage storage.Client
}

func newCreateBackupHandler(request *backuppb.CreateBackupRequest, params *paramtable.BackupParams) *createBackupHandler {
	return &createBackupHandler{request: request, params: params}
}

func (h *createBackupHandler) complete() {
	if h.request.GetRequestId() == "" {
		h.request.RequestId = uuid.NewString()
	}
}

func (h *createBackupHandler) initClient(ctx context.Context) error {
	backupStorage, err := storage.NewBackupStorage(ctx, &h.params.MinioCfg)
	if err != nil {
		return err
	}
	h.backupStorage = backupStorage

	milvusStorage, err := storage.NewMilvusStorage(ctx, &h.params.MinioCfg)
	if err != nil {
		return err
	}
	h.milvusStorage = milvusStorage

	milvusClient, err := milvus.NewGrpc(&h.params.MilvusCfg)
	if err != nil {
		return err
	}
	h.milvusClient = milvusClient

	restfulClient, err := milvus.NewRestful(&h.params.MilvusCfg)
	if err != nil {
		return err
	}
	h.restfulClient = restfulClient

	manageAddr := h.params.MilvusCfg.Address
	if h.request.GetGcPauseAddress() != "" {
		manageAddr = h.request.GetGcPauseAddress()
	}

	h.manageClient = milvus.NewManage(manageAddr)
	return nil
}

func (h *createBackupHandler) toFilter() (filter.Filter, error) {
	if h.request.GetFilter() != nil {
		return h.filterToFilter()
	}

	dbCollectionsStr := utils.GetDBCollections(h.request.GetDbCollections())
	if len(dbCollectionsStr) > 0 {
		return h.dbCollectionsToFilter(dbCollectionsStr)
	}

	if len(h.request.GetCollectionNames()) > 0 {
		return h.collectionNamesToFilter()
	}

	return filter.Filter{}, nil
}

func (h *createBackupHandler) dbCollectionsToFilter(dbCollectionsStr string) (filter.Filter, error) {
	var dbCollections map[string][]string
	if err := json.Unmarshal([]byte(dbCollectionsStr), &dbCollections); err != nil {
		return filter.Filter{}, fmt.Errorf("server: unmarshal dbCollections: %w", err)
	}

	dbCollFilter := make(map[string]filter.CollFilter)
	for dbName, colls := range dbCollections {
		if len(colls) == 0 {
			dbCollFilter[dbName] = filter.CollFilter{AllowAll: true}
		} else {
			collName := make(map[string]struct{}, len(colls))
			for _, coll := range colls {
				collName[coll] = struct{}{}
			}
			dbCollFilter[dbName] = filter.CollFilter{CollName: collName}
		}
	}

	return filter.Filter{DBCollFilter: dbCollFilter}, nil
}

func (h *createBackupHandler) collectionNamesToFilter() (filter.Filter, error) {
	dbCollFilter := make(map[string]filter.CollFilter)
	for _, nsStr := range h.request.GetCollectionNames() {
		ns, err := namespace.Parse(nsStr)
		if err != nil {
			return filter.Filter{}, fmt.Errorf("server: invalid collection name %s", nsStr)
		}

		if _, ok := dbCollFilter[ns.DBName()]; !ok {
			dbCollFilter[ns.DBName()] = filter.CollFilter{CollName: make(map[string]struct{})}
		}
		dbCollFilter[ns.DBName()].CollName[ns.CollName()] = struct{}{}
	}

	return filter.Filter{DBCollFilter: dbCollFilter}, nil
}

func (h *createBackupHandler) filterToFilter() (filter.Filter, error) {
	f, err := filter.FromPB(h.request.GetFilter())
	if err != nil {
		return filter.Filter{}, fmt.Errorf("server: build filter from pb: %w", err)
	}

	return f, nil
}

func (h *createBackupHandler) toStrategy() (backup.Strategy, error) {
	if h.request.GetStrategy() != "" {
		return backup.ParseStrategy(h.request.GetStrategy())
	}

	if h.request.GetForce() {
		log.Warn("force option is deprecated, pls use strategy=skip_flush instead")
		return backup.StrategySkipFlush, nil
	}

	if h.request.GetMetaOnly() {
		log.Warn("meta_only option is deprecated, pls use strategy=meta_only instead")
		return backup.StrategyMetaOnly, nil
	}

	return backup.StrategyAuto, nil
}

func (h *createBackupHandler) toOption(params *paramtable.BackupParams) (backup.Option, error) {
	f, err := h.toFilter()
	if err != nil {
		return backup.Option{}, fmt.Errorf("server: build filter: %w", err)
	}

	strategy, err := h.toStrategy()
	if err != nil {
		return backup.Option{}, fmt.Errorf("server: build strategy: %w", err)
	}

	return backup.Option{
		BackupName: h.request.GetBackupName(),
		PauseGC:    h.request.GetGcPauseEnable() || params.BackupCfg.GcPauseEnable,
		Strategy:   strategy,
		BackupRBAC: h.request.GetRbac(),
		BackupEZK:  h.request.GetWithEzk(),
		Filter:     f,
	}, nil
}

func (h *createBackupHandler) toArgs() (backup.TaskArgs, error) {
	option, err := h.toOption(h.params)
	if err != nil {
		return backup.TaskArgs{}, fmt.Errorf("server: build option: %w", err)
	}

	backupRoot := h.params.MinioCfg.BackupRootPath
	if h.request.GetBackupRootPath() != "" {
		backupRoot = h.request.GetBackupRootPath()
		log.Info("use backup root from request", zap.String("backup_root", backupRoot))
	}
	backupDir := mpath.BackupDir(backupRoot, h.request.GetBackupName())

	return backup.TaskArgs{
		TaskID:        h.request.GetRequestId(),
		Option:        option,
		MilvusStorage: h.milvusStorage,
		BackupStorage: h.backupStorage,
		BackupDir:     backupDir,
		Params:        h.params,
		Grpc:          h.milvusClient,
		Restful:       h.restfulClient,
		Manage:        h.manageClient,
		TaskMgr:       taskmgr.DefaultMgr,
	}, nil
}

func (h *createBackupHandler) validate() error {
	if err := backup.ValidateName(h.request.GetBackupName()); err != nil {
		return err
	}

	return nil
}

func (h *createBackupHandler) runSync(ctx context.Context, args backup.TaskArgs) *backuppb.BackupInfoResponse {
	resp := &backuppb.BackupInfoResponse{RequestId: h.request.GetRequestId()}

	task := backup.NewTask(args)
	if err := task.Execute(ctx); err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	taskView, err := taskmgr.DefaultMgr.GetBackupTask(h.request.GetRequestId())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	backupInfo, err := meta.Read(ctx, h.backupStorage, args.BackupDir)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	metaSize, err := storage.Size(ctx, h.backupStorage, mpath.MetaDir(args.BackupDir))
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.NewBackupInfoBrief(taskView, backupInfo, metaSize)

	return &backuppb.BackupInfoResponse{Code: backuppb.ResponseCode_Success, Msg: "success"}
}

func (h *createBackupHandler) runAsync(args backup.TaskArgs) *backuppb.BackupInfoResponse {
	resp := &backuppb.BackupInfoResponse{RequestId: h.request.GetRequestId()}
	task := backup.NewTask(args)

	go func() {
		if err := task.Execute(context.Background()); err != nil {
			log.Error("create backup task execute fail", zap.String("backupId", h.request.GetRequestId()), zap.Error(err))
		}
	}()

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "create backup is executing asynchronously"
	return resp
}

func (h *createBackupHandler) run(ctx context.Context) *backuppb.BackupInfoResponse {
	h.complete()

	resp := &backuppb.BackupInfoResponse{RequestId: h.request.GetRequestId()}
	if err := h.validate(); err != nil {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = err.Error()
		return resp
	}

	if err := h.initClient(ctx); err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	args, err := h.toArgs()
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	if h.request.GetAsync() {
		return h.runAsync(args)
	} else {
		return h.runSync(ctx, args)
	}
}
