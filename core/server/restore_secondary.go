package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/secondary"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// RestoreBackup Restore interface
// @Summary Restore interface
// @Description Submit a request to restore the data from backup
// @Tags Restore
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body backuppb.RestoreSecondaryRequest   true  "RestoreBackupRequest JSON"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /restore_secondary [post]
func (s *Server) handleRestoreSecondary(c *gin.Context) {
	var request backuppb.RestoreSecondaryRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request body: %s", err)})
		return
	}

	log.Info("receive restore secondary request", zap.Any("request", &request))

	h := newRestoreSecondaryHandler(&request, s.params)
	resp := h.run(c.Request.Context())

	c.JSON(http.StatusOK, resp)
}

func newRestoreSecondaryHandler(request *backuppb.RestoreSecondaryRequest, params *cfg.Config) *restoreSecondaryHandler {
	return &restoreSecondaryHandler{request: request, params: params}
}

type restoreSecondaryHandler struct {
	params  *cfg.Config
	request *backuppb.RestoreSecondaryRequest

	backupStorage  storage.Client
	backupRootPath string
}

func (h *restoreSecondaryHandler) validate() error {
	if len(h.request.GetBackupName()) == 0 {
		return errors.New("backup name is required")
	}

	if len(h.request.GetSourceClusterID()) == 0 {
		return errors.New("source cluster id is required")
	}

	if len(h.request.GetTargetClusterID()) == 0 {
		return errors.New("target cluster id is required")
	}

	return nil
}

func (h *restoreSecondaryHandler) complete() {
	if len(h.request.GetRequestId()) == 0 {
		h.request.RequestId = uuid.NewString()
	}
}

func (h *restoreSecondaryHandler) run(ctx context.Context) *backuppb.RestoreBackupResponse {
	h.complete()

	resp := &backuppb.RestoreBackupResponse{RequestId: h.request.GetRequestId()}
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

	backupDir := mpath.BackupDir(h.backupRootPath, h.request.GetBackupName())
	exist, err := meta.Exist(ctx, h.backupStorage, backupDir)
	if err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}
	if !exist {
		msg := fmt.Sprintf("backup %s not found", h.request.GetBackupName())
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Parameter_Error, Msg: msg}
	}

	task, err := h.newTask(ctx)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	if h.request.GetAsync() {
		return h.runAsync(task)
	}

	return h.runSync(ctx, task)
}

func (h *restoreSecondaryHandler) initClient(ctx context.Context) error {
	backupStorage, err := storage.NewBackupStorage(ctx, &h.params.Minio)
	if err != nil {
		return fmt.Errorf("server: create backup storage: %w", err)
	}

	backupRootPath := h.params.Minio.BackupRootPath.Val
	if len(h.request.GetPath()) != 0 {
		backupRootPath = h.request.GetPath()
	}

	h.backupStorage = backupStorage
	h.backupRootPath = backupRootPath
	return nil
}

func (h *restoreSecondaryHandler) newTask(ctx context.Context) (tasklet.Tasklet, error) {
	backup, err := meta.Read(ctx, h.backupStorage, mpath.BackupDir(h.backupRootPath, h.request.GetBackupName()))
	if err != nil {
		return nil, fmt.Errorf("server: read backup: %w", err)
	}

	args := secondary.TaskArgs{
		TaskID: h.request.GetRequestId(),

		SourceClusterID: h.request.GetSourceClusterID(),
		TargetClusterID: h.request.GetTargetClusterID(),

		Backup:        backup,
		Params:        h.params,
		BackupDir:     mpath.BackupDir(h.backupRootPath, h.request.GetBackupName()),
		BackupStorage: h.backupStorage,

		TaskMgr: taskmgr.DefaultMgr(),
	}

	task, err := secondary.NewTask(args)
	if err != nil {
		return nil, fmt.Errorf("backup: new restore task: %w", err)
	}

	return task, nil
}

func (h *restoreSecondaryHandler) runAsync(task tasklet.Tasklet) *backuppb.RestoreBackupResponse {
	resp := &backuppb.RestoreBackupResponse{RequestId: h.request.GetRequestId()}

	go func() {
		if err := task.Execute(context.Background()); err != nil {
			log.Error("restore backup task execute fail", zap.String("request_id", h.request.GetRequestId()), zap.Error(err))
		}
	}()

	taskView, err := taskmgr.DefaultMgr().GetRestoreTask(h.request.GetRequestId())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "restore backup is executing asynchronously"
	resp.Data = pbconv.RestoreTaskViewToResp(taskView)
	return resp
}

func (h *restoreSecondaryHandler) runSync(ctx context.Context, task tasklet.Tasklet) *backuppb.RestoreBackupResponse {
	resp := &backuppb.RestoreBackupResponse{RequestId: h.request.GetRequestId()}

	if err := task.Execute(ctx); err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	taskView, err := taskmgr.DefaultMgr().GetRestoreTask(h.request.GetRequestId())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.RestoreTaskViewToResp(taskView)
	return resp
}
