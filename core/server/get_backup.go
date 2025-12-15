package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// RestoreBackup Get backup interface
// @Summary Get backup interface
// @Description Get the backup with the given name or id
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param backup_name query string false "backup_name"
// @Param backup_id query string false "backup_id"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /get_backup [get]
func (s *Server) handleGetBackup(c *gin.Context) {
	request := &backuppb.GetBackupRequest{
		RequestId:  c.GetHeader("request_id"),
		BackupName: c.Query("backup_name"),
		BackupId:   c.Query("backup_id"),
		Path:       c.Query("path"),
	}

	log.Info("receive get backup request", zap.Any("request", request))

	h := newGetBackupHandler(request, s.params)
	resp := h.get(c.Request.Context())

	log.Info("response get backup response", zap.Any("resp", resp))
	c.JSON(http.StatusOK, resp)
}

type getBackupHandler struct {
	request *backuppb.GetBackupRequest

	taskMgr *taskmgr.Mgr

	params *paramtable.BackupParams

	backupStorage storage.Client
}

func newGetBackupHandler(request *backuppb.GetBackupRequest, params *paramtable.BackupParams) *getBackupHandler {
	return &getBackupHandler{request: request, params: params, taskMgr: taskmgr.DefaultMgr}
}

func (h *getBackupHandler) complete() {
	if h.request.GetRequestId() == "" {
		h.request.RequestId = uuid.NewString()
	}
}

func (h *getBackupHandler) validate() error {
	if h.request.GetBackupId() == "" && h.request.GetBackupName() == "" {
		return fmt.Errorf("server: empty backup name and backup id, please set a backup name or id")
	}

	return nil
}

func (h *getBackupHandler) initClient(ctx context.Context) error {
	cli, err := storage.NewBackupStorage(ctx, &h.params.MinioCfg)
	if err != nil {
		return fmt.Errorf("server: init backup storage client %w", err)
	}
	h.backupStorage = cli

	return nil
}

func (h *getBackupHandler) getByName(ctx context.Context, backupName string) (*backuppb.BackupInfoBrief, error) {
	// get task view from task manager
	task, err := h.taskMgr.GetBackupTaskByName(backupName)
	if err != nil && !errors.Is(err, taskmgr.ErrTaskNotFound) {
		return nil, fmt.Errorf("server: get backup task %w", err)
	}

	var backup *backuppb.BackupInfo
	var metaSize int64

	if task != nil && task.StateCode() == backuppb.BackupTaskStateCode_BACKUP_SUCCESS {
		// get backup meta from storage
		backup, metaSize, err = h.readFromStorage(ctx, backupName)
		if err != nil {
			return nil, fmt.Errorf("server: get backup by name %w", err)
		}
	}

	return pbconv.NewBackupInfoBrief(task, backup, metaSize), nil
}

func (h *getBackupHandler) get(ctx context.Context) *backuppb.BackupInfoResponse {
	resp := &backuppb.BackupInfoResponse{RequestId: h.request.GetRequestId()}
	if err := h.validate(); err != nil {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = err.Error()
		return resp
	}

	h.complete()

	if err := h.initClient(ctx); err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	brief, err := h.getBrief(ctx)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Data = brief
	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	return resp
}

func (h *getBackupHandler) readFromStorage(ctx context.Context, backupName string) (*backuppb.BackupInfo, int64, error) {
	// get backup meta from storage
	backupRootPath := h.params.MinioCfg.BackupRootPath
	if h.request.GetPath() != "" {
		backupRootPath = h.request.GetPath()
	}

	backupDir := mpath.BackupDir(backupRootPath, backupName)
	exist, err := meta.Exist(ctx, h.backupStorage, backupDir)
	if err != nil {
		return nil, 0, fmt.Errorf("server: check backup exist %w", err)
	}
	if !exist {
		return nil, 0, fmt.Errorf("server: backup %s not found", backupName)
	}

	backup, err := meta.Read(ctx, h.backupStorage, backupDir)
	if err != nil {
		return nil, 0, fmt.Errorf("server: read backup meta %w", err)
	}

	metaSize, err := storage.Size(ctx, h.backupStorage, mpath.MetaDir(backupDir))
	if err != nil {
		return nil, 0, fmt.Errorf("server: get meta size %w", err)
	}

	return backup, metaSize, nil
}

func (h *getBackupHandler) getByID(ctx context.Context, backupID string) (*backuppb.BackupInfoBrief, error) {
	// get task view from task manager
	task, err := h.taskMgr.GetBackupTask(backupID)
	if err != nil {
		return nil, fmt.Errorf("server: get backup task %w", err)
	}

	var backup *backuppb.BackupInfo
	var metaSize int64
	if task.StateCode() == backuppb.BackupTaskStateCode_BACKUP_SUCCESS {
		// get backup meta from storage
		backupName := task.Name()
		backup, metaSize, err = h.readFromStorage(ctx, backupName)
		if err != nil {
			return nil, fmt.Errorf("server: read backup meta %w", err)
		}
	}

	return pbconv.NewBackupInfoBrief(task, backup, metaSize), nil
}

func (h *getBackupHandler) getBrief(ctx context.Context) (*backuppb.BackupInfoBrief, error) {
	if h.request.GetBackupId() != "" {
		brief, err := h.getByID(ctx, h.request.GetBackupId())
		if err != nil {
			return nil, fmt.Errorf("server: get backup by id %w", err)
		}
		return brief, nil
	}

	brief, err := h.getByName(ctx, h.request.GetBackupName())
	if err != nil {
		return nil, fmt.Errorf("server: get backup by name %w", err)
	}
	return brief, nil
}
