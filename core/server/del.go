package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/zilliztech/milvus-backup/core/del"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// DeleteBackup Delete backup interface
// @Summary Delete backup interface
// @Description Delete a backup with the given name
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param backup_name query string true "backup_name"
// @Success 200 {object} backuppb.DeleteBackupResponse
// @Router /delete [delete]
func (s *Server) handleDeleteBackup(c *gin.Context) (interface{}, error) {
	req := &backuppb.DeleteBackupRequest{
		RequestId:  c.GetHeader("request_id"),
		BackupName: c.Query("backup_name"),
	}

	h := newDelHandler(req, s.params)
	resp := h.run(c.Request.Context())

	c.JSON(http.StatusOK, resp)

	return nil, nil
}

type delHandler struct {
	params *paramtable.BackupParams

	req *backuppb.DeleteBackupRequest

	backupStorage storage.Client
}

func newDelHandler(req *backuppb.DeleteBackupRequest, params *paramtable.BackupParams) *delHandler {
	return &delHandler{
		req:    req,
		params: params,
	}
}

func (h *delHandler) complete() {
	if len(h.req.GetRequestId()) == 0 {
		h.req.RequestId = uuid.NewString()
	}
}

func (h *delHandler) validate() error {
	if len(h.req.GetBackupName()) == 0 {
		return errors.New("backup name is required")
	}

	return nil
}

func (h *delHandler) initClient(ctx context.Context) error {
	backupStorage, err := storage.NewBackupStorage(ctx, &h.params.MinioCfg)
	if err != nil {
		return fmt.Errorf("delete: create backup storage: %w", err)
	}

	h.backupStorage = backupStorage

	return nil
}

func (h *delHandler) run(ctx context.Context) *backuppb.DeleteBackupResponse {
	h.complete()

	resp := &backuppb.DeleteBackupResponse{RequestId: h.req.GetRequestId()}
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

	task := del.NewTask(h.backupStorage, mpath.BackupDir(h.params.MinioCfg.BackupRootPath, h.req.GetBackupName()))
	if err := task.Execute(ctx); err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"

	return resp
}
