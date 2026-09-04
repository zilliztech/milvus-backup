package server

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

// getBackupUC is the slice of app.GetBackup the handler needs. The consumer
// defines it: app returns concrete types, and this narrow interface is what
// handler tests stub out.
type getBackupUC interface {
	Execute(ctx context.Context, req app.GetBackupRequest) (*app.BackupView, error)
}

// Get backup Get backup interface
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
	requestID := c.GetHeader("request_id")
	if requestID == "" {
		requestID = uuid.NewString()
	}

	req := app.GetBackupRequest{
		Name: c.Query("backup_name"),
		ID:   c.Query("backup_id"),
		Path: c.Query("path"),
	}
	log.Info("receive get backup request", zap.Any("request", req))

	resp := &backuppb.BackupInfoResponse{RequestId: requestID}
	if req.Name == "" && req.ID == "" {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "server: empty backup name and backup id, please set a backup name or id"
		writeResponse(c, "get backup fail", resp)
		return
	}

	uc, err := s.config.newGetBackup(c.Request.Context(), s.params)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "get backup fail", resp)
		return
	}

	view, err := uc.Execute(c.Request.Context(), req)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "get backup fail", resp)
		return
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.NewBackupInfoBrief(view.Task, view.Meta, view.MetaSize)

	log.Info("response get backup response", zap.Any("resp", resp))
	writeResponse(c, "get backup fail", resp)
}
