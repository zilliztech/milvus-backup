package server

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// deleteBackupUC is the slice of app.DeleteBackup the handler needs. The
// consumer defines it: app returns concrete types, and this narrow interface
// is what handler tests stub out.
type deleteBackupUC interface {
	Execute(ctx context.Context, name string) error
}

// DeleteBackup Delete backup interface
// @Summary Delete backup interface
// @Description Delete a backup with the given name
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param backup_name query string true "backup_name"
// @Success 200 {object} backuppb.DeleteBackupResponse
// @Router /delete [delete]
func (s *Server) handleDeleteBackup(c *gin.Context) {
	req := &backuppb.DeleteBackupRequest{
		RequestId:  c.GetHeader("request_id"),
		BackupName: c.Query("backup_name"),
	}
	if len(req.GetRequestId()) == 0 {
		req.RequestId = uuid.NewString()
	}

	resp := &backuppb.DeleteBackupResponse{RequestId: req.GetRequestId()}
	if len(req.GetBackupName()) == 0 {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "backup name is required"
		writeResponse(c, "delete backup fail", resp)
		return
	}

	uc, err := s.config.newDeleteBackup(c.Request.Context(), s.params)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "delete backup fail", resp)
		return
	}

	if err := uc.Execute(c.Request.Context(), req.GetBackupName()); err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "delete backup fail", resp)
		return
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"

	writeResponse(c, "delete backup fail", resp)
}
