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

// getRestoreUC is the slice of app.GetRestore the handler needs. The consumer
// defines it: app returns concrete types, and this narrow interface is what
// handler tests stub out.
type getRestoreUC interface {
	Execute(ctx context.Context, id string) (app.RestoreView, error)
}

// GetRestore Get restore interface
// @Summary Get restore interface
// @Description Get restore task state with the given id
// @Tags Restore
// @Produce application/json
// @Param request_id header string false "request_id"
// @param id query string true "id"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /get_restore [get]
func (s *Server) handleGetRestore(c *gin.Context) {
	requestID := c.GetHeader("request_id")
	if requestID == "" {
		requestID = uuid.NewString()
	}
	id := c.Query("id")
	log.Info("receive GetRestoreStateRequest", zap.String("id", id))

	resp := &backuppb.RestoreBackupResponse{RequestId: requestID}

	if id == "" {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "empty restore id"
		writeResponse(c, "get restore fail", resp)
		return
	}

	uc, err := s.config.newGetRestore()
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "get restore fail", resp)
		return
	}

	view, err := uc.Execute(c.Request.Context(), id)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "get restore fail", resp)
		return
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.RestoreTaskViewToResp(view.Task)
	log.Info("End to GetRestoreStateRequest", zap.Any("resp", resp))
	writeResponse(c, "get restore fail", resp)
}
