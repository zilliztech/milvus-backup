package server

import (
	"context"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

// getRestoreUC is the slice of app.GetRestore the handler needs. The consumer
// defines it: app returns concrete types, and this narrow interface is what
// handler tests stub out.
type getRestoreUC interface {
	Execute(ctx context.Context, id string) (jobstate.RestoreTaskView, error)
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
func (s *Server) handleGetRestore(c *echo.Context) error {
	requestID := c.Request().Header.Get("request_id")
	if requestID == "" {
		requestID = uuid.NewString()
	}
	id := c.QueryParam("id")
	log.Info("receive GetRestoreStateRequest", zap.String("id", id))

	resp := &backuppb.RestoreBackupResponse{RequestId: requestID}

	if id == "" {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "empty restore id"
		return writeResponse(c, "get restore fail", resp)
	}

	uc, err := s.config.newGetRestore()
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return writeResponse(c, "get restore fail", resp)
	}

	view, err := uc.Execute(c.Request().Context(), id)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return writeResponse(c, "get restore fail", resp)
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.RestoreTaskViewToResp(view)
	log.Info("End to GetRestoreStateRequest", zap.Any("resp", resp))
	return writeResponse(c, "get restore fail", resp)
}
