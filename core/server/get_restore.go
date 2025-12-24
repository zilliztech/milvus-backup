package server

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

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
	req := &backuppb.GetRestoreStateRequest{RequestId: uuid.NewString()}

	req.RequestId = c.GetHeader("request_id")
	req.Id = c.Query("id")
	log.Info("receive GetRestoreStateRequest", zap.Any("request", req))

	resp := &backuppb.RestoreBackupResponse{RequestId: req.GetRequestId()}

	if req.GetId() == "" {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "empty restore id"
		c.JSON(http.StatusOK, resp)
		return
	}

	taskView, err := taskmgr.DefaultMgr().GetRestoreTask(req.GetId())
	if err != nil && !errors.Is(err, taskmgr.ErrTaskNotFound) {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "restore id not exist in task manager"
		c.JSON(http.StatusOK, resp)
		return
	}
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		c.JSON(http.StatusOK, resp)
		return
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.RestoreTaskViewToResp(taskView)
	log.Info("End to GetRestoreStateRequest", zap.Any("resp", resp))
	c.JSON(http.StatusOK, resp)
}
