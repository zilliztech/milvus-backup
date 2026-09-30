package server

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/samber/lo"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// listBackupsUC is the slice of app.ListBackups the handler needs. The
// consumer defines it: app returns concrete types, and this narrow interface
// is what handler tests stub out.
type listBackupsUC interface {
	Execute(ctx context.Context) ([]app.BackupSummary, error)
}

// ListBackups List Backups interface
// @Summary List Backups interface
// @Description List all backups in current storage
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param collection_name query string false "collection_name"
// @Success 200 {object} backuppb.ListBackupsResponse
// @Router /list [get]
func (s *Server) handleListBackups(c *echo.Context) error {
	req := backuppb.ListBackupsRequest{
		RequestId:      c.Request().Header.Get("request_id"),
		CollectionName: c.QueryParam("collection_name"),
	}

	resp := &backuppb.ListBackupsResponse{RequestId: req.GetRequestId()}
	if len(req.GetCollectionName()) > 0 {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "collection_name is deprecated"
		return c.JSON(http.StatusOK, resp)
	}

	uc, err := s.config.newListBackups(c.Request().Context(), s.params)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return c.JSON(http.StatusOK, resp)
	}

	summaries, err := uc.Execute(c.Request().Context())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return c.JSON(http.StatusOK, resp)
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Data = lo.Map(summaries, func(s app.BackupSummary, _ int) *backuppb.BackupSummary {
		return &backuppb.BackupSummary{
			Id:            s.ID,
			Name:          s.Name,
			Size:          s.Size,
			MilvusVersion: s.MilvusVersion,
		}
	})
	return c.JSON(http.StatusOK, resp)
}
