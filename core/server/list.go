package server

import (
	"github.com/gin-gonic/gin"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// ListBackups List Backups interface
// @Summary List Backups interface
// @Description List all backups in current storage
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param collection_name query string false "collection_name"
// @Success 200 {object} backuppb.ListBackupsResponse
// @Router /list [get]
func (s *Server) handleListBackups(c *gin.Context) {
	req := backuppb.ListBackupsRequest{
		RequestId:      c.GetHeader("request_id"),
		CollectionName: c.Query("collection_name"),
	}

	resp := &backuppb.ListBackupsResponse{RequestId: req.GetRequestId()}
	if len(req.GetCollectionName()) > 0 {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "collection_name is deprecated"
		c.JSON(200, resp)
		return
	}

	backupStorage, err := storage.NewBackupStorage(c.Request.Context(), &s.params.Minio)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		c.JSON(200, resp)
		return
	}

	summaries, err := meta.List(c.Request.Context(), backupStorage, s.params.Minio.BackupRootPath.Val)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		c.JSON(200, resp)
		return
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Data = summaries
	c.JSON(200, resp)
}
