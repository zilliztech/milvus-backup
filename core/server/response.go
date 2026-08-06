package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// codeMsg is the common shape of the backup service responses that carry a
// response code, a message and the request id that produced them.
type codeMsg interface {
	GetRequestId() string
	GetCode() backuppb.ResponseCode
	GetMsg() string
}

// writeResponse logs an error when the response carries a failure code, then
// writes the response back to the client.
func writeResponse(c *gin.Context, op string, resp codeMsg) {
	if resp.GetCode() != backuppb.ResponseCode_Success {
		log.Error(op,
			zap.String("request_id", resp.GetRequestId()),
			zap.String("code", resp.GetCode().String()),
			zap.String("msg", resp.GetMsg()))
	}
	c.JSON(http.StatusOK, resp)
}
