package server

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// codeMsg is the common shape of the backup service responses that carry a
// response code, a message and the request id that produced them.
type codeMsg interface {
	GetRequestId() string
	GetCode() backuppb.ResponseCode
	GetMsg() string
}

// writeResponse writes the response back to the client. It does not log
// failures: the failure a response carries was already logged where it
// settled — the job that recorded it, or the admission check that refused
// it — so logging here too would print the same error twice.
func writeResponse(c *echo.Context, resp codeMsg) error {
	return c.JSON(http.StatusOK, resp)
}
