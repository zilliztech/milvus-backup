package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// stubDeleteBackup stands in for app.DeleteBackup: a canned error, the name
// it was called with, and a call count so tests can assert whether the
// handler reached the action at all.
type stubDeleteBackup struct {
	name       string
	executeErr error
	calls      int
}

func (s *stubDeleteBackup) Execute(_ context.Context, name string) error {
	s.name = name
	s.calls++
	return s.executeErr
}

// withDeleteBackup wires the stub as the delete usecase. newErr simulates the
// client-construction failure, which happens before any Execute call.
func withDeleteBackup(stub *stubDeleteBackup, newErr error) Option {
	return func(c *config) {
		c.newDeleteBackup = func(context.Context, *v2.Config) (deleteBackupUC, error) {
			return stub, newErr
		}
	}
}

func delBackup(t *testing.T, s *Server, query string, requestID string) backuppb.DeleteBackupResponse {
	t.Helper()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/delete"+query, nil)
	if requestID != "" {
		req.Header.Set("request_id", requestID)
	}
	s.engine.ServeHTTP(w, req)

	var resp backuppb.DeleteBackupResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	return resp
}

func TestHandleDeleteBackup(t *testing.T) {
	t.Run("DeletesThroughTheUsecase", func(t *testing.T) {
		stub := &stubDeleteBackup{}
		s := newListTestServer(t, withDeleteBackup(stub, nil))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
		assert.Equal(t, "backup1", stub.name)
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("RejectsMissingNameWithoutCallingUsecase", func(t *testing.T) {
		stub := &stubDeleteBackup{}
		s := newListTestServer(t, withDeleteBackup(stub, nil))

		resp := delBackup(t, s, "", "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "backup name is required")
		assert.Zero(t, stub.calls)
	})

	t.Run("GeneratesRequestIdWhenMissing", func(t *testing.T) {
		s := newListTestServer(t, withDeleteBackup(&stubDeleteBackup{}, nil))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.NotEmpty(t, resp.GetRequestId())
	})

	t.Run("ForwardsRequestId", func(t *testing.T) {
		s := newListTestServer(t, withDeleteBackup(&stubDeleteBackup{}, nil))

		resp := delBackup(t, s, "?backup_name=backup1", "rid-1")

		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("MapsConstructorErrorToFail", func(t *testing.T) {
		s := newListTestServer(t, withDeleteBackup(&stubDeleteBackup{}, errors.New("dial timeout")))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
	})

	t.Run("MapsExecuteErrorToFail", func(t *testing.T) {
		stub := &stubDeleteBackup{executeErr: errors.New("meta unreadable")}
		s := newListTestServer(t, withDeleteBackup(stub, nil))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "meta unreadable")
		assert.Equal(t, 1, stub.calls)
	})
}
