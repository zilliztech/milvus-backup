package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// stubGetRestore stands in for app.GetRestore: a canned view, the id it was
// called with, and a call count so tests can assert whether the handler
// reached the action at all.
type stubGetRestore struct {
	id         string
	view       app.RestoreView
	executeErr error
	calls      int
}

func (s *stubGetRestore) Execute(_ context.Context, id string) (app.RestoreView, error) {
	s.id = id
	s.calls++
	return s.view, s.executeErr
}

// withGetRestore wires the stub as the get-restore usecase. newErr simulates
// the construction failure, which happens before any Execute call.
func withGetRestore(stub *stubGetRestore, newErr error) Option {
	return func(c *config) {
		c.newGetRestore = func() (getRestoreUC, error) {
			return stub, newErr
		}
	}
}

func getRestore(t *testing.T, s *Server, query, requestID string) backuppb.RestoreBackupResponse {
	t.Helper()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/get_restore"+query, nil)
	if requestID != "" {
		req.Header.Set("request_id", requestID)
	}
	s.engine.ServeHTTP(w, req)

	var resp backuppb.RestoreBackupResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	return resp
}

// newRestoreTaskView returns a mock task view answering every read the v1
// rendering makes.
func newRestoreTaskView(t *testing.T) *taskmgr.MockRestoreTaskView {
	t.Helper()

	now := time.Now()
	task := taskmgr.NewMockRestoreTaskView(t)
	task.EXPECT().ID().Return("task-1")
	task.EXPECT().StateCode().Return(backuppb.RestoreTaskStateCode_EXECUTING)
	task.EXPECT().ErrorMessage().Return("coll1 fail")
	task.EXPECT().StartTime().Return(now)
	task.EXPECT().EndTime().Return(now)
	task.EXPECT().Progress().Return(int32(42))
	task.EXPECT().CollTasks().Return(nil)

	return task
}

func TestHandleGetRestore(t *testing.T) {
	t.Run("RendersTheTaskView", func(t *testing.T) {
		stub := &stubGetRestore{view: app.RestoreView{Task: newRestoreTaskView(t)}}
		s := newListTestServer(t, withGetRestore(stub, nil))

		resp := getRestore(t, s, "?id=task-1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
		data := resp.GetData()
		require.NotNil(t, data)
		assert.Equal(t, "task-1", data.GetId())
		assert.Equal(t, backuppb.RestoreTaskStateCode_EXECUTING, data.GetStateCode())
		assert.Equal(t, "coll1 fail", data.GetErrorMessage())
		assert.Equal(t, int32(42), data.GetProgress())
		assert.Equal(t, 1, stub.calls)
		assert.Equal(t, "task-1", stub.id)
	})

	t.Run("RejectsEmptyIDWithoutCallingUsecase", func(t *testing.T) {
		stub := &stubGetRestore{}
		s := newListTestServer(t, withGetRestore(stub, nil))

		resp := getRestore(t, s, "", "")

		// The historical contract answers Fail here, not Parameter_Error.
		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Equal(t, "empty restore id", resp.GetMsg())
		assert.Zero(t, stub.calls)
	})

	t.Run("GeneratesRequestIdWhenMissing", func(t *testing.T) {
		stub := &stubGetRestore{view: app.RestoreView{Task: newRestoreTaskView(t)}}
		s := newListTestServer(t, withGetRestore(stub, nil))

		resp := getRestore(t, s, "?id=task-1", "")

		assert.NotEmpty(t, resp.GetRequestId())
	})

	t.Run("ForwardsRequestId", func(t *testing.T) {
		stub := &stubGetRestore{view: app.RestoreView{Task: newRestoreTaskView(t)}}
		s := newListTestServer(t, withGetRestore(stub, nil))

		resp := getRestore(t, s, "?id=task-1", "rid-1")

		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("MapsConstructorErrorToFail", func(t *testing.T) {
		s := newListTestServer(t, withGetRestore(&stubGetRestore{}, errors.New("dial timeout")))

		resp := getRestore(t, s, "?id=task-1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
	})

	t.Run("MapsExecuteErrorToFail", func(t *testing.T) {
		stub := &stubGetRestore{
			executeErr: fmt.Errorf("app: get restore task task-1: %w", taskmgr.ErrTaskNotFound),
		}
		s := newListTestServer(t, withGetRestore(stub, nil))

		resp := getRestore(t, s, "?id=task-1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "task not found")
		assert.Equal(t, 1, stub.calls)
	})
}
