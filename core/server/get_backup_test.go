package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// stubGetBackup stands in for app.GetBackup: a canned view, the request it
// was called with, and a call count so tests can assert whether the handler
// reached the action at all.
type stubGetBackup struct {
	req        app.GetBackupRequest
	view       *app.BackupView
	executeErr error
	calls      int
}

func (s *stubGetBackup) Execute(_ context.Context, req app.GetBackupRequest) (*app.BackupView, error) {
	s.req = req
	s.calls++
	return s.view, s.executeErr
}

// withGetBackup wires the stub as the get usecase. newErr simulates the
// client-construction failure, which happens before any Execute call.
func withGetBackup(stub *stubGetBackup, newErr error) Option {
	return func(c *config) {
		c.newGetBackup = func(context.Context, *v2.Config) (getBackupUC, error) {
			return stub, newErr
		}
	}
}

func getBackup(t *testing.T, s *Server, query, requestID string) backuppb.BackupInfoResponse {
	t.Helper()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/get_backup"+query, nil)
	if requestID != "" {
		req.Header.Set("request_id", requestID)
	}
	s.engine.ServeHTTP(w, req)

	var resp backuppb.BackupInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	return resp
}

func TestHandleGetBackup(t *testing.T) {
	t.Run("RendersTheMergedView", func(t *testing.T) {
		task := taskmgr.NewMockBackupTaskView(t)
		task.EXPECT().Name().Return("backup1")
		task.EXPECT().ID().Return("task-1")
		task.EXPECT().StateCode().Return(backuppb.BackupTaskStateCode_BACKUP_SUCCESS)
		task.EXPECT().ErrorMessage().Return("")
		task.EXPECT().StartTime().Return(time.Now())
		task.EXPECT().EndTime().Return(time.Now())
		task.EXPECT().Progress().Return(int32(100))

		stub := &stubGetBackup{view: &app.BackupView{
			Task:     task,
			Meta:     &backuppb.BackupInfo{Name: "backup1", Size: 100},
			MetaSize: 42,
		}}
		s := newListTestServer(t, withGetBackup(stub, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
		brief := resp.GetData()
		require.NotNil(t, brief)
		assert.Equal(t, "backup1", brief.GetName())
		assert.Equal(t, int64(100), brief.GetSize())
		assert.Equal(t, int64(42), brief.GetMetaSize())
		assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_SUCCESS, brief.GetStateCode())
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("RejectsMissingNameAndIDWithoutCallingUsecase", func(t *testing.T) {
		stub := &stubGetBackup{}
		s := newListTestServer(t, withGetBackup(stub, nil))

		resp := getBackup(t, s, "", "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "empty backup name and backup id")
		assert.Zero(t, stub.calls)
	})

	t.Run("PassesNameIDAndPathThrough", func(t *testing.T) {
		stub := &stubGetBackup{view: &app.BackupView{
			Meta: &backuppb.BackupInfo{Name: "backup1"},
		}}
		s := newListTestServer(t, withGetBackup(stub, nil))

		resp := getBackup(t, s, "?backup_name=backup1&backup_id=task-1&path=other", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "backup1", stub.req.Name)
		assert.Equal(t, "task-1", stub.req.ID)
		assert.Equal(t, "other", stub.req.Path)
	})

	t.Run("GeneratesRequestIdWhenMissing", func(t *testing.T) {
		s := newListTestServer(t, withGetBackup(&stubGetBackup{view: &app.BackupView{
			Meta: &backuppb.BackupInfo{Name: "backup1"},
		}}, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.NotEmpty(t, resp.GetRequestId())
	})

	t.Run("ForwardsRequestId", func(t *testing.T) {
		s := newListTestServer(t, withGetBackup(&stubGetBackup{view: &app.BackupView{
			Meta: &backuppb.BackupInfo{Name: "backup1"},
		}}, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "rid-1")

		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("MapsConstructorErrorToFail", func(t *testing.T) {
		s := newListTestServer(t, withGetBackup(&stubGetBackup{}, errors.New("dial timeout")))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
	})

	t.Run("MapsExecuteErrorToFail", func(t *testing.T) {
		stub := &stubGetBackup{executeErr: errors.New("backup not found")}
		s := newListTestServer(t, withGetBackup(stub, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "backup not found")
		assert.Equal(t, 1, stub.calls)
	})
}
