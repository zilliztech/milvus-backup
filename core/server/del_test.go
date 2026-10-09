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

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// stubDeleteJob stands in for app.DeleteJob: Run does nothing and Wait
// serves the canned status, because the handler answers from what Wait
// returns, not from Run.
type stubDeleteJob struct {
	status jobstate.DeleteStatus
}

func (s *stubDeleteJob) Run(context.Context) {}

func (s *stubDeleteJob) Wait(context.Context) (jobstate.DeleteStatus, error) {
	return s.status, nil
}

// withDeleteJob wires the delete seam: newErr simulates a registration
// failure (unreadable meta, duplicate name), status is what the job settles
// on and the handler should answer with.
func withDeleteJob(newErr error, status jobstate.DeleteStatus) Option {
	return func(c *config) {
		c.newDeleteJob = func(context.Context, *cfg.Config, app.DeleteBackupRequest) (deleteJob, error) {
			return &stubDeleteJob{status: status}, newErr
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
	successStatus := jobstate.DeleteStatus{State: jobstate.DeleteStateSuccess}

	t.Run("AnswersSuccessWhenJobSettles", func(t *testing.T) {
		s := newListTestServer(t, withDeleteJob(nil, successStatus))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
	})

	t.Run("RejectsMissingNameWithoutRegisteringJob", func(t *testing.T) {
		s := newListTestServer(t, withDeleteJob(errors.New("must not be built"), successStatus))

		resp := delBackup(t, s, "", "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "backup name is required")
	})

	t.Run("GeneratesRequestIdWhenMissing", func(t *testing.T) {
		s := newListTestServer(t, withDeleteJob(nil, successStatus))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.NotEmpty(t, resp.GetRequestId())
	})

	t.Run("ForwardsRequestId", func(t *testing.T) {
		s := newListTestServer(t, withDeleteJob(nil, successStatus))

		resp := delBackup(t, s, "?backup_name=backup1", "rid-1")

		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("MapsRegistrationErrorToFail", func(t *testing.T) {
		s := newListTestServer(t, withDeleteJob(errors.New("meta unreadable"), successStatus))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "meta unreadable")
	})

	t.Run("AnswersWithTheFailedStatusTheJobSettledOn", func(t *testing.T) {
		failStatus := jobstate.DeleteStatus{
			State:        jobstate.DeleteStateFail,
			ErrorMessage: "connection closed",
		}
		s := newListTestServer(t, withDeleteJob(nil, failStatus))

		resp := delBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "connection closed")
	})
}
