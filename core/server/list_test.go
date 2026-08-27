package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// stubListBackups stands in for app.ListBackups: canned data or errors, and a
// call count so tests can assert whether the handler reached the action at
// all.
type stubListBackups struct {
	summaries  []app.BackupSummary
	executeErr error
	calls      int
}

func (s *stubListBackups) Execute(_ context.Context) ([]app.BackupSummary, error) {
	s.calls++
	return s.summaries, s.executeErr
}

// withListBackups wires the stub as the list usecase. newErr simulates the
// client-construction failure, which happens before any Execute call.
func withListBackups(stub *stubListBackups, newErr error) Option {
	return func(c *config) {
		c.newListBackups = func(context.Context, *v2.Config) (listBackupsUC, error) {
			return stub, newErr
		}
	}
}

func newListTestServer(t *testing.T, opts ...Option) *Server {
	t.Helper()

	s, err := New(v2.New(), opts...)
	require.NoError(t, err)
	gin.SetMode(gin.TestMode)

	return s
}

func getList(t *testing.T, s *Server, query string) backuppb.ListBackupsResponse {
	t.Helper()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/list"+query, nil)
	s.engine.ServeHTTP(w, req)

	var resp backuppb.ListBackupsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	return resp
}

func TestHandleListBackups(t *testing.T) {
	t.Run("RendersSummaries", func(t *testing.T) {
		stub := &stubListBackups{summaries: []app.BackupSummary{
			{ID: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"},
			{ID: "b", Name: "backup2", Size: 200, MilvusVersion: "2.0.0"},
		}}
		s := newListTestServer(t, withListBackups(stub, nil))

		resp := getList(t, s, "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		expected := []*backuppb.BackupSummary{
			{Id: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"},
			{Id: "b", Name: "backup2", Size: 200, MilvusVersion: "2.0.0"},
		}
		assert.Equal(t, expected, resp.GetData())
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("ForwardsRequestId", func(t *testing.T) {
		s := newListTestServer(t, withListBackups(&stubListBackups{}, nil))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/list", nil)
		req.Header.Set("request_id", "rid-1")
		s.engine.ServeHTTP(w, req)

		var resp backuppb.ListBackupsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("RejectsDeprecatedCollectionName", func(t *testing.T) {
		stub := &stubListBackups{}
		s := newListTestServer(t, withListBackups(stub, nil))

		resp := getList(t, s, "?collection_name=books")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "deprecated")
		assert.Zero(t, stub.calls)
	})

	t.Run("MapsConstructorErrorToFail", func(t *testing.T) {
		s := newListTestServer(t, withListBackups(&stubListBackups{}, errors.New("dial timeout")))

		resp := getList(t, s, "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
	})

	t.Run("MapsExecuteErrorToFail", func(t *testing.T) {
		stub := &stubListBackups{executeErr: errors.New("root unreadable")}
		s := newListTestServer(t, withListBackups(stub, nil))

		resp := getList(t, s, "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "root unreadable")
		assert.Equal(t, 1, stub.calls)
	})
}
