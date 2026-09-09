package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// stubGetBackup stands in for app.GetBackup: a canned artifact read, the
// name it was called with, the params its constructor received, and a call
// count so tests can assert whether the handler reached the action at all.
type stubGetBackup struct {
	name   string
	params *v2.Config
	info   *backuppb.BackupInfo
	size   int64
	err    error
	calls  int
}

func (s *stubGetBackup) Execute(_ context.Context, name string) (*backuppb.BackupInfo, int64, error) {
	s.name = name
	s.calls++
	return s.info, s.size, s.err
}

// stubGetBackupTask is the job-half counterpart of stubGetBackup, standing
// in for app.GetBackupTask.
type stubGetBackupTask struct {
	req   app.GetBackupTaskRequest
	view  taskmgr.BackupTaskView
	err   error
	calls int
}

func (s *stubGetBackupTask) Execute(_ context.Context, req app.GetBackupTaskRequest) (taskmgr.BackupTaskView, error) {
	s.req = req
	s.calls++
	return s.view, s.err
}

// withGetBackup wires the stub as the artifact usecase. newErr simulates the
// client-construction failure, which happens before any Execute call.
func withGetBackup(stub *stubGetBackup, newErr error) Option {
	return func(c *config) {
		c.newGetBackup = func(_ context.Context, params *v2.Config) (getBackupUC, error) {
			stub.params = params
			return stub, newErr
		}
	}
}

// withGetBackupTask is the job-half counterpart of withGetBackup.
func withGetBackupTask(stub *stubGetBackupTask, newErr error) Option {
	return func(c *config) {
		c.newGetBackupTask = func() (getBackupTaskUC, error) {
			return stub, newErr
		}
	}
}

// noBackupTask wires the common post-restart case: no job is known for any
// selector.
func noBackupTask() Option {
	return withGetBackupTask(&stubGetBackupTask{err: taskmgr.ErrTaskNotFound}, nil)
}

// newLoadedTestServer is the fork-capable counterpart of newListTestServer:
// the params are Load'ed the way the real server's are, so a request can fork
// them. A hand-built v2.New() has no source behind it, and Fork refuses one.
func newLoadedTestServer(t *testing.T, opts ...Option) *Server {
	t.Helper()

	params, err := v2.Load("", nil)
	require.NoError(t, err)

	s, err := New(params, opts...)
	require.NoError(t, err)
	gin.SetMode(gin.TestMode)

	return s
}

// expectBriefRender teaches the mock job view the calls
// pbconv.NewBackupInfoBrief makes when it renders a task half.
func expectBriefRender(task *taskmgr.MockBackupTaskView, id, name string, state backuppb.BackupTaskStateCode) {
	task.EXPECT().Name().Return(name)
	task.EXPECT().ID().Return(id)
	task.EXPECT().StateCode().Return(state)
	task.EXPECT().ErrorMessage().Return("")
	task.EXPECT().StartTime().Return(time.Now())
	task.EXPECT().EndTime().Return(time.Now())
	task.EXPECT().Progress().Return(int32(100))
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
	t.Run("MergesArtifactAndJob", func(t *testing.T) {
		task := taskmgr.NewMockBackupTaskView(t)
		expectBriefRender(task, "task-1", "backup1", backuppb.BackupTaskStateCode_BACKUP_SUCCESS)

		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1", Size: 100}, size: 42}
		taskStub := &stubGetBackupTask{view: task}
		s := newListTestServer(t, withGetBackup(backup, nil), withGetBackupTask(taskStub, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
		brief := resp.GetData()
		require.NotNil(t, brief)
		assert.Equal(t, "backup1", brief.GetName())
		assert.Equal(t, "task-1", brief.GetId())
		assert.Equal(t, int64(100), brief.GetSize())
		assert.Equal(t, int64(42), brief.GetMetaSize())
		assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_SUCCESS, brief.GetStateCode())

		assert.Equal(t, "backup1", backup.name)
		assert.Equal(t, app.GetBackupTaskRequest{Name: "backup1"}, taskStub.req)
	})

	t.Run("PathParameterForksConfig", func(t *testing.T) {
		// The v1 path parameter is applied as a config override, not sent
		// through the selector-only request.
		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1"}}
		s := newLoadedTestServer(t, withGetBackup(backup, nil), noBackupTask())

		resp := getBackup(t, s, "?backup_name=backup1&path=other", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		require.NotNil(t, backup.params)
		assert.Equal(t, "other", backup.params.Backup.Storage.RootPath.Val)
		// The fork, not the server's own config, reaches the usecase.
		assert.NotSame(t, s.params, backup.params)
		// The server's own config stays on the default root path.
		assert.Equal(t, "backup", s.params.Backup.Storage.RootPath.Val)
	})

	t.Run("ArtifactWithoutJob", func(t *testing.T) {
		// The completed-backup-after-restart case: no job is known anymore,
		// the artifact alone is the answer.
		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1", Size: 100}, size: 42}
		s := newListTestServer(t, withGetBackup(backup, nil), noBackupTask())

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		brief := resp.GetData()
		require.NotNil(t, brief)
		assert.Equal(t, "backup1", brief.GetName())
		assert.Equal(t, int64(100), brief.GetSize())
		// No path parameter: the config travels to the usecase untouched.
		assert.Same(t, s.params, backup.params)
	})

	t.Run("InFlightJobWithoutArtifact", func(t *testing.T) {
		task := taskmgr.NewMockBackupTaskView(t)
		// The same StateCode expectation serves both the handler's
		// success check and the brief rendering.
		expectBriefRender(task, "task-1", "backup1", backuppb.BackupTaskStateCode_BACKUP_EXECUTING)

		backup := &stubGetBackup{err: app.ErrBackupNotFound}
		s := newListTestServer(t, withGetBackup(backup, nil), withGetBackupTask(&stubGetBackupTask{view: task}, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		brief := resp.GetData()
		require.NotNil(t, brief)
		assert.Equal(t, "backup1", brief.GetName())
		assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_EXECUTING, brief.GetStateCode())
		assert.Zero(t, brief.GetSize())
	})

	t.Run("SuccessJobWithoutArtifactFails", func(t *testing.T) {
		task := taskmgr.NewMockBackupTaskView(t)
		task.EXPECT().StateCode().Return(backuppb.BackupTaskStateCode_BACKUP_SUCCESS)

		backup := &stubGetBackup{err: app.ErrBackupNotFound}
		s := newListTestServer(t, withGetBackup(backup, nil), withGetBackupTask(&stubGetBackupTask{view: task}, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "meta is missing")
	})

	t.Run("NeitherHalfExistsIsNotFound", func(t *testing.T) {
		backup := &stubGetBackup{err: app.ErrBackupNotFound}
		s := newListTestServer(t, withGetBackup(backup, nil), noBackupTask())

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "backup1")
		assert.Contains(t, resp.GetMsg(), "not found")
	})

	t.Run("IDResolvesBackupNameThroughJob", func(t *testing.T) {
		task := taskmgr.NewMockBackupTaskView(t)
		expectBriefRender(task, "task-1", "backup1", backuppb.BackupTaskStateCode_BACKUP_SUCCESS)

		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1"}}
		taskStub := &stubGetBackupTask{view: task}
		s := newListTestServer(t, withGetBackup(backup, nil), withGetBackupTask(taskStub, nil))

		resp := getBackup(t, s, "?backup_id=task-1", "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, app.GetBackupTaskRequest{ID: "task-1"}, taskStub.req)
		assert.Equal(t, "backup1", backup.name)
	})

	t.Run("UnknownIDFailsWithoutReadingArtifact", func(t *testing.T) {
		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1"}}
		s := newListTestServer(t, withGetBackup(backup, nil), noBackupTask())

		resp := getBackup(t, s, "?backup_id=task-1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Zero(t, backup.calls)
	})

	t.Run("JobLookupFailureByNameFails", func(t *testing.T) {
		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1"}}
		taskStub := &stubGetBackupTask{err: errors.New("manager closed")}
		s := newListTestServer(t, withGetBackup(backup, nil), withGetBackupTask(taskStub, nil))

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "manager closed")
		assert.Zero(t, backup.calls)
	})

	t.Run("ArtifactReadFailureFails", func(t *testing.T) {
		backup := &stubGetBackup{err: errors.New("connection closed")}
		s := newListTestServer(t, withGetBackup(backup, nil), noBackupTask())

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "connection closed")
	})

	t.Run("RejectsMissingNameAndIDWithoutCallingUsecases", func(t *testing.T) {
		backup := &stubGetBackup{}
		taskStub := &stubGetBackupTask{}
		s := newListTestServer(t, withGetBackup(backup, nil), withGetBackupTask(taskStub, nil))

		resp := getBackup(t, s, "", "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "empty backup name and backup id")
		assert.Zero(t, backup.calls)
		assert.Zero(t, taskStub.calls)
	})

	t.Run("GeneratesRequestIdWhenMissing", func(t *testing.T) {
		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1"}}
		s := newListTestServer(t, withGetBackup(backup, nil), noBackupTask())

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.NotEmpty(t, resp.GetRequestId())
	})

	t.Run("ForwardsRequestId", func(t *testing.T) {
		backup := &stubGetBackup{info: &backuppb.BackupInfo{Name: "backup1"}}
		s := newListTestServer(t, withGetBackup(backup, nil), noBackupTask())

		resp := getBackup(t, s, "?backup_name=backup1", "rid-1")

		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("MapsArtifactConstructorErrorToFail", func(t *testing.T) {
		s := newListTestServer(t, withGetBackup(&stubGetBackup{}, errors.New("dial timeout")), noBackupTask())

		resp := getBackup(t, s, "?backup_name=backup1", "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
	})
}
