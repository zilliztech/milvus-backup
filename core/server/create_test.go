package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/filter"
)

// stubBackupJob stands in for *app.BackupJob: a canned Run error, and a ran
// channel so async tests can wait for the handler's goroutine.
type stubBackupJob struct {
	runErr error
	ran    chan struct{}
}

func (j stubBackupJob) Run(context.Context) error {
	if j.ran != nil {
		close(j.ran)
	}
	return j.runErr
}

// stubCreateFactory stands in for the job factory: the params and request it
// was called with, a canned construction error, and a call count so tests can
// assert whether the handler reached the action at all.
type stubCreateFactory struct {
	params *v2.Config
	req    app.CreateBackupRequest
	newErr error
	job    stubBackupJob
	calls  int
}

// withCreateBackup wires the stub as the job factory. newErr simulates a
// construction failure — client build, preflight or registration — which
// happens before any job runs.
func withCreateBackup(stub *stubCreateFactory) Option {
	return func(c *config) {
		c.newBackupJob = func(_ context.Context, params *v2.Config, req app.CreateBackupRequest) (backupJob, error) {
			stub.params = params
			stub.req = req
			stub.calls++
			if stub.newErr != nil {
				return nil, stub.newErr
			}
			return stub.job, nil
		}
	}
}

func postBackup(t *testing.T, s *Server, body, requestID string) backuppb.BackupInfoResponse {
	t.Helper()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/create", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		req.Header.Set("request_id", requestID)
	}
	s.engine.ServeHTTP(w, req)

	var resp backuppb.BackupInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	return resp
}

func TestHandleCreateBackup(t *testing.T) {
	t.Run("SyncRunsTheJob", func(t *testing.T) {
		stub := &stubCreateFactory{}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
		assert.Equal(t, "backup1", stub.req.Option.BackupName)
		assert.Equal(t, "rid-1", stub.req.TaskID)
		assert.Equal(t, backup.StrategyAuto, stub.req.Option.Strategy)
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("SyncSuccessResponseCarriesNoPayload", func(t *testing.T) {
		// Preserved v1 wire behavior: the historical handler computed the
		// backup payload and request id, then returned a bare code+msg
		// response. The quirk is kept, not fixed.
		stub := &stubCreateFactory{}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Empty(t, resp.GetRequestId())
		assert.Nil(t, resp.GetData())
	})

	t.Run("GeneratesRequestIdWhenMissing", func(t *testing.T) {
		stub := &stubCreateFactory{}
		s := newListTestServer(t, withCreateBackup(stub))

		postBackup(t, s, `{"backup_name":"backup1"}`, "")

		assert.NotEmpty(t, stub.req.TaskID)
	})

	t.Run("RejectsInvalidNameWithoutBuildingAJob", func(t *testing.T) {
		stub := &stubCreateFactory{}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"bad name"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "whitespace")
		assert.Zero(t, stub.calls)
	})

	t.Run("MapsJobBuildErrorToFail", func(t *testing.T) {
		stub := &stubCreateFactory{newErr: errors.New("dial timeout")}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
	})

	t.Run("MapsRequestBuildErrorToFail", func(t *testing.T) {
		stub := &stubCreateFactory{}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1","strategy":"bogus"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "build strategy")
		assert.Zero(t, stub.calls)
	})

	t.Run("SyncMapsRunErrorToFail", func(t *testing.T) {
		stub := &stubCreateFactory{job: stubBackupJob{runErr: errors.New("bucket unavailable")}}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "bucket unavailable")
		assert.Equal(t, "rid-1", resp.GetRequestId())
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("AsyncRunsTheJobInTheBackground", func(t *testing.T) {
		stub := &stubCreateFactory{job: stubBackupJob{ran: make(chan struct{})}}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1","async":true}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "create backup is executing asynchronously", resp.GetMsg())
		assert.Equal(t, "rid-1", resp.GetRequestId())
		assert.Equal(t, "rid-1", stub.req.TaskID)
		require.Eventually(t, func() bool {
			select {
			case <-stub.job.ran:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond)
	})

	t.Run("AsyncMapsJobBuildErrorToFail", func(t *testing.T) {
		stub := &stubCreateFactory{newErr: errors.New("backup1 (existing task task-1)")}
		s := newListTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1","async":true}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "existing task")
		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("BackupRootPathForksConfig", func(t *testing.T) {
		// The v1 backup_root_path field is applied as a config override, not
		// sent through the request.
		stub := &stubCreateFactory{}
		s := newLoadedTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1","backup_root_path":"other"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		require.NotNil(t, stub.params)
		assert.Equal(t, "other", stub.params.Backup.Storage.RootPath.Val)
		// The fork, not the server's own config, reaches the job.
		assert.NotSame(t, s.params, stub.params)
		// The server's own config stays on the default root path.
		assert.Equal(t, "backup", s.params.Backup.Storage.RootPath.Val)
	})

	t.Run("NoBackupRootPathKeepsServerConfig", func(t *testing.T) {
		stub := &stubCreateFactory{}
		s := newLoadedTestServer(t, withCreateBackup(stub))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		// No backup_root_path: the config travels to the job untouched.
		assert.Same(t, s.params, stub.params)
	})
}

func TestToCreateBackupRequest(t *testing.T) {
	t.Run("MapsOptionFields", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateFactory{}))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{
			BackupName:     "backup1",
			RequestId:      "rid-1",
			Rbac:           true,
			WithIndexExtra: true,
			GcPauseEnable:  true,
			GcPauseAddress: "http://manage",
			Strategy:       "skip_flush",
			Format:         "binlog",
		})

		require.NoError(t, err)
		assert.Equal(t, "rid-1", req.TaskID)
		assert.Equal(t, "backup1", req.Option.BackupName)
		assert.Equal(t, backup.StrategySkipFlush, req.Option.Strategy)
		assert.Equal(t, backup.FormatBinlog, req.Option.Format)
		assert.True(t, req.Option.BackupRBAC)
		assert.True(t, req.Option.BackupIndexExtra)
		assert.True(t, req.Option.PauseGC)
		assert.Equal(t, "http://manage", req.Option.ManageAddr)
	})

	t.Run("DeprecatedForceMapsToSkipFlush", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateFactory{}))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{Force: true})

		require.NoError(t, err)
		assert.Equal(t, backup.StrategySkipFlush, req.Option.Strategy)
	})

	t.Run("DeprecatedMetaOnlyMapsToMetaOnly", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateFactory{}))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{MetaOnly: true})

		require.NoError(t, err)
		assert.Equal(t, backup.StrategyMetaOnly, req.Option.Strategy)
	})

	t.Run("ExplicitStrategyWinsOverDeprecatedFields", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateFactory{}))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{Strategy: "meta_only", Force: true})

		require.NoError(t, err)
		assert.Equal(t, backup.StrategyMetaOnly, req.Option.Strategy)
	})
}

func TestCreateBackupToFilter(t *testing.T) {
	t.Run("FromFilter", func(t *testing.T) {
		f, err := toFilter(&backuppb.CreateBackupRequest{Filter: map[string]*backuppb.CollFilter{
			"db1": {Colls: []string{"*"}},
			"db2": {Colls: []string{"coll1", "coll2"}},
		}})
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1": {AllowAll: true},
			"db2": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		}, f.DBCollFilter)
	})

	t.Run("FromDBCollections", func(t *testing.T) {
		f, err := toFilter(&backuppb.CreateBackupRequest{DbCollections: &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: `{"db1":["coll1","coll2"],"db2":["coll3","coll4"],"db3":[]}`},
		}})
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
			"db3": {AllowAll: true},
		}, f.DBCollFilter)
	})

	t.Run("FromCollectionNames", func(t *testing.T) {
		f, err := toFilter(&backuppb.CreateBackupRequest{CollectionNames: []string{"coll1", "db2.coll2"}})
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"default": {CollName: map[string]struct{}{"coll1": {}}},
			"db2":     {CollName: map[string]struct{}{"coll2": {}}},
		}, f.DBCollFilter)
	})

	t.Run("EmptyRequestFiltersNothing", func(t *testing.T) {
		f, err := toFilter(&backuppb.CreateBackupRequest{})
		assert.NoError(t, err)
		assert.Nil(t, f.DBCollFilter)
	})
}

func TestCreateBackupDBCollectionsToFilter(t *testing.T) {
	dbColl := `{"db1":["coll1","coll2"],"db2":["coll3","coll4"],"db3":[]}`
	f, err := dbCollectionsToFilter(dbColl)
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
		"db3": {AllowAll: true},
	}, f.DBCollFilter)
}

func TestCreateBackupCollectionNamesToFilter(t *testing.T) {
	f, err := collectionNamesToFilter([]string{"coll1", "db1.coll2", "db2.coll3"})
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"default": {CollName: map[string]struct{}{"coll1": {}}},
		"db1":     {CollName: map[string]struct{}{"coll2": {}}},
		"db2":     {CollName: map[string]struct{}{"coll3": {}}},
	}, f.DBCollFilter)
}
