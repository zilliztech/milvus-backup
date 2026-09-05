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

// stubCreateBackup stands in for app.CreateBackup: the request it was called
// with, canned errors, and a call count so tests can assert whether the
// handler reached the action at all. Start hands out a job that signals the
// ran channel, so async tests can wait for the handler's goroutine.
type stubCreateBackup struct {
	req        app.CreateBackupRequest
	startErr   error
	executeErr error
	calls      int
	ran        chan struct{}
}

func (s *stubCreateBackup) Execute(_ context.Context, req app.CreateBackupRequest) (*app.BackupView, error) {
	s.req = req
	s.calls++
	if s.executeErr != nil {
		return nil, s.executeErr
	}
	return &app.BackupView{}, nil
}

func (s *stubCreateBackup) Start(req app.CreateBackupRequest) (app.BackupJob, error) {
	s.req = req
	s.calls++
	if s.startErr != nil {
		return nil, s.startErr
	}
	return stubJob{ran: s.ran}, nil
}

type stubJob struct {
	ran chan struct{}
}

func (j stubJob) Run(context.Context) error {
	if j.ran != nil {
		close(j.ran)
	}
	return nil
}

// withCreateBackup wires the stub as the create usecase. newErr simulates the
// client-construction failure, which happens before any action call.
func withCreateBackup(stub *stubCreateBackup, newErr error) Option {
	return func(c *config) {
		c.newCreateBackup = func(context.Context, *v2.Config) (createBackupUC, error) {
			return stub, newErr
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
	t.Run("SyncRunsThroughTheUsecase", func(t *testing.T) {
		stub := &stubCreateBackup{}
		s := newListTestServer(t, withCreateBackup(stub, nil))

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
		// response. The quirk is kept, not fixed, by this move-only PR.
		stub := &stubCreateBackup{}
		s := newListTestServer(t, withCreateBackup(stub, nil))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Empty(t, resp.GetRequestId())
		assert.Nil(t, resp.GetData())
	})

	t.Run("GeneratesRequestIdWhenMissing", func(t *testing.T) {
		stub := &stubCreateBackup{}
		s := newListTestServer(t, withCreateBackup(stub, nil))

		postBackup(t, s, `{"backup_name":"backup1"}`, "")

		assert.NotEmpty(t, stub.req.TaskID)
	})

	t.Run("RejectsInvalidNameWithoutCallingUsecase", func(t *testing.T) {
		stub := &stubCreateBackup{}
		s := newListTestServer(t, withCreateBackup(stub, nil))

		resp := postBackup(t, s, `{"backup_name":"bad name"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "whitespace")
		assert.Zero(t, stub.calls)
	})

	t.Run("MapsConstructorErrorToFail", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateBackup{}, errors.New("dial timeout")))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
	})

	t.Run("MapsRequestBuildErrorToFail", func(t *testing.T) {
		stub := &stubCreateBackup{}
		s := newListTestServer(t, withCreateBackup(stub, nil))

		resp := postBackup(t, s, `{"backup_name":"backup1","strategy":"bogus"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "build strategy")
		assert.Zero(t, stub.calls)
	})

	t.Run("SyncMapsExecuteErrorToFail", func(t *testing.T) {
		stub := &stubCreateBackup{executeErr: errors.New("bucket unavailable")}
		s := newListTestServer(t, withCreateBackup(stub, nil))

		resp := postBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "bucket unavailable")
		assert.Equal(t, "rid-1", resp.GetRequestId())
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("AsyncStartsJobAndReturnsImmediately", func(t *testing.T) {
		stub := &stubCreateBackup{ran: make(chan struct{})}
		s := newListTestServer(t, withCreateBackup(stub, nil))

		resp := postBackup(t, s, `{"backup_name":"backup1","async":true}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "create backup is executing asynchronously", resp.GetMsg())
		assert.Equal(t, "rid-1", resp.GetRequestId())
		assert.Equal(t, "rid-1", stub.req.TaskID)
		require.Eventually(t, func() bool {
			select {
			case <-stub.ran:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond)
	})

	t.Run("AsyncMapsStartErrorToFail", func(t *testing.T) {
		stub := &stubCreateBackup{startErr: errors.New("backup1 (existing task task-1)")}
		s := newListTestServer(t, withCreateBackup(stub, nil))

		resp := postBackup(t, s, `{"backup_name":"backup1","async":true}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "existing task")
		assert.Equal(t, "rid-1", resp.GetRequestId())
	})
}

func TestToCreateBackupRequest(t *testing.T) {
	t.Run("MapsOptionFields", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateBackup{}, nil))

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
		s := newListTestServer(t, withCreateBackup(&stubCreateBackup{}, nil))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{Force: true})

		require.NoError(t, err)
		assert.Equal(t, backup.StrategySkipFlush, req.Option.Strategy)
	})

	t.Run("DeprecatedMetaOnlyMapsToMetaOnly", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateBackup{}, nil))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{MetaOnly: true})

		require.NoError(t, err)
		assert.Equal(t, backup.StrategyMetaOnly, req.Option.Strategy)
	})

	t.Run("ExplicitStrategyWinsOverDeprecatedFields", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateBackup{}, nil))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{Strategy: "meta_only", Force: true})

		require.NoError(t, err)
		assert.Equal(t, backup.StrategyMetaOnly, req.Option.Strategy)
	})

	t.Run("BackupRootPathOverrides", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateBackup{}, nil))

		req, err := s.toCreateBackupRequest(&backuppb.CreateBackupRequest{BackupRootPath: "other"})

		require.NoError(t, err)
		assert.Equal(t, "other", req.RootPath)
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
