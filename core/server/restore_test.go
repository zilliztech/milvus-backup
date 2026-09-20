package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func TestValidateRestoreRequest(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup"}
		assert.NoError(t, validateRestoreRequest(request))
	})

	t.Run("BackupNameEmpty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		assert.Error(t, validateRestoreRequest(request))
	})

	t.Run("DropAndNotCreate", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", DropExistCollection: true, SkipCreateCollection: true}
		assert.Error(t, validateRestoreRequest(request))
	})

	t.Run("RestorePlanAndCollectionSuffix", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", RestorePlan: &backuppb.RestorePlan{}, CollectionSuffix: "_suffix"}
		assert.Error(t, validateRestoreRequest(request))
	})

	t.Run("RestorePlanAndCollectionRenames", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", RestorePlan: &backuppb.RestorePlan{}, CollectionRenames: map[string]string{"db1.coll1": "db2.coll2"}}
		assert.Error(t, validateRestoreRequest(request))
	})

	t.Run("InvalidCollectionSuffix", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{BackupName: "backup", CollectionSuffix: "invalid-suffix"}
		assert.Error(t, validateRestoreRequest(request))
	})
}

func TestNewTableMapperFromCollRename(t *testing.T) {
	r, err := newTableMapperFromCollRename(map[string]string{
		"db1.*":     "db2.*",
		"db1.coll1": "db2.coll2",
		"coll1":     "coll2",
		"db1.":      "db2.",
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"db1": "db2"}, r.DBWildcard)
	assert.Equal(t, map[string][]collref.Name{
		"db1.coll1": {
			collref.New("db2", "coll2"),
		},
		"default.coll1": {
			collref.New("", "coll2"),
		},
	}, r.NameMapping)
}

func TestNewCollMapperFromPlan(t *testing.T) {
	plan := &backuppb.RestorePlan{Mapping: []*backuppb.RestoreMapping{
		{
			Source: "db1",
			Target: "db2",
			Colls: []*backuppb.RestoreCollectionMapping{
				{Source: "coll1", Target: "coll2"},
				{Source: "coll2", Target: "coll3"},
			},
		},
		{
			Source: "db1",
			Target: "db3",
			Colls: []*backuppb.RestoreCollectionMapping{
				{Source: "coll1", Target: "coll2"},
				{Source: "coll2", Target: "coll3"},
			},
		},
	}}

	mapper, err := newCollMapperFromPlan(plan)
	assert.NoError(t, err)
	tMapper, ok := mapper.(*restore.TableMapper)
	assert.True(t, ok)
	assert.Equal(t, map[string][]collref.Name{
		"db1.coll1": {
			collref.New("db2", "coll2"),
			collref.New("db3", "coll2"),
		},
		"db1.coll2": {
			collref.New("db2", "coll3"),
			collref.New("db3", "coll3"),
		},
	}, tMapper.NameMapping)
}

func TestNewCollMapper(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{RestorePlan: &backuppb.RestorePlan{
			Mapping: []*backuppb.RestoreMapping{
				{
					Source: "db1",
					Target: "db2",
					Colls: []*backuppb.RestoreCollectionMapping{
						{Source: "coll1", Target: "coll2"},
					},
				},
			},
		}}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		target := mapper.TargetNames(collref.New("db1", "coll1"))
		assert.ElementsMatch(t, []collref.Name{collref.New("db2", "coll2")}, target)
	})

	t.Run("FromCollRename", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionRenames: map[string]string{
			"db1.coll1": "db2.coll2",
		}}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		target := mapper.TargetNames(collref.New("db1", "coll1"))
		assert.ElementsMatch(t, []collref.Name{collref.New("db2", "coll2")}, target)
	})

	t.Run("FromCollSuffix", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionSuffix: "_suffix"}
		mapper, err := newCollMapper(request)
		assert.NoError(t, err)
		target := mapper.TargetNames(collref.New("db1", "coll1"))
		assert.ElementsMatch(t, []collref.Name{collref.New("db1", "coll1_suffix")}, target)
	})
}

func TestNewDBMapper(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		plan := &backuppb.RestorePlan{Mapping: []*backuppb.RestoreMapping{{
			Source: "db1",
			Target: "db2",
		}, {
			Source: "db1",
			Target: "db3",
		}}}

		mapper, err := newDBMapper(plan)
		assert.NoError(t, err)
		assert.Equal(t, map[string][]restore.DBMapping{
			"db1": {
				{Target: "db2"},
				{Target: "db3"},
			},
		}, mapper)
	})

	t.Run("Empty", func(t *testing.T) {
		plan := &backuppb.RestorePlan{}
		mapper, err := newDBMapper(plan)
		assert.NoError(t, err)
		assert.Empty(t, mapper)
	})
}

func TestNewFilterFromDBCollections(t *testing.T) {
	f, err := newFilterFromDBCollections(`{"db1":[],"db2":["coll1","coll2"],"": ["coll3"]}`)
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"db1":     {AllowAll: true},
		"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"default": {CollName: map[string]struct{}{"coll3": {}}},
	}, f.DBCollFilter)
}

func TestNewFilterFromCollectionNames(t *testing.T) {
	f, err := newFilterFromCollectionNames([]string{"coll1", "db2.coll2"})
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"default": {CollName: map[string]struct{}{"coll1": {}}},
		"db2":     {CollName: map[string]struct{}{"coll2": {}}},
	}, f.DBCollFilter)
}

func TestNewBackupFilter(t *testing.T) {
	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{
			// CollectionNames will be ignored
			CollectionNames: []string{"coll1", "db2.coll2"},
			DbCollections: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
			}}
		f, err := newBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
		}, f.DBCollFilter)
	})

	t.Run("FromCollectionNames", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{CollectionNames: []string{"coll1", "db2.coll2"}}
		f, err := newBackupFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"default": {CollName: map[string]struct{}{"coll1": {}}},
			"db2":     {CollName: map[string]struct{}{"coll2": {}}},
		}, f.DBCollFilter)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		f, err := newBackupFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, f.DBCollFilter)
	})
}

func TestNewFilterFromPlan(t *testing.T) {
	plan := &backuppb.RestorePlan{Filter: map[string]*backuppb.CollFilter{
		"db1": {Colls: []string{"coll1", "coll2"}},
		"db2": {Colls: []string{"coll3", "coll4"}},
	}}
	f, err := newFilterFromPlan(plan)
	assert.NoError(t, err)
	assert.Equal(t, map[string]filter.CollFilter{
		"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
		"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
	}, f.DBCollFilter)
}

func TestNewTaskFilter(t *testing.T) {
	t.Run("FromPlan", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{
			// dbCollectionsAfterRename will be ignored
			DbCollectionsAfterRename: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
			},
			RestorePlan: &backuppb.RestorePlan{
				Filter: map[string]*backuppb.CollFilter{
					"db1": {Colls: []string{"coll1", "coll2"}},
					"db2": {Colls: []string{"coll3", "coll4"}},
				},
			}}
		f, err := newTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"db2": {CollName: map[string]struct{}{"coll3": {}, "coll4": {}}},
		}, f.DBCollFilter)
	})

	t.Run("FromDBCollections", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{DbCollectionsAfterRename: &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: `{"db1":[],"db2":["coll1","coll2"], "": ["coll3"]}`},
		}}
		f, err := newTaskFilter(request)
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}, "coll2": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
		}, f.DBCollFilter)
	})

	t.Run("Empty", func(t *testing.T) {
		request := &backuppb.RestoreBackupRequest{}
		f, err := newTaskFilter(request)
		assert.NoError(t, err)
		assert.Empty(t, f.DBCollFilter)
	})
}

func TestNewCollOverridesFromPlan(t *testing.T) {
	t.Run("NilPlan", func(t *testing.T) {
		result := newCollOverridesFromPlan(nil)
		assert.Nil(t, result)
	})

	t.Run("NoOverrides", func(t *testing.T) {
		plan := &backuppb.RestorePlan{
			Mapping: []*backuppb.RestoreMapping{{
				Source: "db1",
				Target: "db2",
				Colls: []*backuppb.RestoreCollectionMapping{
					{Source: "coll1", Target: "coll2"},
				},
			}},
		}
		result := newCollOverridesFromPlan(plan)
		assert.Nil(t, result)
	})

	t.Run("WithShardNumOverride", func(t *testing.T) {
		plan := &backuppb.RestorePlan{
			Mapping: []*backuppb.RestoreMapping{{
				Source: "db1",
				Target: "db2",
				Colls: []*backuppb.RestoreCollectionMapping{
					{
						Source:   "coll1",
						Target:   "coll2",
						Override: &backuppb.RestoreCollectionOverride{ShardNum: 4},
					},
				},
			}},
		}
		result := newCollOverridesFromPlan(plan)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(4), result["db2.coll2"].ShardNum)
		assert.Equal(t, "", result["db2.coll2"].Description)
	})

	t.Run("WithDescriptionOverride", func(t *testing.T) {
		plan := &backuppb.RestorePlan{
			Mapping: []*backuppb.RestoreMapping{{
				Source: "db1",
				Target: "db2",
				Colls: []*backuppb.RestoreCollectionMapping{
					{
						Source:   "coll1",
						Target:   "coll2",
						Override: &backuppb.RestoreCollectionOverride{Description: "new desc"},
					},
				},
			}},
		}
		result := newCollOverridesFromPlan(plan)
		assert.Len(t, result, 1)
		assert.Equal(t, "new desc", result["db2.coll2"].Description)
	})

	t.Run("WithBothOverrides", func(t *testing.T) {
		plan := &backuppb.RestorePlan{
			Mapping: []*backuppb.RestoreMapping{{
				Source: "db1",
				Target: "db2",
				Colls: []*backuppb.RestoreCollectionMapping{
					{
						Source:   "coll1",
						Target:   "coll2",
						Override: &backuppb.RestoreCollectionOverride{ShardNum: 2, Description: "desc"},
					},
					{
						Source: "coll3",
						Target: "coll4",
					},
				},
			}},
		}
		result := newCollOverridesFromPlan(plan)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(2), result["db2.coll2"].ShardNum)
		assert.Equal(t, "desc", result["db2.coll2"].Description)
		_, exists := result["db2.coll4"]
		assert.False(t, exists)
	})

	t.Run("EmptyOverrideSkipped", func(t *testing.T) {
		plan := &backuppb.RestorePlan{
			Mapping: []*backuppb.RestoreMapping{{
				Source: "db1",
				Target: "db2",
				Colls: []*backuppb.RestoreCollectionMapping{
					{
						Source:   "coll1",
						Target:   "coll2",
						Override: &backuppb.RestoreCollectionOverride{},
					},
				},
			}},
		}
		result := newCollOverridesFromPlan(plan)
		assert.Nil(t, result)
	})
}

// stubRestoreJob stands in for app.RestoreJob: a canned error and a call
// count, so tests can assert whether the handler ran the job at all. The
// count is atomic because async tests poll it from the test goroutine while
// the handler's job goroutine increments it.
type stubRestoreJob struct {
	runErr error
	calls  atomic.Int32
}

func (j *stubRestoreJob) Run(context.Context) error {
	j.calls.Add(1)
	return j.runErr
}

// stubRestoreBackup stands in for app.Restore: the params its constructor
// received, the request it was called with, canned errors, and a call count.
type stubRestoreBackup struct {
	job      app.RestoreJob
	startErr error

	params *v2.Config
	req    app.RestoreRequest
	calls  int
}

func (s *stubRestoreBackup) Start(_ context.Context, req app.RestoreRequest) (app.RestoreJob, error) {
	s.calls++
	s.req = req
	if s.startErr != nil {
		return nil, s.startErr
	}

	return s.job, nil
}

// withRestoreBackup wires the stub as the restore usecase. newErr simulates
// the construction failure, which happens before any Start call.
func withRestoreBackup(stub *stubRestoreBackup, newErr error) Option {
	return func(c *config) {
		c.newRestoreBackup = func(ctx context.Context, params *v2.Config) (restoreBackupUC, error) {
			stub.params = params
			return stub, newErr
		}
	}
}

// newStubRestoreView returns a task view the rendering can run over.
func newStubRestoreView(t *testing.T) *taskmgr.MockRestoreTaskView {
	t.Helper()

	view := taskmgr.NewMockRestoreTaskView(t)
	view.EXPECT().ID().Return("task-1").Maybe()
	view.EXPECT().StateCode().Return(backuppb.RestoreTaskStateCode_SUCCESS).Maybe()
	view.EXPECT().ErrorMessage().Return("").Maybe()
	view.EXPECT().StartTime().Return(time.Unix(1, 0)).Maybe()
	view.EXPECT().EndTime().Return(time.Unix(2, 0)).Maybe()
	view.EXPECT().Progress().Return(int32(100)).Maybe()
	view.EXPECT().TotalSize().Return(int64(0)).Maybe()
	view.EXPECT().CollTasks().Return(nil).Maybe()

	return view
}

func restoreBackup(t *testing.T, s *Server, body string, requestID string) backuppb.RestoreBackupResponse {
	t.Helper()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/restore", strings.NewReader(body))
	if requestID != "" {
		req.Header.Set("request_id", requestID)
	}
	s.engine.ServeHTTP(w, req)

	var resp backuppb.RestoreBackupResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	return resp
}

func TestHandleRestoreBackup(t *testing.T) {
	t.Run("RestoresThroughTheUsecase", func(t *testing.T) {
		job := &stubRestoreJob{}
		stub := &stubRestoreBackup{job: job}
		gr := &stubGetRestore{view: newStubRestoreView(t)}
		s := newListTestServer(t, withRestoreBackup(stub, nil), withGetRestore(gr, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
		assert.Equal(t, "task-1", resp.GetData().GetId())
		assert.Equal(t, 1, stub.calls)
		assert.Equal(t, int32(1), job.calls.Load())
		// The view is read through the get-restore usecase, for the same
		// task id that went to Start.
		assert.Equal(t, stub.req.TaskID, gr.id)
	})

	t.Run("RejectsInvalidBody", func(t *testing.T) {
		stub := &stubRestoreBackup{}
		s := newListTestServer(t, withRestoreBackup(stub, nil))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/restore", strings.NewReader("{invalid"))
		s.engine.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Zero(t, stub.calls)
	})

	t.Run("RejectsMissingNameWithoutCallingUsecase", func(t *testing.T) {
		stub := &stubRestoreBackup{}
		s := newListTestServer(t, withRestoreBackup(stub, nil))

		resp := restoreBackup(t, s, `{}`, "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "backup name is required")
		assert.Zero(t, stub.calls)
	})

	t.Run("DefaultsRequestIdAndTaskId", func(t *testing.T) {
		job := &stubRestoreJob{}
		stub := &stubRestoreBackup{job: job}
		s := newListTestServer(t, withRestoreBackup(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "")

		// The response echoes a generated request id; the usecase got a
		// generated task id of the endpoint's own "restore_" shape.
		assert.NotEmpty(t, resp.GetRequestId())
		assert.True(t, strings.HasPrefix(stub.req.TaskID, "restore_"))
		assert.Equal(t, "backup1", stub.req.BackupName)
	})

	t.Run("ForwardsRequestId", func(t *testing.T) {
		job := &stubRestoreJob{}
		stub := &stubRestoreBackup{job: job}
		s := newListTestServer(t, withRestoreBackup(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		// This endpoint takes the request id from the body field, unlike the
		// list and delete handlers, which read the header.
		resp := restoreBackup(t, s, `{"backup_name":"backup1","requestId":"rid-1"}`, "")

		assert.Equal(t, "rid-1", resp.GetRequestId())
	})

	t.Run("BucketNameForksConfig", func(t *testing.T) {
		// The v1 bucket_name field is applied as a config override, not sent
		// through the request.
		stub := &stubRestoreBackup{job: &stubRestoreJob{}}
		s := newLoadedTestServer(t, withRestoreBackup(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1","bucket_name":"other-bucket"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		require.NotNil(t, stub.params)
		assert.Equal(t, "other-bucket", stub.params.Backup.Storage.BucketName.Val)
		// The fork, not the server's own config, reaches the usecase.
		assert.NotSame(t, s.params, stub.params)
		// The server's own config stays on its own bucket.
		assert.NotEqual(t, "other-bucket", s.params.Backup.Storage.BucketName.Val)
	})

	t.Run("PathForksConfig", func(t *testing.T) {
		// The v1 path field is applied as a config override, not sent
		// through the request.
		stub := &stubRestoreBackup{job: &stubRestoreJob{}}
		s := newLoadedTestServer(t, withRestoreBackup(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1","path":"other"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		require.NotNil(t, stub.params)
		assert.Equal(t, "other", stub.params.Backup.Storage.RootPath.Val)
		// The fork, not the server's own config, reaches the usecase.
		assert.NotSame(t, s.params, stub.params)
		// The server's own config stays on the default root path.
		assert.Equal(t, "backup", s.params.Backup.Storage.RootPath.Val)
	})

	t.Run("NoOverridesKeepServerConfig", func(t *testing.T) {
		stub := &stubRestoreBackup{job: &stubRestoreJob{}}
		s := newLoadedTestServer(t, withRestoreBackup(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		// No override fields: the config travels to the usecase untouched.
		assert.Same(t, s.params, stub.params)
	})

	t.Run("MapsUnknownBackupToParameterError", func(t *testing.T) {
		stub := &stubRestoreBackup{startErr: fmt.Errorf("app: backup backup1: %w", app.ErrBackupNotFound)}
		s := newListTestServer(t, withRestoreBackup(stub, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "backup backup1: backup not found")
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("MapsConstructionErrorToFail", func(t *testing.T) {
		stub := &stubRestoreBackup{}
		s := newListTestServer(t, withRestoreBackup(stub, errors.New("dial timeout")))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
		assert.Zero(t, stub.calls)
	})

	t.Run("MapsStartErrorToFail", func(t *testing.T) {
		stub := &stubRestoreBackup{startErr: errors.New("dial timeout")}
		s := newListTestServer(t, withRestoreBackup(stub, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("MapsRunErrorToFail", func(t *testing.T) {
		job := &stubRestoreJob{runErr: errors.New("bulk insert failed")}
		stub := &stubRestoreBackup{job: job}
		s := newListTestServer(t, withRestoreBackup(stub, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "bulk insert failed")
		assert.Equal(t, int32(1), job.calls.Load())
	})

	t.Run("MapsGetRestoreErrorToFail", func(t *testing.T) {
		job := &stubRestoreJob{}
		stub := &stubRestoreBackup{job: job}
		s := newListTestServer(t, withRestoreBackup(stub, nil), withGetRestore(&stubGetRestore{executeErr: errors.New("task mgr closed")}, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "task mgr closed")
	})

	t.Run("AsyncRunsTheJobAndReportsIt", func(t *testing.T) {
		job := &stubRestoreJob{}
		stub := &stubRestoreBackup{job: job}
		s := newListTestServer(t, withRestoreBackup(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		resp := restoreBackup(t, s, `{"backup_name":"backup1","async":true}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "restore backup is executing asynchronously", resp.GetMsg())
		assert.Equal(t, "task-1", resp.GetData().GetId())
		assert.Eventually(t, func() bool { return job.calls.Load() == 1 }, time.Second, 10*time.Millisecond)
	})
}

// stubRestoreSecondary stands in for app.RestoreSecondary.
type stubRestoreSecondary struct {
	job      app.RestoreJob
	startErr error

	params *v2.Config
	req    app.RestoreSecondaryRequest
	calls  int
}

func (s *stubRestoreSecondary) Start(_ context.Context, req app.RestoreSecondaryRequest) (app.RestoreJob, error) {
	s.calls++
	s.req = req
	if s.startErr != nil {
		return nil, s.startErr
	}

	return s.job, nil
}

// withRestoreSecondary wires the stub as the secondary restore usecase.
// newErr simulates the construction failure, which happens before any Start
// call.
func withRestoreSecondary(stub *stubRestoreSecondary, newErr error) Option {
	return func(c *config) {
		c.newRestoreSecondary = func(ctx context.Context, params *v2.Config) (restoreSecondaryUC, error) {
			stub.params = params
			return stub, newErr
		}
	}
}

func restoreSecondary(t *testing.T, s *Server, body string, requestID string) backuppb.RestoreBackupResponse {
	t.Helper()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/restore_secondary", strings.NewReader(body))
	if requestID != "" {
		req.Header.Set("request_id", requestID)
	}
	s.engine.ServeHTTP(w, req)

	var resp backuppb.RestoreBackupResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	return resp
}

func TestHandleRestoreSecondary(t *testing.T) {
	t.Run("RestoresThroughTheUsecase", func(t *testing.T) {
		job := &stubRestoreJob{}
		stub := &stubRestoreSecondary{job: job}
		gr := &stubGetRestore{view: newStubRestoreView(t)}
		s := newListTestServer(t, withRestoreSecondary(stub, nil), withGetRestore(gr, nil))

		resp := restoreSecondary(t, s, `{"backup_name":"backup1","sourceClusterID":"src","targetClusterID":"dst"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "success", resp.GetMsg())
		assert.Equal(t, "task-1", resp.GetData().GetId())
		assert.Equal(t, 1, stub.calls)
		assert.Equal(t, int32(1), job.calls.Load())
		// The request id doubles as the restore task id.
		assert.NotEmpty(t, resp.GetRequestId())
		assert.Equal(t, resp.GetRequestId(), stub.req.TaskID)
		// The view is read through the get-restore usecase, for the task id
		// the request id doubles as.
		assert.Equal(t, stub.req.TaskID, gr.id)
	})

	t.Run("RejectsMissingClusterIDsWithoutCallingUsecase", func(t *testing.T) {
		stub := &stubRestoreSecondary{}
		s := newListTestServer(t, withRestoreSecondary(stub, nil))

		resp := restoreSecondary(t, s, `{"backup_name":"backup1"}`, "")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "source cluster id is required")
		assert.Zero(t, stub.calls)
	})

	t.Run("PathForksConfig", func(t *testing.T) {
		// The v1 path field is applied as a config override, not sent
		// through the request.
		stub := &stubRestoreSecondary{job: &stubRestoreJob{}}
		s := newLoadedTestServer(t, withRestoreSecondary(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		resp := restoreSecondary(t, s, `{"backup_name":"backup1","sourceClusterID":"src","targetClusterID":"dst","path":"other"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		require.NotNil(t, stub.params)
		assert.Equal(t, "other", stub.params.Backup.Storage.RootPath.Val)
		// The fork, not the server's own config, reaches the usecase.
		assert.NotSame(t, s.params, stub.params)
		// The server's own config stays on the default root path.
		assert.Equal(t, "backup", s.params.Backup.Storage.RootPath.Val)
	})

	t.Run("MapsUnknownBackupToParameterError", func(t *testing.T) {
		stub := &stubRestoreSecondary{startErr: fmt.Errorf("app: backup backup1: %w", app.ErrBackupNotFound)}
		s := newListTestServer(t, withRestoreSecondary(stub, nil))

		resp := restoreSecondary(t, s, `{"backup_name":"backup1","sourceClusterID":"src","targetClusterID":"dst"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Parameter_Error, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "backup backup1: backup not found")
	})

	t.Run("MapsConstructionErrorToFail", func(t *testing.T) {
		stub := &stubRestoreSecondary{}
		s := newListTestServer(t, withRestoreSecondary(stub, errors.New("dial timeout")))

		resp := restoreSecondary(t, s, `{"backup_name":"backup1","sourceClusterID":"src","targetClusterID":"dst"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "dial timeout")
		assert.Zero(t, stub.calls)
	})

	t.Run("MapsRunErrorToFail", func(t *testing.T) {
		job := &stubRestoreJob{runErr: errors.New("ddl replay failed")}
		stub := &stubRestoreSecondary{job: job}
		s := newListTestServer(t, withRestoreSecondary(stub, nil))

		resp := restoreSecondary(t, s, `{"backup_name":"backup1","sourceClusterID":"src","targetClusterID":"dst"}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
		assert.Contains(t, resp.GetMsg(), "ddl replay failed")
	})

	t.Run("AsyncRunsTheJobAndReportsIt", func(t *testing.T) {
		job := &stubRestoreJob{}
		stub := &stubRestoreSecondary{job: job}
		s := newListTestServer(t, withRestoreSecondary(stub, nil), withGetRestore(&stubGetRestore{view: newStubRestoreView(t)}, nil))

		resp := restoreSecondary(t, s, `{"backup_name":"backup1","sourceClusterID":"src","targetClusterID":"dst","async":true}`, "rid-1")

		assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())
		assert.Equal(t, "restore backup is executing asynchronously", resp.GetMsg())
		assert.Eventually(t, func() bool { return job.calls.Load() == 1 }, time.Second, 10*time.Millisecond)
	})
}
