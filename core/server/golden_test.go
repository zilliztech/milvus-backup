package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// The v1 wire shape is an external contract, and this package carries the
// hand-written structs that render it. Goldens pin the JSON the handlers
// answer: every quirk of the shape — success responses carrying no code
// field, data present as null on an empty payload, the fields the generated
// tags never omit — is part of what clients see, so the structs must keep
// reproducing these files value for value.
//
// The cases run over the same stubs the behavior tests use: a request body
// goes in through the real engine, the response body comes out untouched.
// Regenerate with GOLDEN_WRITE=1.
func assertGolden(t *testing.T, name string, body []byte) {
	t.Helper()

	path := filepath.Join("testdata", "golden", strings.ReplaceAll(name, "/", "_")+".json")
	if os.Getenv("GOLDEN_WRITE") == "1" {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		var indented bytes.Buffer
		require.NoError(t, json.Indent(&indented, body, "", "  "))
		require.NoError(t, os.WriteFile(path, append(indented.Bytes(), '\n'), 0o644))
		return
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(body), "golden %s drifted", name)
}

// call runs one HTTP request against the engine and answers the raw response
// body. Every golden case passes an explicit request id, so the responses —
// which echo it — stay stable across runs.
func call(t *testing.T, s *Server, method, path, body, requestID string) []byte {
	t.Helper()

	w := httptest.NewRecorder()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	if requestID != "" {
		req.Header.Set("request_id", requestID)
	}
	s.engine.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	return w.Body.Bytes()
}

// newGoldenRestoreStatus returns a job snapshot with one collection, so the
// rendered response pins both levels of the restore wire shape. Map order
// would be free-form, so the map carries exactly one collection.
func newGoldenRestoreStatus() jobstate.RestoreStatus {
	return jobstate.RestoreStatus{
		ID:        "task-1",
		State:     jobstate.RestoreStateSuccess,
		StartTime: time.Unix(1, 0),
		EndTime:   time.Unix(2, 0),
		Progress:  100,
		Colls: map[collref.Name]jobstate.RestoreCollStatus{
			collref.New("db1", "coll1"): {
				State:        jobstate.RestoreStateExecuting,
				ErrorMessage: "slow copy",
				StartTime:    time.Unix(1, 0),
				EndTime:      time.Unix(2, 0),
				Progress:     42,
			},
		},
	}
}

func TestGoldenV1(t *testing.T) {
	t.Run("CreateSyncSuccess", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateFactory{}))
		body := call(t, s, http.MethodPost, "/api/v1/create", `{"backup_name":"backup1"}`, "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("CreateAsyncSuccess", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateFactory{}))
		body := call(t, s, http.MethodPost, "/api/v1/create", `{"backup_name":"backup1","async":true}`, "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("CreateParamError", func(t *testing.T) {
		s := newListTestServer(t, withCreateBackup(&stubCreateFactory{}))
		body := call(t, s, http.MethodPost, "/api/v1/create", `{"backup_name":"bad/name"}`, "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("CreateJobFail", func(t *testing.T) {
		stub := &stubCreateFactory{newErr: assert.AnError}
		s := newListTestServer(t, withCreateBackup(stub))
		body := call(t, s, http.MethodPost, "/api/v1/create", `{"backup_name":"backup1"}`, "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("RestoreSuccess", func(t *testing.T) {
		stub := &stubRestoreFactory{job: &stubRestoreJob{}}
		s := newListTestServer(t, withRestoreBackup(stub), withGetRestore(&stubGetRestore{status: newGoldenRestoreStatus()}, nil))
		body := call(t, s, http.MethodPost, "/api/v1/restore", `{"requestId":"rid-1","backup_name":"backup1"}`, "")
		assertGolden(t, t.Name(), body)
	})

	t.Run("RestoreAsyncSuccess", func(t *testing.T) {
		stub := &stubRestoreFactory{job: &stubRestoreJob{}}
		s := newListTestServer(t, withRestoreBackup(stub), withGetRestore(&stubGetRestore{status: newGoldenRestoreStatus()}, nil))
		body := call(t, s, http.MethodPost, "/api/v1/restore", `{"requestId":"rid-1","backup_name":"backup1","async":true}`, "")
		assertGolden(t, t.Name(), body)
	})

	t.Run("RestoreParamError", func(t *testing.T) {
		s := newListTestServer(t, withRestoreBackup(&stubRestoreFactory{}))
		body := call(t, s, http.MethodPost, "/api/v1/restore", `{"requestId":"rid-1"}`, "")
		assertGolden(t, t.Name(), body)
	})

	t.Run("RestoreSecondarySuccess", func(t *testing.T) {
		stub := &stubRestoreSecondaryFactory{job: &stubRestoreJob{}}
		s := newListTestServer(t, withRestoreSecondary(stub), withGetRestore(&stubGetRestore{status: newGoldenRestoreStatus()}, nil))
		body := call(t, s, http.MethodPost, "/api/v1/restore_secondary",
			`{"requestId":"rid-1","backup_name":"backup1","sourceClusterID":"src","targetClusterID":"dst"}`, "")
		assertGolden(t, t.Name(), body)
	})

	t.Run("GetBackupMerged", func(t *testing.T) {
		// The artifact half carries every brief field with a value that makes
		// its tag visible: zero-able fields the wire never omits (size,
		// has_index) sit next to ones it does (schema, index_infos), so the
		// golden tells the two apart.
		task := &jobstate.BackupStatus{
			ID: "task-1", Name: "backup1", State: jobstate.BackupStateSuccess,
			StartTime: time.Unix(1, 0), EndTime: time.Unix(2, 0),
		}
		backup := &stubGetBackup{
			info: &backuppb.BackupInfo{
				Name: "backup1", Size: 100, MilvusVersion: "2.4.0", Format: "binlog",
				BackupTimestamp: 1700000000000,
				CollectionBackups: []*backuppb.CollectionBackupInfo{{
					Id: "c-1", CollectionId: 448344929307584300, DbName: "db1", CollectionName: "coll1",
					ShardsNum: 2, Size: 0, HasIndex: false, LoadState: "LoadStateLoaded",
					BackupTimestamp:         1700000000000,
					BackupPhysicalTimestamp: 1700000000,
					ChannelCheckpoints:      map[string]string{"ch-0": "checkpoint"},
					Properties:              []*backuppb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}},
				}},
			},
			size: 42,
		}
		s := newListTestServer(t, withGetBackup(backup, nil), withGetBackupTask(&stubGetBackupTask{status: task}, nil))

		body := call(t, s, http.MethodGet, "/api/v1/get_backup?backup_name=backup1", "", "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("GetBackupParamError", func(t *testing.T) {
		s := newListTestServer(t, noBackupTask())
		body := call(t, s, http.MethodGet, "/api/v1/get_backup", "", "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("ListSuccess", func(t *testing.T) {
		stub := &stubListBackups{summaries: []app.BackupSummary{
			{ID: "a", Name: "backup1", Size: 100, MilvusVersion: "2.4.0"},
			{ID: "b", Name: "backup2", Size: 200, MilvusVersion: "2.5.0"},
		}}
		s := newListTestServer(t, withListBackups(stub, nil))

		body := call(t, s, http.MethodGet, "/api/v1/list", "", "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("DeleteSuccess", func(t *testing.T) {
		status := jobstate.DeleteStatus{ID: "rid-1", Name: "backup1", State: jobstate.DeleteStateSuccess}
		s := newListTestServer(t, withDeleteJob(nil, status))
		body := call(t, s, http.MethodDelete, "/api/v1/delete?backup_name=backup1", "", "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("DeleteParamError", func(t *testing.T) {
		s := newListTestServer(t, withDeleteJob(nil, jobstate.DeleteStatus{}))
		body := call(t, s, http.MethodDelete, "/api/v1/delete", "", "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("GetRestoreSuccess", func(t *testing.T) {
		s := newListTestServer(t, withGetRestore(&stubGetRestore{status: newGoldenRestoreStatus()}, nil))
		body := call(t, s, http.MethodGet, "/api/v1/get_restore?id=task-1", "", "rid-1")
		assertGolden(t, t.Name(), body)
	})

	t.Run("GetRestoreEmptyID", func(t *testing.T) {
		s := newListTestServer(t, withGetRestore(&stubGetRestore{}, nil))
		body := call(t, s, http.MethodGet, "/api/v1/get_restore?id=", "", "rid-1")
		assertGolden(t, t.Name(), body)
	})
}
