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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

func TestRestoreControl(t *testing.T) {
	s := newListTestServer(t)
	post := func(route, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/"+route, strings.NewReader(body)))
		return w
	}
	for _, route := range []string{"resume_restore", "cleanup_failed_restore"} {
		for _, body := range []string{`{}`, `{"id":"x","strategy":"per_seg"}`, `{"id":"x","persistence":{"type":"local"}}`, `{"id":"x"}{}`} {
			assert.Equal(t, http.StatusBadRequest, post(route, body).Code)
		}
		assert.Equal(t, http.StatusNotFound, post(route, `{"id":"missing"}`).Code)
	}
	id := uuid.NewString()
	req := `{"id":"` + id + `"}`
	tr, addErr := jobstate.Default().AddRestoreTask(id, true)
	require.NoError(t, addErr)
	require.NotNil(t, tr)
	checkResumable := func() {
		t.Helper()
		view := getRestore(t, s, "?id="+id, "")
		require.NotNil(t, view.Data)
		assert.True(t, view.Data.EnableResume)
	}
	checkResumable()
	require.True(t, tr.Start())
	checkResumable()
	assert.Equal(t, http.StatusConflict, post("resume_restore", req).Code)
	config := cfg.New()
	config.Milvus.Grpc.TLSMode.Val = cfg.TLSServer
	config.Milvus.Grpc.CACertPath.Val = t.TempDir() + "/missing"
	tr.SavePlan(&restoreplan.Plan{Milvus: config.Milvus})
	tr.SetFail(assert.AnError)
	require.Equal(t, http.StatusOK, post("resume_restore", req).Code)
	require.Eventually(t, func() bool { return tr.Snapshot().State == jobstate.RestoreStateFail }, time.Second, time.Millisecond)
	view := getRestore(t, s, "?id="+id, "")
	require.NotNil(t, view.Data)
	assert.Contains(t, view.Data.ErrorMessage, "create transport credentials")
	resp := post("cleanup_failed_restore", req)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), `"state_code":6`)
	checkResumable()
	assert.Equal(t, http.StatusConflict, post("resume_restore", req).Code)
}

func TestRestoreControlLoadsStartupLocation(t *testing.T) {
	config := cfg.New().Restore.Persistence
	config.Enabled.Val = true
	config.Type.Val, config.Local.Directory.Val = "local", t.TempDir()
	params := cfg.New()
	params.Restore.Persistence = config
	server, err := New(params)
	require.NoError(t, err)
	silenceEngine(server)
	t.Cleanup(func() { jobstate.Default().ConfigurePersistence(cfg.PersistenceConfig{}) })
	id := uuid.NewString()
	// A separate store simulates a previous process.
	previous := jobstate.NewStore()
	previous.ConfigurePersistence(config)
	tracker, err := previous.AddRestoreTask(id, true)
	require.NoError(t, err)
	plan := &restoreplan.Plan{}
	plan.Restore.KeepTempFiles.Val = true
	tracker.SavePlan(plan)
	tracker.Finish(assert.AnError)
	body, err := json.Marshal(map[string]string{"id": id})
	require.NoError(t, err)
	response := httptest.NewRecorder()
	server.engine.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/cleanup_failed_restore", bytes.NewReader(body)))
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())
	assert.Contains(t, response.Body.String(), `"state_code":6`)
	status, err := jobstate.Default().GetRestoreTask(id)
	require.NoError(t, err)
	assert.Equal(t, jobstate.RestoreStateFailedCleaned, status.State)
}

func TestRestoreControlRejectsUnusableSnapshotUntilCleanup(t *testing.T) {
	params := cfg.New()
	params.Restore.Persistence.Enabled.Val = true
	params.Restore.Persistence.Type.Val = "local"
	params.Restore.Persistence.Local.Directory.Val = t.TempDir()
	server, err := New(params)
	require.NoError(t, err)
	silenceEngine(server)
	t.Cleanup(func() { jobstate.Default().ConfigurePersistence(cfg.PersistenceConfig{}) })
	id := uuid.NewString()
	previous := jobstate.NewStore()
	previous.ConfigurePersistence(params.Restore.Persistence)
	tracker, err := previous.AddRestoreTask(id, true)
	require.NoError(t, err)
	plan := &restoreplan.Plan{}
	plan.Restore.KeepTempFiles.Val = true
	tracker.SavePlan(plan)
	tracker.Finish(assert.AnError)
	// The previous process saved its claim, then exited before saving new progress.
	_, _, err = previous.StartResume(id)
	require.NoError(t, err)
	view := getRestore(t, server, "?id="+id, "")
	assert.Nil(t, view.Data)
	assert.Contains(t, view.Msg, "saved restore progress is no longer usable")
	post := func(route string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		server.engine.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/"+route, strings.NewReader(`{"id":"`+id+`"}`)))
		return response
	}
	rejected := post("resume_restore")
	assert.Equal(t, http.StatusConflict, rejected.Code)
	assert.Contains(t, rejected.Body.String(), "saved restore progress is no longer usable")
	cleaned := post("cleanup_failed_restore")
	assert.Equal(t, http.StatusOK, cleaned.Code, cleaned.Body.String())
	assert.Contains(t, cleaned.Body.String(), `"state_code":6`)
	assert.NotContains(t, cleaned.Body.String(), "snapshotStained")
	assert.Equal(t, http.StatusConflict, post("resume_restore").Code)
}

func TestRestoreControlReturnsPersistenceErrorBeforeExecution(t *testing.T) {
	for _, tc := range []struct {
		name  string
		route string
		state jobstate.RestoreState
	}{
		{"Resume", "resume_restore", jobstate.RestoreStateFail},
		{"Cleanup", "cleanup_failed_restore", jobstate.RestoreStateFailedCleanupPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := cfg.New()
			params.Restore.Persistence.Enabled.Val = true
			params.Restore.Persistence.Type.Val = "local"
			params.Restore.Persistence.Local.Directory.Val = filepath.Join(t.TempDir(), "blocked")
			require.NoError(t, os.WriteFile(params.Restore.Persistence.Local.Directory.Val, []byte("not a directory"), 0600))
			server, err := New(params)
			require.NoError(t, err)
			silenceEngine(server)
			t.Cleanup(func() { jobstate.Default().ConfigurePersistence(cfg.PersistenceConfig{}) })
			id := uuid.NewString()
			tracker, err := jobstate.Default().AddRestoreTask(id, true)
			require.NoError(t, err)
			tracker.SavePlan(&restoreplan.Plan{})
			tracker.Finish(assert.AnError)

			response := httptest.NewRecorder()
			server.engine.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/"+tc.route, strings.NewReader(`{"id":"`+id+`"}`)))
			assert.Equal(t, http.StatusInternalServerError, response.Code)
			assert.Contains(t, response.Body.String(), jobstate.ErrPersistence.Error())
			status, err := jobstate.Default().GetRestoreTask(id)
			require.NoError(t, err)
			assert.Equal(t, tc.state, status.State)
			assert.Equal(t, assert.AnError.Error(), status.ErrorMessage)
		})
	}
}
