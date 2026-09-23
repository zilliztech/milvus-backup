package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestL0CompactHTTPValidation(t *testing.T) {
	s := newListTestServer(t)
	for _, body := range []string{
		`{}`,
		`{"backup_name":"src","output_name":"src","path":"restore/job"}`,
		`{"backup_name":"../src","output_name":"dst","path":"restore/job"}`,
	} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/l0compact", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		s.engine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	}
}

func TestL0CompactHTTPReturnsExistingJob(t *testing.T) {
	req := l0CompactRequest{BackupName: "src", OutputName: "dst", Path: "restore/job", BucketName: "bucket"}
	s := newListTestServer(t)
	s.compactJobs = map[string]*l0CompactJob{
		l0CompactKey(req.BucketName, req.Path, req.OutputName): {request: req, state: l0CompactExecuting},
	}
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		w := httptest.NewRecorder()
		var request *http.Request
		if method == http.MethodPost {
			body, err := json.Marshal(req)
			require.NoError(t, err)
			request = httptest.NewRequest(method, "/api/v1/l0compact", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
		} else {
			request = httptest.NewRequest(method,
				"/api/v1/get_l0compact?backup_name=src&output_name=dst&path=restore/job&bucket_name=bucket", nil)
		}
		s.engine.ServeHTTP(w, request)
		assert.Equal(t, http.StatusOK, w.Code)
		var response l0CompactResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
		assert.Equal(t, l0CompactExecuting, response.StateCode)
		assert.Equal(t, "dst", response.OutputName)
	}
}
