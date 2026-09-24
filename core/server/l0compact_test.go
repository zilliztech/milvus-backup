package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

func TestHasL0HTTP(t *testing.T) {
	root := t.TempDir()
	params, err := v2.Load("", map[string]string{
		"backup.storage.provider": v2.ProviderLocal,
		"backup.storage.rootPath": root,
	})
	require.NoError(t, err)
	s, err := New(params)
	require.NoError(t, err)
	cli := &storage.LocalClient{}
	backupDir := mpath.BackupDir(root, "src")
	info := &backuppb.BackupInfo{Name: "src", CollectionBackups: []*backuppb.CollectionBackupInfo{{
		PartitionBackups: []*backuppb.PartitionBackupInfo{{
			SegmentBackups: []*backuppb.SegmentBackupInfo{{SegmentId: 200}},
		}},
	}}}
	query := "/api/v1/has_l0?backup_name=src&path=" + url.QueryEscape(root)

	for _, tc := range []struct {
		name  string
		setL0 func()
		want  bool
	}{
		{name: "no L0", want: false},
		{name: "collection L0", setL0: func() {
			info.CollectionBackups[0].L0Segments = []*backuppb.SegmentBackupInfo{{SegmentId: 100}}
		}, want: true},
		{name: "partition L0", setL0: func() {
			info.CollectionBackups[0].L0Segments = nil
			info.CollectionBackups[0].PartitionBackups[0].SegmentBackups[0].IsL0 = true
		}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setL0 != nil {
				tc.setL0()
			}
			require.NoError(t, meta.Write(context.Background(), cli, backupDir, info))
			w := httptest.NewRecorder()
			s.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, query, nil))
			require.Equal(t, http.StatusOK, w.Code)
			var response hasL0Response
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			assert.Equal(t, "src", response.BackupName)
			assert.Equal(t, tc.want, response.HasL0)
		})
	}

	for _, tc := range []struct {
		query string
		code  int
	}{
		{query: "/api/v1/has_l0?backup_name=src", code: http.StatusBadRequest},
		{query: "/api/v1/has_l0?backup_name=../bad&path=" + url.QueryEscape(root), code: http.StatusBadRequest},
		{query: "/api/v1/has_l0?backup_name=missing&path=" + url.QueryEscape(root), code: http.StatusInternalServerError},
	} {
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.query, nil))
		assert.Equal(t, tc.code, w.Code)
	}
}

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
