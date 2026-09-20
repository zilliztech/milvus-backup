package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// Use real storage clients against an HTTP stub so these tests cover both the
// request parameters and the admission boundary, without accessing cloud services.
func preflightTestServer(t *testing.T, source http.HandlerFunc) (*Server, string) {
	t.Helper()
	oss := httptest.NewServer(source)
	t.Cleanup(oss.Close)
	host, port, err := net.SplitHostPort(strings.TrimPrefix(oss.URL, "http://"))
	require.NoError(t, err)
	portNum, err := strconv.Atoi(port)
	require.NoError(t, err)
	dest := t.TempDir()
	params, err := v2.Load("", map[string]string{
		"milvus.storage.provider":             v2.ProviderMinio,
		"milvus.storage.address":              host,
		"milvus.storage.port":                 strconv.Itoa(portNum),
		"milvus.storage.region":               "cn-hangzhou",
		"milvus.storage.auth.type":            v2.AuthStatic,
		"milvus.storage.auth.accessKeyID":     "test-ak",
		"milvus.storage.auth.secretAccessKey": "test-sk",
		"milvus.storage.bucketName":           "source-bucket",
		"milvus.storage.rootPath":             "source-instance/",
		"backup.storage.provider":             v2.ProviderLocal,
		"backup.storage.rootPath":             dest,
		"milvus.grpc.tlsMode":                 v2.TLSServer,
		"milvus.grpc.caCertPath":              dest + "/missing-ca.pem",
	})
	require.NoError(t, err)
	return &Server{params: params, config: newDefaultConfig()}, dest
}

type preflightIterator struct{ mock.Mock }

func (i *preflightIterator) Next(ctx context.Context) (storage.ObjectAttr, bool, error) {
	r := i.Called(ctx)
	return r.Get(0).(storage.ObjectAttr), r.Bool(1), r.Error(2)
}

func (i *preflightIterator) Close() error { return i.Called().Error(0) }

func TestSourcePreflightIterator(t *testing.T) {
	denied := errors.New("access denied")
	for _, tc := range []struct {
		name    string
		listErr error
		nextErr error
		hasNext bool
	}{
		{name: "ListPrefixError", listErr: denied},
		{name: "NextError", nextErr: denied},
		{name: "EmptyListing"},
		{name: "ReadOnlyOneResult", hasNext: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := storage.NewMockClient(t)
			params := v2.New()
			params.Milvus.Storage.RootPath.Val = "configured-root/"
			h := &createBackupHandler{params: params, milvusStorage: source}
			var iter storage.ObjectIterator
			if tc.listErr == nil {
				mockIter := &preflightIterator{}
				mockIter.On("Next", mock.Anything).Run(func(args mock.Arguments) {
					deadline, ok := args.Get(0).(context.Context).Deadline()
					assert.True(t, ok)
					assert.WithinDuration(t, time.Now().Add(10*time.Second), deadline, time.Second)
				}).Return(storage.ObjectAttr{}, tc.hasNext, tc.nextErr).Once()
				mockIter.On("Close").Return(nil).Once()
				t.Cleanup(func() { mockIter.AssertExpectations(t) })
				iter = mockIter
			}
			source.EXPECT().ListPrefix(mock.Anything, "configured-root/insert_log/", true).Return(iter, tc.listErr).Once()
			if tc.listErr != nil || tc.nextErr != nil {
				source.EXPECT().Config().Return(storage.Config{Bucket: "configured-bucket"}).Once()
			}
			err := h.checkSourceStorage(context.Background(), backup.StrategyAuto)
			if tc.listErr != nil || tc.nextErr != nil {
				assert.ErrorIs(t, err, denied)
				assert.ErrorContains(t, err, "configured-bucket")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func callCreate(ctx context.Context, t *testing.T, s *Server, req *backuppb.CreateBackupRequest) *backuppb.BackupInfoResponse {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/create", bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("request_id", req.RequestId)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = r
	s.handleCreateBackup(c)
	assert.Equal(t, http.StatusOK, w.Code)
	var resp backuppb.BackupInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, req.RequestId, resp.RequestId)
	return &resp
}

func requireNoBackupTask(t *testing.T, req *backuppb.CreateBackupRequest, dest string) {
	t.Helper()
	_, err := taskmgr.DefaultMgr().GetBackupTask(req.RequestId)
	assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	_, err = taskmgr.DefaultMgr().GetBackupTaskByName(req.BackupName)
	assert.ErrorIs(t, err, taskmgr.ErrTaskNotFound)
	entries, err := os.ReadDir(dest)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestCreateStoragePreflightRetry(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "Sync", true: "Async"}[async], func(t *testing.T) {
			var allowed atomic.Bool
			var calls atomic.Int32
			s, dest := preflightTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/source-bucket/", r.URL.Path)
				assert.Equal(t, "source-instance/insert_log/", r.URL.Query().Get("prefix"))
				assert.Equal(t, "2", r.URL.Query().Get("list-type"))
				w.Header().Set("Content-Type", "application/xml")
				if !allowed.Load() {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>list permission not ready</Message><RequestId>preflight-denied</RequestId></Error>`)
					return
				}
				// Empty prefixes must pass preflight.
				fmt.Fprint(w, `<ListBucketResult><Name>source-bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
			})
			req := &backuppb.CreateBackupRequest{
				RequestId: uuid.NewString(), BackupName: "preflight_" + strings.ReplaceAll(uuid.NewString(), "-", ""), Async: async,
				BackupRootPath: dest + "/override", // Must not change the source prefix.
			}
			resp := callCreate(context.Background(), t, s, req)
			assert.Equal(t, backuppb.ResponseCode_Storage_Not_Ready, resp.Code)
			assert.Contains(t, resp.Msg, "source-bucket")
			assert.Contains(t, resp.Msg, "source-instance/insert_log/")
			assert.Contains(t, resp.Msg, "list permission not ready")
			requireNoBackupTask(t, req, dest)

			allowed.Store(true)
			resp = callCreate(context.Background(), t, s, req)
			if async {
				assert.Equal(t, backuppb.ResponseCode_Success, resp.Code)
			} else {
				// Sync execution gets past admission to our intentional Milvus error.
				assert.Equal(t, backuppb.ResponseCode_Fail, resp.Code)
				assert.Contains(t, resp.Msg, "read ca cert")
			}
			_, err := taskmgr.DefaultMgr().GetBackupTask(req.RequestId)
			assert.NoError(t, err)
			_, err = taskmgr.DefaultMgr().GetBackupTaskByName(req.BackupName)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, calls.Load())
		})
	}
}

func TestCreateStoragePreflightCancellation(t *testing.T) {
	s, dest := preflightTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	req := &backuppb.CreateBackupRequest{RequestId: uuid.NewString(), BackupName: "cancel_" + strings.ReplaceAll(uuid.NewString(), "-", ""), Async: true}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	resp := callCreate(ctx, t, s, req)
	assert.Equal(t, backuppb.ResponseCode_Storage_Not_Ready, resp.Code)
	assert.Contains(t, resp.Msg, "context deadline exceeded")
	requireNoBackupTask(t, req, dest)
}

func TestCreateStoragePreflightMetaOnly(t *testing.T) {
	for _, strategy := range []string{"legacy", "meta_only"} {
		t.Run(map[string]string{"legacy": "Legacy", "meta_only": "MetaOnly"}[strategy], func(t *testing.T) {
			var calls atomic.Int32
			s, _ := preflightTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusForbidden)
			})
			req := &backuppb.CreateBackupRequest{RequestId: uuid.NewString(), BackupName: "meta_" + strings.ReplaceAll(uuid.NewString(), "-", ""), Async: true}
			if strategy == "legacy" {
				req.MetaOnly = true
			} else {
				req.Strategy = strategy
			}
			resp := callCreate(context.Background(), t, s, req)
			assert.Equal(t, backuppb.ResponseCode_Success, resp.Code)
			assert.Zero(t, calls.Load())
		})
	}
}
