package milvus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestfulAuth(t *testing.T) {
	got := restfulAuth("username", "password")
	assert.Equal(t, "username:password", got)

	got = restfulAuth("", "")
	assert.Equal(t, "", got)
}

func TestBulkInsertV2InputOpts(t *testing.T) {
	t.Run("AutoCommitByDefault", func(t *testing.T) {
		opts := (&BulkInsertV2Input{}).opts()
		_, ok := opts["auto_commit"]
		assert.False(t, ok)
	})

	t.Run("ManualCommitDisablesAutoCommit", func(t *testing.T) {
		opts := (&BulkInsertV2Input{ManualCommit: true}).opts()
		assert.Equal(t, "false", opts["auto_commit"])
	})
}

// TestPostImportJobAction drives CommitImport and AbortImport against a real
// HTTP server: both post the db and job id to their endpoint and take a
// non-zero code as a definitive refusal, not something to retry.
func TestPostImportJobAction(t *testing.T) {
	t.Run("CommitAndAbortPostJobToTheirEndpoint", func(t *testing.T) {
		var gotPath atomic.Value
		var gotBody atomic.Value
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath.Store(r.URL.Path)
			var body GetProgressReq
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			gotBody.Store(body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0}`))
		}))
		defer srv.Close()

		cli := &RestfulClient{cli: req.C().SetBaseURL(srv.URL)}

		require.NoError(t, cli.CommitImport(context.Background(), "db1", "job1"))
		assert.Equal(t, "/v2/vectordb/jobs/import/commit", gotPath.Load())
		assert.Equal(t, GetProgressReq{DBName: "db1", JobId: "job1"}, gotBody.Load())

		require.NoError(t, cli.AbortImport(context.Background(), "db1", "job1"))
		assert.Equal(t, "/v2/vectordb/jobs/import/abort", gotPath.Load())
		assert.Equal(t, GetProgressReq{DBName: "db1", JobId: "job1"}, gotBody.Load())
	})

	// A non-zero code is the server refusing this job (say, already aborted):
	// retrying cannot change the answer, so the call must fail at once.
	t.Run("NonZeroCodeFailsWithoutRetry", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":1,"message":"import job is not uncommitted"}`))
		}))
		defer srv.Close()

		cli := &RestfulClient{cli: req.C().SetBaseURL(srv.URL)}

		err := cli.CommitImport(context.Background(), "db1", "job1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "import job is not uncommitted")
		assert.Equal(t, int32(1), calls.Load())
	})
}
