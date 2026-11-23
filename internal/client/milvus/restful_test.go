package milvus

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

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

func TestGetBulkInsertStateDiscardsPartialResponseOnRetry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch calls.Add(1) {
		case 1:
			// JSON can populate message before reporting a type error on another field.
			_, _ = w.Write([]byte(`{"message":"import job does not exist, jobID=42","code":"invalid"}`))
			return
		case 2:
			_, _ = w.Write([]byte(`{"code":2100}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer srv.Close()

	cli := &RestfulClient{cli: req.C().SetBaseURL(srv.URL)}
	resp, err := cli.GetBulkInsertState(t.Context(), "db", "42")
	require.NoError(t, err)
	assert.EqualValues(t, 3, calls.Load())
	assert.Zero(t, resp.Code)
	assert.Empty(t, resp.Message)
	assert.NotErrorIs(t, ImportLookupError("42", resp.Code, resp.Message), ErrImportJobNotFound)
}

func TestGetBulkInsertStateQueryRetries(t *testing.T) {
	for _, tc := range []struct {
		name              string
		first, next       string
		calls             int
		wantError, cancel bool
	}{
		{"TransientBusinessError", `{"code":1,"message":"temporarily unavailable"}`, `{"code":0,"data":{"state":"Completed"}}`, 2, false, false},
		{"RetryLimit", `{"code":1,"message":"temporarily unavailable"}`, `{"code":1,"message":"temporarily unavailable"}`, 10, true, false},
		{"TaskFailedIsNotQueryFailure", `{"code":0,"data":{"state":"Failed","reason":"s3 read failed"}}`, "", 1, false, false},
		{"MissingJobIsNotRetriedHere", `{"code":2100,"message":"import job does not exist, jobID=42"}`, "", 1, false, false},
		{"MissingJobInFailureReason", `{"code":0,"data":{"state":"Failed","reason":"import job does not exist, jobID=42"}}`, "", 1, false, false},
		{"InvalidEnvelope", `{}`, "", 1, true, false},
		{"CanceledQuery", `{"code":1}`, "", 1, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var calls int
				client := req.C().SetBaseURL("http://milvus.test")
				client.Transport.WrapRoundTripFunc(func(_ http.RoundTripper) req.HttpRoundTripFunc {
					return func(request *http.Request) (*http.Response, error) {
						calls++
						body := tc.first
						if calls > 1 {
							body = tc.next
						}
						if tc.cancel {
							cancel()
						}
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
					}
				})
				cli := &RestfulClient{cli: client}
				resp, err := cli.GetBulkInsertState(ctx, "db", "42")
				if tc.wantError {
					assert.Error(t, err)
					assert.Nil(t, resp)
				} else {
					require.NoError(t, err)
					require.NotNil(t, resp)
					var expected GetProcessResp
					body := tc.first
					if tc.calls > 1 {
						body = tc.next
					}
					require.NoError(t, json.Unmarshal([]byte(body), &expected))
					assert.Equal(t, expected, *resp)
				}
				assert.Equal(t, tc.calls, calls)
				if tc.cancel {
					assert.ErrorIs(t, err, context.Canceled)
				}
			})
		})
	}
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

	// The snapshot source contract composes with manual commit: source_type
	// alongside backup and auto_commit=false, with the external spec and
	// partition mapping; the binlog-only knobs stay out.
	t.Run("SnapshotSource", func(t *testing.T) {
		opts := (&BulkInsertV2Input{
			SourceType:       SourceTypeSnapshot,
			ExternalSpec:     `{"extfs":{"access_key_id":"ak"}}`,
			PartitionMapping: `{"p1":"p1"}`,
			ManualCommit:     true,
		}).opts()
		assert.Equal(t, "snapshot", opts["source_type"])
		assert.Equal(t, "true", opts["backup"])
		assert.Equal(t, "false", opts["auto_commit"])
		assert.Equal(t, `{"extfs":{"access_key_id":"ak"}}`, opts["external_spec"])
		assert.Equal(t, `{"p1":"p1"}`, opts["partition_mapping"])
		assert.NotContains(t, opts, "storage_version")
		assert.NotContains(t, opts, "l0_import")
		assert.NotContains(t, opts, "end_ts")
	})

	t.Run("SnapshotKnobsAbsentByDefault", func(t *testing.T) {
		opts := (&BulkInsertV2Input{}).opts()
		assert.NotContains(t, opts, "source_type")
		assert.NotContains(t, opts, "external_spec")
		assert.NotContains(t, opts, "partition_mapping")
	})
}

// The create request carries the source credentials and the ezk in its options;
// the logged view masks both rather than writing them into the log.
func TestCreateImportReqRedacted(t *testing.T) {
	req := createImportReq{Options: map[string]string{
		"backup":            "true",
		"source_type":       "snapshot",
		"external_spec":     `{"extfs":{"access_key_id":"ak","access_key_value":"sk"}}`,
		"ezk":               "secret-key",
		"partition_mapping": `{"p1":"p1"}`,
	}}

	masked := req.redacted()
	assert.Equal(t, "****", masked.Options["external_spec"])
	assert.Equal(t, "****", masked.Options["ezk"])
	assert.Equal(t, "snapshot", masked.Options["source_type"])
	// the request that goes on the wire keeps the real values
	assert.Contains(t, req.Options["external_spec"], "ak")
	assert.Equal(t, "secret-key", req.Options["ezk"])
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
