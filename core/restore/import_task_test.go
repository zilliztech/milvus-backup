package restore

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestGetFailedReason(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "failed_reason", Value: "hello"}})
		assert.Equal(t, "hello", r)
	})

	t.Run("WithoutFailedReason", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, "", r)
	})
}

func TestGetProcess(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "progress_percent", Value: "100"}})
		assert.Equal(t, 100, r)
	})

	t.Run("WithoutProgress", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, 0, r)
	})
}

func TestStartImport(t *testing.T) {
	for _, tc := range []struct {
		name        string
		id          string
		localDone   bool
		state       importJobState
		lookupErr   error
		wantID      string
		wantDone    bool
		wantErr     error
		wantQueries int
		wantSubmits int
	}{
		{name: "NewJob", wantID: "new", wantSubmits: 1},
		{name: "LocallyCompleted", id: "old", localDone: true, wantID: "old", wantDone: true},
		{name: "RemotelyCompleted", id: "old", state: importJobState{completed: true}, wantID: "old", wantDone: true, wantQueries: 1},
		{name: "AdoptRunning", id: "old", state: importJobState{progress: 40}, wantID: "old", wantQueries: 1},
		{name: "ReplaceFailed", id: "old", lookupErr: milvus.ErrRemoteJobFailed, wantID: "new", wantQueries: 1, wantSubmits: 1},
		{name: "KeepMissing", id: "old", lookupErr: milvus.ErrImportJobNotFound, wantID: "old", wantQueries: 1, wantErr: milvus.ErrImportJobNotFound},
		{name: "KeepOnQueryError", id: "old", lookupErr: assert.AnError, wantID: "old", wantErr: assert.AnError, wantQueries: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("task", true)
			require.NoError(t, addErr)
			coll := restoreTracker.NewCollTracker(collref.New("db", "coll"), 1)
			job := coll.NewJob(1)
			job.Bind(tc.id)
			if tc.localDone {
				job.SetCompleted()
			}
			var queries, submits int
			state := func(_ context.Context, id string) (importJobState, error) {
				queries++
				assert.Equal(t, "old", id)
				return tc.state, tc.lookupErr
			}
			submit := func(context.Context) (string, error) {
				submits++
				return "new", nil
			}
			id, done, err := startImport(t.Context(), job, submit, state)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.Equal(t, tc.wantID, id)
			assert.Equal(t, tc.wantDone, done)
			assert.Equal(t, tc.wantQueries, queries)
			assert.Equal(t, tc.wantSubmits, submits)
			storedID, _ := job.Current()
			assert.Equal(t, tc.wantID, storedID)
		})
	}
}

func TestStartImportRetainsIDWhenSubmissionIsInterrupted(t *testing.T) {
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("task", true)
	require.NoError(t, addErr)
	coll := restoreTracker.NewCollTracker(collref.New("db", "coll"), 1)
	job := coll.NewJob(1)
	job.Bind("old")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	state := func(context.Context, string) (importJobState, error) {
		return importJobState{}, milvus.ErrRemoteJobFailed
	}
	submit := func(context.Context) (string, error) {
		cancel()
		return "new", nil
	}
	_, _, err := startImport(ctx, job, submit, state)
	require.ErrorIs(t, err, context.Canceled)
	id, done := job.Current()
	assert.Equal(t, "new", id)
	assert.False(t, done)

	// The next execution adopts the recorded ID rather than submitting again.
	state = func(_ context.Context, id string) (importJobState, error) {
		assert.Equal(t, "new", id)
		return importJobState{completed: true}, nil
	}
	submit = func(context.Context) (string, error) {
		assert.Fail(t, "must adopt the recorded submission")
		return "", assert.AnError
	}
	id, done, err = startImport(t.Context(), job, submit, state)
	assert.NoError(t, err)
	assert.Equal(t, "new", id)
	assert.True(t, done)
}

func TestSubmitImportContext(t *testing.T) {
	for name, enableResume := range map[string]bool{"Default": false, "Resumable": true} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_, err := submitImport(ctx, enableResume, func(sub context.Context) (int, error) {
				cancel()
				if enableResume {
					_, ok := sub.Deadline()
					assert.True(t, ok)
					assert.NoError(t, sub.Err())
				} else {
					assert.Same(t, ctx, sub)
					assert.ErrorIs(t, sub.Err(), context.Canceled)
				}
				return 0, nil
			})
			assert.NoError(t, err)
		})
	}
}
