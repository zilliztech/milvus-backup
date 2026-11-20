package jobstate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

func TestResumeUsesCurrentState(t *testing.T) {
	s := NewStore()
	tr, addErr := s.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	// Planning may have registered some progress slots before encountering an error.
	coll := tr.NewCollTracker(collref.New("db", "partial"), 1)
	coll.NewBinlogImport(restoreplan.BinlogImportBatch{})
	tr.SetFail(assert.AnError)
	assert.True(t, tr.Snapshot().Resumable)
	assert.Empty(t, coll.TrackersForBinlogImports())
	_, _, err := s.StartResume("job")
	require.ErrorIs(t, err, ErrRestoreConflict)
	plan := &restoreplan.Plan{BackupDir: "backup/"}
	tr.SavePlan(plan)
	owner, got, err := s.StartResume("job")
	require.NoError(t, err)
	assert.Equal(t, plan, got)
	_, _, err = s.StartResume("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	_, _, err = s.StartCleanup("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	owner.SetSuccess()
	assert.True(t, owner.Snapshot().Resumable)
	assert.Nil(t, tr.task.plan)
}

func TestSetFailWithoutErrorKeepsRestoreResumable(t *testing.T) {
	store := NewStore()
	tr, addErr := store.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	require.True(t, tr.Start())
	target := collref.New("db", "coll")
	coll := tr.NewCollTracker(target, 1)
	batch := restoreplan.BinlogImportBatch{Dirs: []restoreplan.Directory{{Insert: "binlogs/", Size: 1}}}
	coll.NewBinlogImport(batch)
	tr.SavePlan(&restoreplan.Plan{Collections: []restoreplan.Collection{{Target: target}}})

	tr.SetFail(nil)
	status := tr.Snapshot()
	assert.Equal(t, RestoreStateFail, status.State)
	assert.NotEmpty(t, status.ErrorMessage)
	assert.True(t, status.Resumable)

	owner, _, err := store.StartResume("job")
	require.NoError(t, err)
	imports := owner.TrackerForCollection(target).TrackersForBinlogImports()
	require.Len(t, imports, 1)
	assert.Equal(t, batch, imports[0].Batch)
}

func TestFailedCleanupExcludesResumeAndRetries(t *testing.T) {
	s := NewStore()
	tr, addErr := s.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	target := collref.New("db", "coll")
	coll := tr.NewCollTracker(target, 1)
	batch := restoreplan.BinlogImportBatch{Dirs: []restoreplan.Directory{{Insert: "binlogs/", Size: 1}}}
	coll.NewBinlogImport(batch).Tracker.SetStagingDir("restore-temp-job/")
	tr.SavePlan(&restoreplan.Plan{Collections: []restoreplan.Collection{{Target: target}}})
	tr.SetFail(assert.AnError)
	owner, _, err := s.StartCleanup("job")
	require.NoError(t, err)
	_, _, err = s.StartCleanup("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	owner.FinishCleanup(assert.AnError)
	assert.Equal(t, RestoreStateFailedCleanupPending, tr.Snapshot().State)
	assert.Equal(t, assert.AnError.Error(), tr.Snapshot().ErrorMessage)
	_, _, err = s.StartResume("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	owner, _, err = s.StartCleanup("job")
	require.NoError(t, err)
	imports := owner.TrackerForCollection(target).TrackersForBinlogImports()
	require.Len(t, imports, 1)
	assert.Equal(t, batch, imports[0].Batch)
	assert.Equal(t, "restore-temp-job/", imports[0].Tracker.StagingDir())
	owner.FinishCleanup(nil)
	assert.Empty(t, coll.TrackersForBinlogImports())
	owner, _, err = s.StartCleanup("job")
	assert.NoError(t, err)
	assert.Nil(t, owner)
	assert.Equal(t, RestoreStateFailedCleaned, tr.Snapshot().State)
	assert.Nil(t, tr.task.plan)
}

func TestConcurrentResumeAndCleanup(t *testing.T) {
	store := NewStore()
	tr, addErr := store.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	tr.SavePlan(&restoreplan.Plan{})
	tr.SetFail(assert.AnError)
	start := make(chan struct{})
	result := make(chan error, 2)
	go func() { <-start; _, _, err := store.StartResume("job"); result <- err }()
	go func() { <-start; _, _, err := store.StartCleanup("job"); result <- err }()
	close(start)
	a, b := <-result, <-result
	assert.True(t, (a == nil && errors.Is(b, ErrRestoreConflict)) || (b == nil && errors.Is(a, ErrRestoreConflict)))
}

func TestTwoPCCompletionBelongsToBoundJobID(t *testing.T) {
	restoreTracker, addErr := NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	coll := restoreTracker.NewCollTracker(collref.New("db", "target"), 0)
	job := coll.NewBinlogImport(restoreplan.BinlogImportBatch{}).Tracker
	job.Bind("first")
	job.MarkPrimaryCompleted()
	job.MarkMirrorCompleted("secondary")
	job.Bind("first")
	assert.True(t, job.PrimaryCompleted())
	assert.True(t, job.MirrorCompleted("secondary"))
	job.Bind("second")
	assert.False(t, job.PrimaryCompleted())
	assert.False(t, job.MirrorCompleted("secondary"))
	assert.False(t, job.CommitRequested())
}
