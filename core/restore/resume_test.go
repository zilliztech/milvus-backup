package restore

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
	"github.com/zilliztech/milvus-backup/internal/snapshotsource"
)

func TestRebuiltSnapshotRunsOnlyPostprocessing(t *testing.T) {
	store := jobstate.NewStore()
	persist := cfg.New().Restore.Persistence
	persist.Enabled.Val = true
	persist.Storage.Provider.Val, persist.Storage.RootPath.Val = "local", t.TempDir()
	store.ConfigurePersistence(persist)
	tracker, err := store.AddRestoreTask("job", true)
	require.NoError(t, err)
	target := collref.New("db", "target")
	meta := &backuppb.CollectionBackupInfo{CollectionName: "source", SnapshotBackup: &backuppb.SnapshotBackupInfo{MetadataPath: "bundle/snapshots/1/metadata/2.json"}}
	cli := milvus.NewMockGrpc(t)
	// Construct and run the first collection plan with a remote job that succeeds.
	original := &Task{args: TaskArgs{TaskID: "job", Params: cfg.New(), Option: &Option{}}, tracker: tracker, grpc: cli, logger: zap.NewNop(), snapshotSource: snapshotsource.Source{DirURI: "s3://backup/snap"}}
	original.args.Params.Restore.Concurrency.Collections.Val = 1
	original.collTargets = []collTarget{{target: target, collBackup: meta, collOverride: CollOverride{Description: "override"}}}
	plan := newRestoreSnapPlan(original, original.collTargets)
	tracker.SavePlan(original.savedPlan(plan))
	cli.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return(123, nil).Once()
	cli.EXPECT().GetRestoreSnapshotState(mock.Anything, int64(123)).Return(&milvuspb.RestoreSnapshotInfo{State: milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted}, nil).Once()
	cli.EXPECT().AlterCollection(mock.Anything, "db", "target", mock.Anything).Return(assert.AnError).Once()
	err = plan.Execute(t.Context())
	require.ErrorIs(t, err, assert.AnError)
	tracker.SetFail(err)

	store = jobstate.NewStore()
	store.ConfigurePersistence(persist)
	owner, saved, err := store.StartResume("job")
	require.NoError(t, err)
	fresh := &Task{args: TaskArgs{TaskID: "job", Params: cfg.New(), Option: &saved.Option}, tracker: owner, grpc: cli, logger: zap.NewNop(), snapshotSource: saved.SnapshotSource}
	require.NoError(t, fresh.rebuild(t.Context(), saved))
	cli.EXPECT().AlterCollection(mock.Anything, "db", "target", mock.Anything).Return(nil).Once()
	require.NoError(t, fresh.plan.Execute(t.Context()))
	got := owner.Finish(nil)
	assert.Equal(t, jobstate.RestoreStateSuccess, got.State)
	require.Len(t, got.Colls[target].Jobs, 1)
	assert.Equal(t, "123", got.Colls[target].Jobs[0].ID)
}

func TestCleanupFromStoredStaging(t *testing.T) {
	root := t.TempDir()
	dest := newTestStorageClient(t, cfg.ProviderLocal)
	store := jobstate.NewStore()
	tr, addErr := store.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	target := collref.New("db", "target")
	coll := tr.NewCollTracker(target, 1)
	job := coll.NewBinlogImport(restoreplan.BinlogImportBatch{}).Tracker
	args := importTaskArgs{taskID: "job", target: target, jobs: coll, staging: true, keepTempFiles: true, milvusStorage: dest, milvusRootPath: root, logger: zap.NewNop()}
	first := newImportCopyTask(args, job)
	_, err := first.Execute(t.Context(), nil)
	require.NoError(t, err)
	require.NotEmpty(t, job.StagingDir())
	rebuilt := newImportCopyTask(args, job)
	assert.Equal(t, first.tempDir, rebuilt.tempDir)
	dir := filepath.Join(root, job.StagingDir())
	require.NoError(t, os.MkdirAll(dir, 0o755))
	file := filepath.Join(dir, "log")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))
	job.SetCompleted()
	first.Cleanup(t.Context())
	require.FileExists(t, file)

	failed := coll.NewBinlogImport(restoreplan.BinlogImportBatch{}).Tracker
	failed.SetStagingDir("failed-batch/")
	failed.SetFailed(assert.AnError)
	failedDir := filepath.Join(root, failed.StagingDir())
	require.NoError(t, os.MkdirAll(failedDir, 0o755))
	failedFile := filepath.Join(failedDir, "partial-log")
	require.NoError(t, os.WriteFile(failedFile, []byte("partial"), 0o644))

	config := cfg.New()
	config.Restore.KeepTempFiles.Val = true
	config.Milvus.Storage.RootPath.Val = root
	config.Milvus.Storage.Provider.Val = cfg.ProviderLocal
	tr.SavePlan(&restoreplan.Plan{Milvus: config.Milvus, Restore: config.Restore, Collections: []restoreplan.Collection{{Target: target}}})
	tr.SetFail(assert.AnError)
	require.NoError(t, CleanupFailed(t.Context(), store, "job"))
	assert.NoFileExists(t, file)
	assert.Empty(t, job.StagingDir())
	assert.NoFileExists(t, failedFile)
	assert.Empty(t, failed.StagingDir())
	assert.Equal(t, jobstate.RestoreStateFailedCleaned, tr.Snapshot().State)
}

func TestRebuiltImportSnapUsesOriginalJob(t *testing.T) {
	original := newSelectPlanTask(t, "snapshot", StrategyImportSnap, true)
	persist := cfg.New().Restore.Persistence
	persist.Enabled.Val = true
	persist.Storage.Provider.Val, persist.Storage.RootPath.Val = "local", t.TempDir()
	store := jobstate.NewStore()
	store.ConfigurePersistence(persist)
	tracker, err := store.AddRestoreTask("job", true)
	require.NoError(t, err)
	original.tracker = tracker
	original.snapshotSource = snapshotsource.Source{DirURI: "s3://backup/snap"}
	original.collTargets = oneSnapCollTargets()
	original.collTargets[0].dbBackup.Ezk = "old"
	original.args.Option.EZKMapping = map[string]string{"old": "new", "new": "wrong"}
	plan, err := newImportSnapPlan(original, original.collTargets)
	require.NoError(t, err)
	job := original.tracker.TrackerForCollection(original.collTargets[0].target).TrackerForSingleJob()
	job.Bind("existing")
	saved := original.savedPlan(plan)
	tracker.SavePlan(saved)
	tracker.Finish(assert.AnError)
	cold := jobstate.NewStore()
	cold.ConfigurePersistence(persist)
	owner, saved, err := cold.StartResume("job")
	require.NoError(t, err)
	job = owner.TrackerForCollection(saved.Collections[0].Target).TrackerForSingleJob()
	endpoint := &snapshotImportEndpoint{states: []string{string(milvus.ImportStateCompleted)}}
	fresh := &Task{args: TaskArgs{Option: &saved.Option}, tracker: owner, restful: endpoint, logger: zap.NewNop(), snapshotSource: saved.SnapshotSource}
	require.NoError(t, fresh.rebuild(t.Context(), saved))
	imp := fresh.plan.(*importSnapPlan).colls[0].imp.(*snapshotImportTask)
	assert.Equal(t, "new", imp.importer.ezk)
	require.NoError(t, imp.Execute(t.Context()))
	id, done := job.Current()
	assert.Equal(t, "existing", id)
	assert.True(t, done)
	assert.Empty(t, endpoint.gotBulkInsert.Paths)
	assert.Len(t, owner.Snapshot().Colls[saved.Collections[0].Target].Jobs, 1)
}

func TestRebuiltBinlogDDLVerifiesFieldIDsAfterSegmentsAreOmitted(t *testing.T) {
	task := newSelectPlanTask(t, meta.FormatBinlog, StrategyMergedSeg, true)
	task.args.Params.Milvus.Storage.Provider.Val = cfg.ProviderLocal
	task.args.Params.Backup.Storage.Provider.Val = cfg.ProviderLocal
	task.collTargets = oneCollTargets()
	coll := task.collTargets[0]
	coll.collBackup.Schema = &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{{Name: "id", FieldID: 100}}}
	task.tracker.NewCollTracker(coll.target, 0)
	saved := task.savedPlan(&binlogPlan{strategy: StrategyMergedSeg})
	require.False(t, backupHasData(saved.Collections[0].Metadata))
	cli := milvus.NewMockGrpc(t)
	task.grpc = cli
	require.NoError(t, task.rebuild(t.Context(), saved))
	ddl := task.plan.(*binlogPlan).colls[0].ddl.(*collDDLTask)
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{}, nil).Once()
	assert.ErrorContains(t, ddl.checkFieldIDs(t.Context()), "missing from the target")
}

func TestResumeTwoPhaseImportFromStore(t *testing.T) {
	for _, format := range []string{meta.FormatBinlog, meta.FormatSnapshot} {
		t.Run(format, func(t *testing.T) { testColdResumeTwoPhaseImport(t, format) })
	}
}

func testColdResumeTwoPhaseImport(t *testing.T, format string) {
	var mirrorAvailable atomic.Bool
	var mirrorReads, primaryReads atomic.Int32
	endpoint := func(secondary bool) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/v2/vectordb/jobs/import/describe", r.URL.Path)
			var query milvus.GetProgressReq
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&query))
			assert.Equal(t, "job1", query.JobId)
			assert.Equal(t, "db", query.DBName)
			w.Header().Set("Content-Type", "application/json")
			if secondary {
				assert.Equal(t, "Bearer mirror-token", r.Header.Get("Authorization"))
				mirrorReads.Add(1)
				if !mirrorAvailable.Load() {
					// A malformed envelope is not retried; this test exercises
					// saved commit observations across executions.
					_, _ = w.Write([]byte(`{"message":"injected invalid query response"}`))
					return
				}
			} else if primaryReads.Add(1) > 1 {
				_, _ = w.Write([]byte(`{"code":2100,"message":"import job does not exist, jobID=job1"}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"state":"Completed"}}`))
		}))
		t.Cleanup(server.Close)
		return server
	}
	primary, mirror := endpoint(false), endpoint(true)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(server, &resumeMilvusServer{})
	t.Cleanup(server.Stop)
	go server.Serve(listener)

	store := jobstate.NewStore()
	original := newSelectPlanTask(t, format, StrategyAuto, true)
	config := cfg.New()
	config.Restore.Persistence.Enabled.Val = true
	config.Restore.Persistence.Storage.Provider.Val, config.Restore.Persistence.Storage.RootPath.Val = "local", t.TempDir()
	store.ConfigurePersistence(config.Restore.Persistence)
	original.tracker, err = store.AddRestoreTask("job", true)
	require.NoError(t, err)
	config.Milvus.Grpc.Address.Val = "127.0.0.1"
	config.Milvus.Grpc.Port.Val = listener.Addr().(*net.TCPAddr).Port
	config.Milvus.Grpc.TLSMode.Val = cfg.TLSDisabled
	config.Milvus.Rest.Endpoint.Val = primary.URL
	config.Milvus.Storage.Provider.Val = cfg.ProviderLocal
	config.Backup.Storage.Provider.Val = cfg.ProviderLocal
	config.Restore.MaxSegmentsPerImportJob.Val = 16
	config.Restore.Concurrency.Collections.Val = 1
	config.Restore.Concurrency.ImportJobs.Val = 1
	original.args.Params = config
	original.args.Option.Secondaries = []Secondary{{Address: mirror.URL, Token: "mirror-token"}}
	original.args.Option.SkipCreateCollection = true
	var events []string
	var mu sync.Mutex
	local := newTwoPCCluster("primary", &events, &mu, string(milvus.ImportStateUncommitted))
	local.commitErr = assert.AnError
	original.restful = local
	original.bulkInsertSem = semaphore.NewWeighted(1)
	original.secondaries = []importSecondary{{addr: mirror.URL, cli: newTwoPCCluster("secondary", &events, &mu, string(milvus.ImportStateUncommitted))}}
	original.collTargets = oneCollTargets()
	cli := milvus.NewMockGrpc(t)
	if format == meta.FormatSnapshot {
		original.collTargets = oneSnapCollTargets()
		original.snapshotSource = snapshotsource.Source{DirURI: "s3://backup/snap", SameService: true}
		cli.EXPECT().HasFeature(milvus.Snapshot).Return(true)
		cli.EXPECT().HasFeature(milvus.ImportSnapshot).Return(true)
	} else {
		cli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(true)
	}
	original.grpc = cli
	cli.EXPECT().HasFeature(milvus.Import2PC).Return(true)
	cli.EXPECT().GetReplicateConfiguration(mock.Anything).Return(&commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{{ClusterId: "primary"}, {ClusterId: "secondary"}},
	}, nil)
	cli.EXPECT().ListDatabases(mock.Anything).Return([]string{"db"}, nil).Twice()
	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(true, nil).Once()
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 123}, nil).Twice()
	cli.EXPECT().HasPartition(mock.Anything, "db", "coll", "p1").Return(true, nil).Once()
	require.True(t, original.tracker.Start())
	require.NoError(t, original.prepare(t.Context()))
	err = original.plan.Execute(t.Context())
	require.ErrorIs(t, err, assert.AnError)
	assert.True(t, original.tracker.Finish(err).EnableResume)
	target := original.collTargets[0].target
	require.Len(t, original.tracker.Snapshot().Colls[target].Jobs, 1)
	original.args.Option.Secondaries[0].Address = primary.URL

	store = jobstate.NewStore()
	store.ConfigurePersistence(config.Restore.Persistence)
	err = Resume(t.Context(), store, "job")
	require.NoError(t, err)
	first := waitForResumedTask(t, store, "job")
	assert.Equal(t, jobstate.RestoreStateFail, first.State)
	assert.Contains(t, first.ErrorMessage, mirror.URL)
	mirrorAvailable.Store(true)
	store = jobstate.NewStore()
	store.ConfigurePersistence(config.Restore.Persistence)
	err = Resume(t.Context(), store, "job")
	require.NoError(t, err)
	final := waitForResumedTask(t, store, "job")
	assert.Equal(t, jobstate.RestoreStateSuccess, final.State)
	assert.EqualValues(t, 2, mirrorReads.Load())
	assert.EqualValues(t, 1, primaryReads.Load())
	require.Len(t, final.Colls[target].Jobs, 1)
	assert.Equal(t, "job1", final.Colls[target].Jobs[0].ID)
	assert.Equal(t, jobstate.RestoreJobCompleted, final.Colls[target].Jobs[0].State)
}

func TestCleanupFailedSecondaryFromStore(t *testing.T) {
	config := cfg.New()
	config.Restore.KeepTempFiles.Val = true
	config.Milvus.Storage.Provider.Val = cfg.ProviderLocal
	config.Milvus.Storage.RootPath.Val = t.TempDir()
	store := jobstate.NewStore()
	tracker, addErr := store.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	plan := &restoreplan.Plan{Milvus: config.Milvus, Restore: config.Restore, Secondary: &restoreplan.Secondary{
		Collections: map[int64]*restoreplan.SecondaryCollection{1: {StageBinlogs: true}, 2: {StageBinlogs: true}},
	}}
	progress := jobstate.NewSecondaryProgress(plan.Secondary)
	tracker.SaveSecondaryPlan(plan, progress)
	coll := progress.Collections[1]
	coll.StagingDir = "restore-temp-job-db-first/"
	progress.Collections[2].StagingDir = "restore-temp-job-db-second/"
	dir := filepath.Join(config.Milvus.Storage.RootPath.Val, coll.StagingDir)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	file := filepath.Join(dir, "binlog")
	require.NoError(t, os.WriteFile(file, []byte("data"), 0o600))
	// A regular file at the second directory prefix makes listing it fail.
	remaining := progress.Collections[2].StagingDir
	blocker := filepath.Join(config.Milvus.Storage.RootPath.Val, remaining)
	require.NoError(t, os.WriteFile(blocker, []byte("blocked"), 0o600))
	tracker.SetFail(assert.AnError)

	require.Error(t, CleanupFailed(t.Context(), store, "job"))
	assert.NoFileExists(t, file)
	assert.Empty(t, coll.StagingDir)
	assert.Equal(t, remaining, progress.Collections[2].StagingDir)
	assert.Same(t, progress, tracker.SecondaryProgress())
	assert.Equal(t, jobstate.RestoreStateFailedCleanupPending, tracker.Snapshot().State)
	assert.ErrorIs(t, Resume(t.Context(), store, "job"), jobstate.ErrRestoreConflict)

	require.NoError(t, os.Remove(blocker))
	require.NoError(t, os.MkdirAll(blocker, 0o755))
	file = filepath.Join(blocker, "binlog")
	require.NoError(t, os.WriteFile(file, []byte("data"), 0o600))
	require.NoError(t, CleanupFailed(t.Context(), store, "job"))
	assert.NoFileExists(t, file)
	assert.Empty(t, progress.Collections[2].StagingDir)
	assert.Nil(t, tracker.SecondaryProgress())
	assert.Equal(t, jobstate.RestoreStateFailedCleaned, tracker.Snapshot().State)
	assert.Error(t, Resume(t.Context(), store, "job"))
}

func waitForResumedTask(t *testing.T, store *jobstate.Store, id string) jobstate.RestoreStatus {
	t.Helper()
	var status jobstate.RestoreStatus
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var err error
		status, err = store.GetRestoreTask(id)
		require.NoError(c, err)
		assert.True(c, status.Terminal())
	}, 5*time.Second, time.Millisecond)
	return status
}

type blockedResumeServer struct {
	milvuspb.UnimplementedMilvusServiceServer
	entered chan struct{}
	release chan struct{}
}

func (s *blockedResumeServer) Connect(ctx context.Context, _ *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return &milvuspb.ConnectResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "injected execution failure"}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestResumeReturnsBeforeBackgroundFailure(t *testing.T) {
	for _, name := range []string{"Ordinary", "Secondary"} {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			endpoint := &blockedResumeServer{entered: make(chan struct{}, 1), release: make(chan struct{})}
			server := grpc.NewServer()
			milvuspb.RegisterMilvusServiceServer(server, endpoint)
			t.Cleanup(server.Stop)
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(endpoint.release) }) })
			go server.Serve(listener)

			config := cfg.New()
			config.Milvus.Grpc.Address.Val = "127.0.0.1"
			config.Milvus.Grpc.Port.Val = listener.Addr().(*net.TCPAddr).Port
			config.Milvus.Grpc.TLSMode.Val = cfg.TLSDisabled
			plan := &restoreplan.Plan{Milvus: config.Milvus}
			if name == "Secondary" {
				plan.Secondary = &restoreplan.Secondary{Backup: &backuppb.BackupInfo{Format: "snapshot"}}
			}
			store := jobstate.NewStore()
			tracker, err := store.AddRestoreTask("job", true)
			require.NoError(t, err)
			tracker.SavePlan(plan)
			tracker.Finish(assert.AnError)

			returned := make(chan error, 1)
			go func() { returned <- Resume(t.Context(), store, "job") }()
			require.Eventually(t, func() bool { return len(returned) == 1 }, 5*time.Second, time.Millisecond)
			require.NoError(t, <-returned, "Resume must not wait for the executor")
			require.Eventually(t, func() bool { return len(endpoint.entered) == 1 }, 5*time.Second, time.Millisecond)
			status, err := store.GetRestoreTask("job")
			require.NoError(t, err)
			assert.Equal(t, jobstate.RestoreStateExecuting, status.State)
			assert.Empty(t, status.ErrorMessage)

			release.Do(func() { close(endpoint.release) })
			status = waitForResumedTask(t, store, "job")
			assert.Equal(t, jobstate.RestoreStateFail, status.State)
			assert.Contains(t, status.ErrorMessage, "injected execution failure")
		})
	}
}

func TestResumeReturnsStainFailureWithoutStarting(t *testing.T) {
	config := cfg.New().Restore.Persistence
	config.Enabled.Val = true
	config.Storage.Provider.Val = "local"
	config.Storage.RootPath.Val = filepath.Join(t.TempDir(), "blocked")
	require.NoError(t, os.WriteFile(config.Storage.RootPath.Val, []byte("not a directory"), 0600))
	store := jobstate.NewStore()
	store.ConfigurePersistence(config)
	tracker, err := store.AddRestoreTask("job", true)
	require.NoError(t, err)
	tracker.SavePlan(&restoreplan.Plan{})
	tracker.Finish(assert.AnError)

	assert.ErrorIs(t, Resume(t.Context(), store, "job"), jobstate.ErrPersistence)
	status, err := store.GetRestoreTask("job")
	require.NoError(t, err)
	assert.Equal(t, jobstate.RestoreStateFail, status.State)
	assert.Equal(t, assert.AnError.Error(), status.ErrorMessage)
}

func TestColdCleanupRejectsInvalidStagingBeforeDeletingFiles(t *testing.T) {
	config := cfg.New()
	config.Milvus.Storage.Provider.Val = cfg.ProviderLocal
	root := t.TempDir()
	config.Milvus.Storage.RootPath.Val = root
	persist := config.Restore.Persistence
	persist.Enabled.Val = true
	persist.Storage.Provider.Val = "local"
	persist.Storage.RootPath.Val = t.TempDir()
	store := jobstate.NewStore()
	store.ConfigurePersistence(persist)
	tr, err := store.AddRestoreTask("job", true)
	require.NoError(t, err)
	target := collref.New("db", "coll")
	coll := tr.NewCollTracker(target, 2)
	tr.SavePlan(&restoreplan.Plan{
		Milvus: config.Milvus, Strategy: StrategyPerSeg,
		Collections: []restoreplan.Collection{{Target: target, Metadata: &backuppb.CollectionBackupInfo{}}},
	})
	staging := newCopyTask(importTaskArgs{taskID: "job", target: target, staging: true, jobs: coll})
	dirs := []string{staging.tempDir, "insert_log/"}
	for _, dir := range dirs {
		job := coll.NewBinlogImport(restoreplan.BinlogImportBatch{PartitionName: "p"})
		job.Tracker.SetStagingDir(dir)
		require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0700))
		require.NoError(t, os.WriteFile(filepath.Join(root, dir, "data"), []byte("data"), 0600))
	}
	tr.SetFail(assert.AnError)

	cold := jobstate.NewStore()
	cold.ConfigurePersistence(persist)
	require.ErrorContains(t, CleanupFailed(t.Context(), cold, "job"), `invalid staging directory "insert_log/"`)
	for _, dir := range dirs {
		assert.FileExists(t, filepath.Join(root, dir, "data"))
	}
}
