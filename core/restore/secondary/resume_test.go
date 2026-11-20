package secondary

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
	"github.com/zilliztech/milvus-backup/internal/snapshotbundle"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

const resumePch = "src-rootcoord-dml_0"

type resumeTarget struct {
	preflightServer
	failFinalVerify bool
	version         string
}

func (s *resumeTarget) Connect(context.Context, *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	version := s.version
	if version == "" {
		version = "3.0.6"
	}
	return &milvuspb.ConnectResponse{Status: &commonpb.Status{}, ServerInfo: &commonpb.ServerInfo{BuildTags: version}}, nil
}

func (s *resumeTarget) GetReplicateInfo(context.Context, *milvuspb.GetReplicateInfoRequest) (*milvuspb.GetReplicateInfoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tick uint64
	if len(s.ticks) > 0 {
		tick = s.ticks[len(s.ticks)-1]
	}
	return &milvuspb.GetReplicateInfoResponse{Checkpoint: &commonpb.ReplicateCheckpoint{ClusterId: "src", Pchannel: resumePch, TimeTick: tick}}, nil
}

func (s *resumeTarget) HasCollection(context.Context, *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failFinalVerify && len(s.ticks) > 0 && s.ticks[len(s.ticks)-1] == 100 {
		s.failFinalVerify = false
		return nil, fmt.Errorf("injected final verification failure")
	}
	return &milvuspb.BoolResponse{Status: &commonpb.Status{}, Value: len(s.ticks) > 0}, nil
}

func (s *resumeTarget) DescribeCollection(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{Status: &commonpb.Status{}, CollectionID: 123}, nil
}

// Bundle bytes are local fixtures; the source URI advertised to Milvus is S3.
type bundleSource struct{ storage.Client }

func (b bundleSource) Config() storage.Config {
	return storage.Config{Provider: cfg.ProviderMinio, Bucket: "backup", Endpoint: "localhost:9000"}
}

func TestSecondaryChannelMappingRequiresResume(t *testing.T) {
	flush := message.NewFlushAllMessageBuilderV2().WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).
		WithBroadcast([]string{resumePch + "_vcchan"}).MustBuildBroadcast().WithBroadcastID(77).SplitIntoMutableMessage()[0].
		WithTimeTick(100).WithLastConfirmed(milvus.NewFakeMessageID(100, message.WALNameKafka)).IntoImmutableMessage(milvus.NewFakeMessageID(100, message.WALNameKafka)).IntoImmutableMessageProto()
	raw, err := proto.Marshal(flush)
	require.NoError(t, err)
	for name, resumable := range map[string]bool{"Default": false, "Resumable": true} {
		t.Run(name, func(t *testing.T) {
			task, err := NewTask(TaskArgs{TaskID: "job", Resumable: resumable, Store: jobstate.NewStore(), Params: cfg.New(), Backup: &backuppb.BackupInfo{Format: meta.FormatBinlog, PhysicalChannelNames: []string{resumePch}, FlushAllMsgsBase64: map[string]string{resumePch: base64.StdEncoding.EncodeToString(raw)}}})
			require.NoError(t, err)
			// The target has a checkpoint but omits its source channel name.
			task.targetChannels = map[string]string{"target": ""}
			err = task.savePlan(t.Context())
			if resumable {
				assert.ErrorContains(t, err, "source channel mapping")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSecondaryResumeImports(t *testing.T) {
	interval := _bulkInsertCheckInterval
	_bulkInsertCheckInterval = 10 * time.Millisecond
	t.Cleanup(func() { _bulkInsertCheckInterval = interval })

	for _, tc := range []struct {
		name, format     string
		first, second    []string
		replace, success bool
		version          string
	}{
		{"BinlogMismatchedID", meta.FormatBinlog, []string{"query-error"}, []string{"wrong-job"}, false, false, ""},
		{"SnapshotMismatchedID", meta.FormatSnapshot, []string{"query-error"}, []string{"wrong-job"}, false, false, ""},
		{"BinlogWithoutResume", meta.FormatBinlog, []string{"Failed"}, nil, false, false, ""},
		{"SnapshotFinalVerification", meta.FormatSnapshot, []string{"Completed"}, nil, false, true, ""},
		{"BinlogLostPoll", meta.FormatBinlog, []string{"query-error"}, []string{"Completed"}, false, true, ""},
		{"BinlogPreCommitFailure", meta.FormatBinlog, []string{"Failed"}, []string{"Uncommitted", "Completed"}, true, true, ""},
		{"BinlogGCReportedAsFailed", meta.FormatBinlog, []string{"query-error"}, []string{"missing-in-reason", "Completed"}, false, true, ""},
		{"SnapshotJobNotYetVisible", meta.FormatSnapshot, []string{"query-error"}, []string{"not-found", "Completed"}, false, true, ""},
		{"SnapshotPreCommitFailure", meta.FormatSnapshot, []string{"Failed"}, []string{"Uncommitted", "Completed"}, true, true, ""},
		{"SnapshotAdoptedPreCommitFailure", meta.FormatSnapshot, []string{"query-error"}, []string{"Failed", "Uncommitted", "Completed"}, true, true, ""},
		{"SnapshotReplacementFailsAgain", meta.FormatSnapshot, []string{"query-error"}, []string{"Failed", "Failed"}, true, false, ""},
		{"BinlogLostCommitResponse", meta.FormatBinlog, []string{"Uncommitted", "query-error"}, []string{"Completed"}, false, true, ""},
		{"BinlogObservedCommitting", meta.FormatBinlog, []string{"Committing", "query-error"}, []string{"Failed", "Uncommitted", "Completed"}, true, true, ""},
		{"SnapshotPostCommitFailure", meta.FormatSnapshot, []string{"Uncommitted", "Failed"}, []string{"Uncommitted", "Completed"}, true, true, ""},
		{"BinlogPostCommitFailure", meta.FormatBinlog, []string{"Uncommitted", "Failed"}, []string{"Uncommitted", "Completed"}, true, true, ""},
		{"BinlogAdoptedFailure", meta.FormatBinlog, []string{"query-error"}, []string{"Failed", "Completed"}, true, true, ""},
		{"BinlogPendingThenFailed", meta.FormatBinlog, []string{"query-error"}, []string{"Pending", "Failed"}, false, false, ""},
		{"BinlogImportingThenFailed", meta.FormatBinlog, []string{"query-error"}, []string{"Importing", "Failed"}, false, false, ""},
		{"SnapshotUncommittedThenFailed", meta.FormatSnapshot, []string{"query-error"}, []string{"Uncommitted", "Failed"}, false, false, ""},
		{"SnapshotCommittingThenFailed", meta.FormatSnapshot, []string{"query-error"}, []string{"Committing", "Failed"}, false, false, ""},
		{"Binlog26Failed", meta.FormatBinlog, []string{"Failed"}, []string{"Completed"}, true, true, "2.6.27"},
		{"Binlog26AdoptedFailure", meta.FormatBinlog, []string{"query-error"}, []string{"Failed", "Completed"}, true, true, "2.6.27"},
		{"Binlog26ReplacementFails", meta.FormatBinlog, []string{"Failed"}, []string{"Failed"}, true, false, "2.6.27"},
		{"Binlog26ImportingThenFailed", meta.FormatBinlog, []string{"query-error"}, []string{"Importing", "Failed"}, false, false, "2.6.27"},
		{"Binlog26LostPoll", meta.FormatBinlog, []string{"query-error"}, []string{"Completed"}, false, true, "2.6.27"},
		{"Binlog26MissingJob", meta.FormatBinlog, []string{"query-error"}, []string{"missing-in-reason", "Completed"}, false, true, "2.6.27"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			srv := &resumeTarget{failFinalVerify: tc.second == nil && tc.success, version: tc.version}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			gs := grpc.NewServer()
			milvuspb.RegisterMilvusServiceServer(gs, srv)
			t.Cleanup(gs.Stop)
			go gs.Serve(listener)
			var mu sync.Mutex
			var staged string
			states := append(append([]string{}, tc.first...), tc.second...)
			var queried []string
			rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				var req milvus.GetProgressReq
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
					return
				}
				queried = append(queried, req.JobId)
				w.Header().Set("Content-Type", "application/json")
				if len(states) == 0 {
					assert.Fail(t, "unexpected job query")
					fmt.Fprint(w, `{"message":"injected invalid query response"}`)
					return
				}
				state := states[0]
				states = states[1:]
				// Read the file while the resumed import is in progress, before cleanup.
				if staged != "" && len(queried) > len(tc.first) {
					data, err := os.ReadFile(staged)
					if assert.NoError(t, err) {
						if req.JobId != queried[0] {
							assert.Equal(t, "binlog", string(data))
						} else {
							assert.Equal(t, "broken", string(data))
						}
					}
				}
				switch state {
				case "wrong-job":
					fmt.Fprint(w, `{"code":0,"data":{"jobId":"other","state":"Failed"}}`)
				case "query-error":
					fmt.Fprint(w, `{"message":"injected invalid query response"}`)
				case "not-found":
					fmt.Fprintf(w, `{"code":2100,"message":"import job does not exist, jobID=%s"}`, req.JobId)
				case "missing-in-reason":
					fmt.Fprintf(w, `{"code":0,"data":{"state":"Failed","reason":"import job does not exist, jobID=%s"}}`, req.JobId)
				default:
					fmt.Fprintf(w, `{"code":0,"data":{"state":%q}}`, state)
				}
			}))
			t.Cleanup(rs.Close)
			c := cfg.New()
			c.Restore.KeepTempFiles.Val = true // Independent secondary staging follows import progress.
			c.Restore.Persistence.Enabled.Val = true
			c.Restore.Persistence.Type.Val = "local"
			c.Restore.Persistence.Local.Directory.Val = t.TempDir()
			c.Milvus.Grpc.Address.Val = "127.0.0.1"
			c.Milvus.Grpc.Port.Val = listener.Addr().(*net.TCPAddr).Port
			c.Milvus.Grpc.TLSMode.Val = cfg.TLSDisabled
			c.Milvus.Rest.Endpoint.Val = rs.URL
			c.Restore.Concurrency.Collections.Val = 1
			c.Transfer.Concurrency.Val = 1
			c.Backup.Storage.Provider.Val = cfg.ProviderLocal
			c.Milvus.Storage.Provider.Val = cfg.ProviderLocal
			root := t.TempDir()
			local, err := storage.NewClient(ctx, storage.Config{Provider: cfg.ProviderLocal})
			require.NoError(t, err)
			src := local
			var binlogPath string
			vch := resumePch + "_123v0"
			coll := newTestCollBackup([]string{vch}, []*backuppb.PartitionBackupInfo{{PartitionId: 10, PartitionName: "p", SegmentBackups: []*backuppb.SegmentBackupInfo{{SegmentId: 20, PartitionId: 10, VChannel: vch, Size: 1}}}}, nil)
			if tc.format == meta.FormatSnapshot {
				coll.SnapshotBackup = &backuppb.SnapshotBackupInfo{MetadataPath: writeTestBundle(t, snapshotbundle.NewStorageStore(local), root, 123, 3, true)}
				src = bundleSource{local}
			} else {
				c.Transfer.Mode.Val = cfg.TransferStreaming
				c.Milvus.Storage.RootPath.Val = t.TempDir()
				dir := mpath.BackupInsertLogDir(root, mpath.CollectionID(123), mpath.PartitionID(10), mpath.GroupID(0))
				require.NoError(t, os.MkdirAll(dir, 0o755))
				binlogPath = filepath.Join(dir, "log")
				require.NoError(t, os.WriteFile(binlogPath, []byte("binlog"), 0o600))
			}
			flush := message.NewFlushAllMessageBuilderV2().WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).
				WithBroadcast([]string{resumePch + "_vcchan"}).MustBuildBroadcast().WithBroadcastID(77).SplitIntoMutableMessage()[0].
				WithTimeTick(100).WithLastConfirmed(milvus.NewFakeMessageID(100, message.WALNameKafka)).IntoImmutableMessage(milvus.NewFakeMessageID(100, message.WALNameKafka)).IntoImmutableMessageProto()
			raw, err := proto.Marshal(flush)
			require.NoError(t, err)
			backup := &backuppb.BackupInfo{Format: tc.format, Name: "backup", PhysicalChannelNames: []string{resumePch}, ControlChannelName: resumePch + "_vcchan", FlushAllMsgsBase64: map[string]string{resumePch: base64.StdEncoding.EncodeToString(raw)}, DatabaseBackups: []*backuppb.DatabaseBackupInfo{{DbId: 1, DbName: "default"}}, CollectionBackups: []*backuppb.CollectionBackupInfo{coll}}
			store := jobstate.NewStore()
			store.ConfigurePersistence(c.Restore.Persistence)
			resumable := tc.name != "BinlogWithoutResume"
			task, err := NewTask(TaskArgs{TaskID: "job", Resumable: resumable, SourceClusterID: "src", TargetClusterID: "tgt", Params: c, BackupDir: root, Backup: backup, BackupStorage: src, MilvusStorage: src, Store: store})
			require.NoError(t, err)
			result, err := task.ExecuteWithResult(ctx)
			require.Error(t, err)
			require.Equal(t, resumable, result.Resumable, "%v", err)
			if !resumable {
				assert.ErrorContains(t, err, "bulk insert failed")
				assert.Nil(t, task.Tracker().SecondaryProgress())
				_, _, err := store.StartResume("job")
				assert.ErrorIs(t, err, jobstate.ErrRestoreConflict)
				_, _, err = store.StartCleanup("job")
				assert.ErrorIs(t, err, jobstate.ErrRestoreConflict)
				relative, err := filepath.Rel(root, binlogPath)
				require.NoError(t, err)
				files, err := filepath.Glob(filepath.Join(c.Milvus.Storage.RootPath.Val, "restore-temp-*", "0-0", relative))
				require.NoError(t, err)
				assert.Empty(t, files)
				assert.FileExists(t, binlogPath)
				return
			}
			store = jobstate.NewStore()
			store.ConfigurePersistence(c.Restore.Persistence)
			owner, p, err := store.StartResume("job")
			require.NoError(t, err)
			planBefore, err := json.Marshal(p.Secondary)
			require.NoError(t, err)
			progress := owner.SecondaryProgress()
			require.NotNil(t, progress)
			collProgress := progress.Collections[123]
			require.NotNil(t, collProgress)
			op := &collProgress.SnapshotImport
			if tc.format == meta.FormatBinlog {
				require.Len(t, collProgress.BinlogImports[0], 1)
				op = &collProgress.BinlogImports[0][0]
				input := p.Secondary.Collections[123].Phases[0][0]
				relative, err := filepath.Rel(root, input.Batch.Dirs[0].Insert)
				require.NoError(t, err)
				mu.Lock()
				staged = filepath.Join(c.Milvus.Storage.RootPath.Val, collProgress.StagingDir, "0-0", relative, "log")
				mu.Unlock()
				require.FileExists(t, staged)
				// Keep the same key and size to detect an incorrect size-only shortcut.
				require.NoError(t, os.WriteFile(staged, []byte("broken"), 0o600))
			}
			if tc.second == nil {
				require.Contains(t, result.ErrorMessage, "injected final verification failure")
				require.Equal(t, jobstate.SecondaryImportCompleted, op.State)
			}
			id := op.JobID
			require.NotZero(t, id)
			assert.True(t, progress.StreamCheckpoint.Sent(importKey(id)))
			Resume(ctx, "job", owner, p)
			result = owner.Snapshot()
			if tc.success {
				require.Equal(t, jobstate.RestoreStateSuccess, result.State, result.ErrorMessage)
				assert.Nil(t, owner.SecondaryProgress())
				assert.Equal(t, jobstate.SecondaryImportCompleted, op.State)
				srv.mu.Lock()
				assert.True(t, proto.Equal(flush, srv.lastMessage))
				if tc.second == nil {
					count := 0
					for _, tick := range srv.ticks {
						if tick == 100 {
							count++
						}
					}
					assert.Equal(t, 1, count)
				}
				srv.mu.Unlock()
			} else {
				assert.Equal(t, jobstate.RestoreStateFail, result.State)
				assert.Same(t, progress, owner.SecondaryProgress())
				assert.NotEqual(t, jobstate.SecondaryImportCompleted, op.State)
			}
			if staged != "" {
				if !tc.success {
					assert.FileExists(t, staged)
					cleaner, stored, err := store.StartCleanup("job")
					require.NoError(t, err)
					err = CleanupFailed(ctx, cleaner, stored)
					cleaner.FinishCleanup(err)
					require.NoError(t, err)
				}
				assert.NoFileExists(t, staged)
				assert.Empty(t, collProgress.StagingDir)
			}
			if tc.replace {
				assert.NotEqual(t, id, op.JobID)
			} else {
				assert.Equal(t, id, op.JobID)
			}
			planAfter, err := json.Marshal(p.Secondary)
			require.NoError(t, err)
			assert.JSONEq(t, string(planBefore), string(planAfter))
			mu.Lock()
			defer mu.Unlock()
			assert.Empty(t, states)
			assert.Equal(t, strconv.FormatInt(op.JobID, 10), queried[len(queried)-1])
		})
	}
}

func TestResumeReplacementInterruptedBeforeSubmission(t *testing.T) {
	store := jobstate.NewStore()
	tracker, addErr := store.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	plan := &restoreplan.Plan{Secondary: &restoreplan.Secondary{
		Collections: map[int64]*restoreplan.SecondaryCollection{123: {}},
	}}
	progress := jobstate.NewSecondaryProgress(plan.Secondary)
	progress.StreamCheckpoint = milvus.NewStreamCheckpoint(nil)
	progress.Collections[123].SnapshotImport = jobstate.SecondaryImportProgress{
		JobID: 41, State: jobstate.SecondaryImportFailed,
	}
	tracker.SaveSecondaryPlan(plan, progress)
	tracker.SetFail(assert.AnError)

	var submitted []int64
	for _, submitErr := range []error{assert.AnError, context.Canceled, nil} {
		owner, _, err := store.StartResume("job")
		require.NoError(t, err)
		saved := owner.SecondaryProgress()
		op := &saved.Collections[123].SnapshotImport
		// Reconstruct the executor from the store for each Resume. No query
		// client is supplied: an unregistered ID must go straight to submit.
		job := bulkInsertJob{
			streamCheckpoint: saved.StreamCheckpoint,
			logger:           zap.NewNop(),
			submit: func(_ context.Context, id int64) error {
				submitted = append(submitted, id)
				assert.Equal(t, id, op.JobID)
				assert.Equal(t, jobstate.SecondaryImportPending, op.State)
				return submitErr // Fail before the message is registered/sent.
			},
		}
		state, err := job.start(t.Context(), op)
		assert.NotZero(t, op.JobID)
		assert.NotEqual(t, int64(41), op.JobID)
		require.NotEmpty(t, submitted)
		assert.Equal(t, submitted[0], op.JobID)
		assert.False(t, saved.StreamCheckpoint.Sent(importKey(op.JobID)))
		if submitErr != nil {
			require.ErrorIs(t, err, submitErr)
			owner.SetFail(err)
			assert.Equal(t, jobstate.SecondaryImportPending, op.State)
			continue
		}
		require.NoError(t, err)
		job.restfulCli = &completedRestful{}
		require.NoError(t, job.wait(t.Context(), op, state))
		assert.Equal(t, jobstate.SecondaryImportCompleted, op.State)
		owner.SetSuccess()
	}
	assert.Len(t, submitted, 3)
	assert.Equal(t, jobstate.RestoreStateSuccess, tracker.Snapshot().State)
}

func TestResumeCompletedCollectionWithoutDatabaseMetadata(t *testing.T) {
	tracker, addErr := jobstate.NewStore().AddRestoreTask("job", true)
	require.NoError(t, addErr)
	coll := &backuppb.CollectionBackupInfo{DbName: "sales", CollectionName: "orders"}
	target := collref.New(coll.GetDbName(), coll.GetCollectionName())
	tracker.NewCollTracker(target, 1).NewJob(1).SetCompleted()

	task := &Task{tracker: tracker}
	assert.NoError(t, task.runCollTask(t.Context(), nil, coll, ddlTaskArgs{}, nil, loadTaskArgs{}))
}

func TestResumeMatchesBinlogProgressToPlannedBatches(t *testing.T) {
	vchannels := newTestVChannels()[:1]
	coll := newTestCollBackup(vchannels, nil, nil)
	id := coll.GetCollectionId()
	delta := func(dir string) batch {
		return batch{isL0: true, partitionDirs: []partitionDir{{deltaLogDir: dir}}}
	}
	plan := &restoreplan.SecondaryCollection{}
	plan.Phases[0] = planImports(10, []batch{{partitionDirs: []partitionDir{{insertLogDir: "/insert"}}}})
	plan.Phases[1] = planImports(10, []batch{delta("/first"), delta("/second")})
	plan.Phases[2] = planImports(common.AllPartitionsID, []batch{delta("/global")})

	store := jobstate.NewStore()
	tracker, addErr := store.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	fullPlan := &restoreplan.Plan{Secondary: &restoreplan.Secondary{
		Collections: map[int64]*restoreplan.SecondaryCollection{id: plan},
	}}
	progress := jobstate.NewSecondaryProgress(fullPlan.Secondary)
	progress.StreamCheckpoint = milvus.NewStreamCheckpoint(newTestPchTS(vchannels))
	tracker.SaveSecondaryPlan(fullPlan, progress)
	batches := progress.Collections[id].BinlogImports
	batches[0][0] = jobstate.SecondaryImportProgress{JobID: 11, State: jobstate.SecondaryImportCompleted}
	batches[1][0] = jobstate.SecondaryImportProgress{JobID: 21, State: jobstate.SecondaryImportCompleted}
	batches[1][1].JobID = 22 // Allocated before the interrupted submission.
	tracker.SetFail(assert.AnError)

	owner, saved, err := store.StartResume("job")
	require.NoError(t, err)
	stream := &recordingStream{}
	task := newTestBinlogDMLTask(t, coll, stream)
	resumed := owner.SecondaryProgress()
	task.saved = saved.Secondary.Collections[id]
	task.progress = resumed.Collections[id]
	task.streamCheckpoint = resumed.StreamCheckpoint
	require.NoError(t, task.Execute(t.Context()))

	bodies := importBodies(t, stream)
	require.Len(t, bodies, 2)
	globalID := batches[2][0].JobID
	assert.NotZero(t, globalID)
	for i, want := range []struct {
		id, partition int64
		path          string
	}{{22, 10, "/second"}, {globalID, common.AllPartitionsID, "/global"}} {
		body := bodies[i]
		assert.Equal(t, want.id, body.GetJobID())
		assert.Equal(t, []int64{want.partition}, body.GetPartitionIDs())
		require.Len(t, body.GetFiles(), 1)
		assert.Equal(t, []string{want.path}, body.GetFiles()[0].GetPaths())
	}
	assert.Equal(t, jobstate.SecondaryImportCompleted, batches[1][1].State)
	assert.Equal(t, jobstate.SecondaryImportCompleted, batches[2][0].State)
}

func TestResubmissionRecopiesBinlogsIntoSavedDirectory(t *testing.T) {
	ctx := t.Context()
	client, err := storage.NewClient(ctx, storage.Config{Provider: cfg.ProviderLocal})
	require.NoError(t, err)
	const stagingDir = "restore-temp-test/"
	task := &collBinlogDMLTask{
		backupStorage: client, milvusStorage: client, backupDir: t.TempDir(), milvusRootPath: t.TempDir(),
		copySem: semaphore.NewWeighted(1), logger: zap.NewNop(),
	}
	source := filepath.Join(task.backupDir, "binlogs")
	dest := filepath.Join(task.milvusRootPath, stagingDir, "binlogs")
	require.NoError(t, os.MkdirAll(source, 0o755))
	require.NoError(t, os.MkdirAll(dest, 0o755))
	for _, name := range []string{"a", "b"} {
		require.NoError(t, os.WriteFile(filepath.Join(source, name), []byte("complete"), 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dest, "a"), []byte("outdated"), 0o600))
	input := batch{partitionDirs: []partitionDir{{insertLogDir: source + "/"}}}
	staged, err := task.stageBatch(ctx, input, stagingDir)
	require.NoError(t, err)
	assert.Equal(t, "restore-temp-test/binlogs/", staged.partitionDirs[0].insertLogDir)
	for _, name := range []string{"a", "b"} {
		data, err := os.ReadFile(filepath.Join(dest, name))
		require.NoError(t, err)
		assert.Equal(t, "complete", string(data))
	}
}

func TestSecondaryPartialCopyCleanup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resumable bool
		wantFile  bool
	}{
		{"DefaultCleansPartialCopy", false, false},
		{"ResumableRetainsPartialCopy", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			local, err := storage.NewClient(ctx, storage.Config{Provider: cfg.ProviderLocal})
			require.NoError(t, err)
			root := t.TempDir()
			insert := filepath.Join(root, "insert")
			require.NoError(t, os.MkdirAll(insert, 0o755))
			file := filepath.Join(insert, "log")
			require.NoError(t, os.WriteFile(file, []byte("data"), 0o600))

			// Listing delta fails after the insert log has been copied.
			plan := &restoreplan.SecondaryCollection{StageBinlogs: true}
			plan.Phases[0] = []restoreplan.SecondaryImport{{Batch: restoreplan.BinlogImportBatch{
				Dirs: []restoreplan.Directory{{Insert: insert + "/", Delta: filepath.Join(file, "delta") + "/"}},
			}}}
			task := &collBinlogDMLTask{
				saved: plan, progress: jobstate.NewSecondaryCollectionProgress(plan),
				backupDir: root, backupStorage: local, milvusStorage: local, milvusRootPath: t.TempDir(),
				copySem: semaphore.NewWeighted(1), logger: zap.NewNop(),
			}
			if tc.resumable {
				task.streamCheckpoint = milvus.NewStreamCheckpoint(nil)
			}
			staged := filepath.Join(task.milvusRootPath, task.progress.StagingDir, "0-0", "insert", "log")
			require.ErrorContains(t, task.Execute(ctx), "stage delta log dir")
			if tc.wantFile {
				assert.FileExists(t, staged)
			} else {
				assert.NoFileExists(t, staged)
			}
		})
	}
}
