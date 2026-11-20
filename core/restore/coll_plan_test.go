package restore

import (
	"context"
	"net"
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
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/snapshotsource"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// newSelectPlanTask builds a task whose plan selection sees the given format
// and strategy pin, with the wiring plan construction touches: both storages
// and the job tracker from newTestTask. The storage clients are local-provider
// ones — constructing them never dials, and their existence probes of the
// backup dir simply find nothing.
func newSelectPlanTask(t *testing.T, format string, strategy Strategy, resumable bool) *Task {
	task := newTestTask(t, resumable)
	task.format = format
	task.args.Option = &Option{Strategy: strategy, Resumable: resumable}
	task.args.BackupDir = "backup/"
	task.args.BackupStorage = newTestStorageClient(t, cfg.ProviderLocal)
	task.args.MilvusStorage = newTestStorageClient(t, cfg.ProviderLocal)
	task.args.Params.Restore.MaxSegmentsPerImportJob.Val = 256
	return task
}

// oneCollTargets is one collection with one partition and one leveled segment:
// the smallest backup meta that still builds a data task.
func oneCollTargets() []collTarget {
	return []collTarget{{
		dbBackup: &backuppb.DatabaseBackupInfo{},
		collBackup: &backuppb.CollectionBackupInfo{
			DbName: "db", CollectionName: "coll", CollectionId: 1,
			PartitionBackups: []*backuppb.PartitionBackupInfo{{
				PartitionId: 1, PartitionName: "p1", Size: 10,
				SegmentBackups: []*backuppb.SegmentBackupInfo{
					{SegmentId: 1, PartitionId: 1, Size: 1},
				},
			}},
		},
		target: collref.New("db", "coll"),
	}}
}

// oneSnapCollTargets adds the bundle pointer a snapshot backup carries: the
// metadata path the import and restore api calls resolve against the backup
// directory.
func oneSnapCollTargets() []collTarget {
	targets := oneCollTargets()
	targets[0].collBackup.SnapshotBackup = &backuppb.SnapshotBackupInfo{
		MetadataPath: "bundle/snapshots/1/metadata/2.json",
	}
	return targets
}

// The plan is picked by the backup's format and the strategy: the format
// narrows which strategies exist, auto resolves a binlog backup against the
// target's v2 import support, and a pin outside the format's set is a loud
// error rather than a silent fallback.
func TestTask_selectPlan(t *testing.T) {
	// The plan builds its whole tree at selection time, so which import api a
	// binlog plan runs on is observable in the tasks themselves.
	assertBinlogPlan := func(t *testing.T, plan tasklet.Tasklet, importTask any) {
		t.Helper()
		bp, ok := plan.(*binlogPlan)
		require.True(t, ok, "plan is %T, want *binlogPlan", plan)
		require.Len(t, bp.colls, 1)
		assert.IsType(t, &collDDLTask{}, bp.colls[0].ddl)
		require.Len(t, bp.colls[0].parts, 1)
		require.Len(t, bp.colls[0].parts[0].leveled, 1)
		assert.IsType(t, importTask, bp.colls[0].parts[0].leveled[0])
	}
	assertStoredImport := func(t *testing.T, task *Task) {
		t.Helper()
		jobs := task.tracker.TrackerForCollection(collref.New("db", "coll")).TrackersForBinlogImports()
		require.Len(t, jobs, 1)
		batch := jobs[0].Batch
		assert.Equal(t, "p1", batch.Partition)
		assert.Zero(t, batch.PartitionIndex)
		assert.False(t, batch.L0)
		require.Len(t, batch.Dirs, 1)
		assert.EqualValues(t, 10, batch.Dirs[0].Size)
		assert.NotEmpty(t, batch.Dirs[0].Insert)
	}

	t.Run("BinlogAutoUsesV2WhenSupported", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyAuto, true)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportV2).Return(true)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaRESTFulTask{})
		assertStoredImport(t, task)
		assert.Equal(t, StrategyMergedSeg, task.savedPlan(plan).Strategy)
	})

	t.Run("BinlogAutoFallsBackToGrpc", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyAuto, true)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportV2).Return(false)
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaGRPCTask{})
		assertStoredImport(t, task)
		assert.Equal(t, StrategyPerSeg, task.savedPlan(plan).Strategy)
	})

	t.Run("BinlogPerSeg", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyPerSeg, true)

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaGRPCTask{})
		assertStoredImport(t, task)
	})

	t.Run("BinlogMergedSeg", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyMergedSeg, true)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaRESTFulTask{})
		assertStoredImport(t, task)
	})

	t.Run("BinlogMetaOnlyBuildsNoDataTasks", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyPerSeg, true)
		task.args.Option.MetaOnly = true

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		mp, ok := plan.(*metaOnlyPlan)
		require.True(t, ok, "plan is %T, want *metaOnlyPlan", plan)
		require.Len(t, mp.colls, 1)
		assert.IsType(t, &collDDLTask{}, mp.colls[0])
	})

	t.Run("BinlogRejectsSnapshotStrategies", func(t *testing.T) {
		for _, strategy := range []Strategy{StrategyRestoreSnap, StrategyImportSnap} {
			_, err := newSelectPlanTask(t, meta.FormatBinlog, strategy, true).selectPlan(t.Context(), nil)
			assert.ErrorContains(t, err, "snapshot")
		}
	})

	t.Run("SnapshotAutoUsesRestoreApi", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyAuto, true)

		plan, err := task.selectPlan(t.Context(), nil)
		require.NoError(t, err)
		assert.IsType(t, &restoreSnapPlan{}, plan)
	})

	t.Run("SnapshotRestoreSnap", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyRestoreSnap, true)

		plan, err := task.selectPlan(t.Context(), nil)
		require.NoError(t, err)
		assert.IsType(t, &restoreSnapPlan{}, plan)
	})

	t.Run("SnapshotRestoreSnapWithSecondariesFails", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyRestoreSnap, false)
		task.args.Option.Secondaries = []Secondary{{Address: "http://secondary:19530"}}

		_, err := task.selectPlan(t.Context(), nil)
		assert.ErrorContains(t, err, "import_snap")
	})

	// The import plan is one DDL head and one snapshot import job per
	// collection: the api expands the bundle's partitions and segments itself.
	t.Run("SnapshotImportSnap", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyImportSnap, true)
		task.snapshotSource = snapshotsource.Source{DirURI: "s3://backup-bucket/backup/mybackup", SameService: true}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportSnapshot).Return(true)
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneSnapCollTargets())
		require.NoError(t, err)
		ip, ok := plan.(*importSnapPlan)
		require.True(t, ok, "plan is %T, want *importSnapPlan", plan)
		require.Len(t, ip.colls, 1)
		assert.IsType(t, &collDDLTask{}, ip.colls[0].ddl)
		imp := ip.colls[0].imp.(*snapshotImportTask)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json", imp.importer.metadataURI)
		assert.JSONEq(t, `{"p1": "p1"}`, imp.importer.partitionMapping)
	})

	for _, tt := range []struct {
		name     string
		strategy Strategy
	}{
		{name: "Auto", strategy: StrategyAuto},
		{name: "ImportSnap", strategy: StrategyImportSnap},
	} {
		t.Run(tt.name+"WithSecondariesUses2PC", func(t *testing.T) {
			task := newSelectPlanTask(t, meta.FormatSnapshot, tt.strategy, false)
			task.args.Option.Secondaries = []Secondary{{Address: "http://secondary:19530"}}
			task.secondaries = []importSecondary{{addr: "secondary", cli: &snapshotImportEndpoint{}}}
			task.snapshotSource = snapshotsource.Source{DirURI: "s3://backup-bucket/backup/mybackup", SameService: true}
			grpcCli := milvus.NewMockGrpc(t)
			grpcCli.EXPECT().HasFeature(milvus.ImportSnapshot).Return(true)
			task.grpc = grpcCli

			plan, err := task.selectPlan(t.Context(), oneSnapCollTargets())
			require.NoError(t, err)
			ip, ok := plan.(*importSnapPlan)
			require.True(t, ok, "plan is %T, want *importSnapPlan", plan)
			require.Len(t, ip.colls, 1)
			imp, ok := ip.colls[0].imp.(*snapshotImport2PCTask)
			require.True(t, ok, "import task is %T, want *snapshotImport2PCTask", ip.colls[0].imp)
			assert.Equal(t, task.secondaries, imp.secondaries)
			assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json", imp.importer.metadataURI)
		})
	}

	t.Run("SnapshotImportSnapNeedsNewerServer", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyImportSnap, true)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportSnapshot).Return(false)
		task.grpc = grpcCli

		_, err := task.selectPlan(t.Context(), oneSnapCollTargets())
		assert.ErrorContains(t, err, "3.0.4")
	})

	t.Run("SnapshotAutoWithSecondariesNeedsNewerServer", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyAuto, false)
		task.args.Option.Secondaries = []Secondary{{Address: "http://secondary:19530"}}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportSnapshot).Return(false)
		task.grpc = grpcCli

		_, err := task.selectPlan(t.Context(), oneSnapCollTargets())
		assert.ErrorContains(t, err, "3.0.4")
	})

	t.Run("SnapshotImportSnapMetaOnlyBuildsNoDataTasks", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyImportSnap, true)
		task.args.Option.MetaOnly = true
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportSnapshot).Return(true)
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneSnapCollTargets())
		require.NoError(t, err)
		assert.IsType(t, &metaOnlyPlan{}, plan)
	})

	t.Run("SnapshotRejectsBinlogStrategies", func(t *testing.T) {
		for _, strategy := range []Strategy{StrategyPerSeg, StrategyMergedSeg} {
			_, err := newSelectPlanTask(t, meta.FormatSnapshot, strategy, true).selectPlan(t.Context(), nil)
			assert.ErrorContains(t, err, "binlog")
		}
	})

	// A secondary commit exists only on the restful import api, so auto
	// resolves to it directly — the preflight has already established the
	// target is new enough — and a per_seg pin is refused rather than silently
	// losing the two-phase commit.
	t.Run("BinlogAutoWithSecondariesUsesRestful", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyAuto, true)
		const endpoint = "http://secondary:19530"
		task.args.Option.Secondaries = []Secondary{{Address: endpoint}}
		task.secondaries = []importSecondary{{addr: endpoint}}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		targets := oneCollTargets()
		plan, err := task.selectPlan(t.Context(), targets)
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaRESTFul2PCTask{})
		target := targets[0].target
		assert.Len(t, task.tracker.Snapshot().Colls[target].Jobs, 1)
		assertStoredImport(t, task)
	})

	t.Run("BinlogPerSegWithSecondariesFails", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyPerSeg, true)
		task.args.Option.Secondaries = []Secondary{{Address: "http://secondary:19530"}}

		_, err := task.selectPlan(t.Context(), nil)
		assert.ErrorContains(t, err, "secondaries")
	})
}

func TestTask_needStaging(t *testing.T) {
	newClient := func(bucket string) storage.Client {
		cli, err := storage.NewClient(context.Background(), storage.Config{
			Provider:   cfg.ProviderMinio,
			Endpoint:   "localhost:9000",
			Region:     "us-east-1",
			Bucket:     bucket,
			Credential: storage.Credential{Type: storage.Static, AK: "a", SK: "b"},
		})
		require.NoError(t, err)
		return cli
	}
	newTask := func(backupBucket, milvusBucket string, streaming bool) *Task {
		task := newTestTask(t, false)
		task.args.BackupStorage = newClient(backupBucket)
		task.args.MilvusStorage = newClient(milvusBucket)
		task.streaming = streaming
		return task
	}

	t.Run("SameBucketAndNotStreaming", func(t *testing.T) {
		assert.False(t, newTask("bucket", "bucket", false).needStaging())
	})

	t.Run("DifferentBucket", func(t *testing.T) {
		assert.True(t, newTask("bucket", "another", false).needStaging())
	})

	t.Run("StreamingForcesCopy", func(t *testing.T) {
		assert.True(t, newTask("bucket", "bucket", true).needStaging())
	})
}

func TestTask_mappedEZK(t *testing.T) {
	newTask := func(mapping map[string]string) *Task {
		task := newTestTask(t, false)
		task.args.Option = &Option{EZKMapping: mapping}
		return task
	}

	t.Run("Normal", func(t *testing.T) {
		dbBackup := &backuppb.DatabaseBackupInfo{Ezk: "hello"}
		assert.Equal(t, "hello", newTask(nil).mappedEZK(dbBackup))
	})

	t.Run("WithoutEZK", func(t *testing.T) {
		assert.Equal(t, "", newTask(nil).mappedEZK(&backuppb.DatabaseBackupInfo{}))
	})

	t.Run("WithMapping", func(t *testing.T) {
		dbBackup := &backuppb.DatabaseBackupInfo{Ezk: "old_key"}
		task := newTask(map[string]string{"old_key": "new_key"})
		assert.Equal(t, "new_key", task.mappedEZK(dbBackup))
	})

	t.Run("WithMappingNoMatch", func(t *testing.T) {
		dbBackup := &backuppb.DatabaseBackupInfo{Ezk: "other_key"}
		task := newTask(map[string]string{"old_key": "new_key"})
		assert.Equal(t, "other_key", task.mappedEZK(dbBackup))
	})

	t.Run("WithMappingEmptyEZK", func(t *testing.T) {
		task := newTask(map[string]string{"old_key": "new_key"})
		assert.Equal(t, "", task.mappedEZK(&backuppb.DatabaseBackupInfo{}))
	})
}

// Serve the resumed plan over a new gRPC connection using the same expectations.
type resumeMilvusServer struct {
	milvuspb.UnimplementedMilvusServiceServer
	client milvus.Grpc
}

func (*resumeMilvusServer) Connect(context.Context, *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	return &milvuspb.ConnectResponse{Status: &commonpb.Status{}, ServerInfo: &commonpb.ServerInfo{BuildTags: "2.6.17"}}, nil
}

func (s *resumeMilvusServer) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.client.DescribeCollection(ctx, "db", req.GetCollectionName())
}

func (s *resumeMilvusServer) DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	indexes, err := s.client.ListIndex(ctx, "db", req.GetCollectionName())
	return &milvuspb.DescribeIndexResponse{Status: &commonpb.Status{}, IndexDescriptions: indexes}, err
}

func (s *resumeMilvusServer) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	id, err := s.client.BulkInsert(ctx, milvus.GrpcBulkInsertInput{Paths: req.GetFiles()})
	return &milvuspb.ImportResponse{Status: &commonpb.Status{}, Tasks: []int64{id}}, err
}

func (s *resumeMilvusServer) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return s.client.GetBulkInsertState(ctx, req.GetTask())
}

func resumeTestConfig(t *testing.T, client milvus.Grpc) *cfg.Config {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(server, &resumeMilvusServer{client: client})
	t.Cleanup(server.Stop)
	go server.Serve(listener)
	params := cfg.New()
	params.Milvus.Grpc.Address.Val = "127.0.0.1"
	params.Milvus.Grpc.Port.Val = listener.Addr().(*net.TCPAddr).Port
	params.Milvus.Grpc.TLSMode.Val = cfg.TLSDisabled
	return params
}

func TestResumeMetaOnlyAfterIndexFailure(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	task := newSelectPlanTask(t, meta.FormatBinlog, StrategyPerSeg, true)
	task.grpc = cli
	task.args.Params = resumeTestConfig(t, cli)
	task.args.Params.Restore.Concurrency.Collections.Val = 1
	task.args.Option.MetaOnly = true
	task.args.Option.SkipCreateCollection = true
	task.args.Option.DropExistIndex = true
	first := collref.New("db", "first")
	second := collref.New("db", "second")
	for _, name := range []collref.Name{first, second} {
		task.collTargets = append(task.collTargets, collTarget{target: name, collBackup: &backuppb.CollectionBackupInfo{CollectionName: name.CollName()}})
		cli.EXPECT().HasCollection(mock.Anything, "db", name.CollName()).Return(true, nil).Once()
	}
	cli.EXPECT().HasFeature(milvus.Import2PC).Return(false).Once()
	cli.EXPECT().ListDatabases(mock.Anything).Return([]string{"db"}, nil).Twice()
	require.True(t, task.tracker.Start())
	require.NoError(t, task.prepare(t.Context()))

	cli.EXPECT().DescribeCollection(mock.Anything, "db", "first").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Once()
	cli.EXPECT().ListIndex(mock.Anything, "db", "first").Return(nil, nil).Once()
	secondInfo := &milvuspb.DescribeCollectionResponse{CollectionID: 200, Status: &commonpb.Status{}}
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "second").Return(secondInfo, nil).Twice()
	cli.EXPECT().ListIndex(mock.Anything, "db", "second").Return([]*milvuspb.IndexDescription{{IndexName: "idx"}}, nil).Once()
	cli.EXPECT().DropIndex(mock.Anything, "db", "second", "idx").Return(assert.AnError).Once()
	err := task.plan.Execute(t.Context())
	require.ErrorIs(t, err, assert.AnError)
	failed := task.tracker.Finish(err)
	require.True(t, failed.Resumable)
	assert.Equal(t, jobstate.RestoreDDLSuccess, failed.Colls[first].DDL.State)
	assert.Equal(t, jobstate.RestoreDDLFail, failed.Colls[second].DDL.State)

	cli.EXPECT().ListIndex(mock.Anything, "db", "second").Return(nil, nil).Once()
	require.NoError(t, Resume(t.Context(), task.args.Store, task.args.TaskID))
	require.Eventually(t, func() bool { return task.tracker.Snapshot().Terminal() }, 5*time.Second, time.Millisecond)
	done := task.tracker.Snapshot()
	require.Equal(t, jobstate.RestoreStateSuccess, done.State, done.ErrorMessage)
	assert.Equal(t, failed.Colls[first], done.Colls[first])
	assert.Equal(t, jobstate.RestoreDDLSuccess, done.Colls[second].DDL.State)
	assert.Empty(t, done.Colls[second].Jobs)
}

// Restart the same collection tree after B's poll fails. Its earlier leveled
// import A has settled and the collection-global L0 task C has not started.
func TestResumeCollectionAtImportBoundary(t *testing.T) {
	mockClient := milvus.NewMockGrpc(t)
	client := mockClient
	params := resumeTestConfig(t, client)
	target := collref.New("db", "coll")
	store := jobstate.NewStore()
	tr, addErr := store.AddRestoreTask("job", true)
	require.NoError(t, addErr)
	coll := tr.NewCollTracker(target, 3)
	ddl := newCollDDLTask("job", &Option{SkipCreateCollection: true, Resumable: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, client, coll.DDL())
	identity := &milvuspb.DescribeCollectionResponse{CollectionID: 100, Status: &commonpb.Status{}}
	mockClient.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(identity, nil).Once()
	for i, path := range []string{"a", "b", "c"} {
		mockClient.EXPECT().BulkInsert(mock.Anything, mock.MatchedBy(func(in milvus.GrpcBulkInsertInput) bool { return len(in.Paths) > 0 && in.Paths[0] == path })).Return(int64(i+1), nil).Once()
	}
	complete := &milvuspb.GetImportStateResponse{Status: &commonpb.Status{}, State: commonpb.ImportState_ImportCompleted}
	mockClient.EXPECT().GetBulkInsertState(mock.Anything, int64(1)).Return(complete, nil).Once()
	mockClient.EXPECT().GetBulkInsertState(mock.Anything, int64(2)).Return(nil, assert.AnError).Once()
	require.True(t, tr.Start())
	params.Transfer.Concurrency.Val = 1
	params.Restore.Concurrency.ImportJobs.Val = 1
	params.Restore.Concurrency.Collections.Val = 1
	params.Backup.Storage.Provider.Val = cfg.ProviderLocal
	params.Milvus.Storage.Provider.Val = cfg.ProviderLocal
	// Limit the original executor's lifetime to this block; Resume gets only store and ID.
	{
		src := newTestStorageClient(t, cfg.ProviderLocal)
		original := &Task{tracker: tr, args: TaskArgs{Params: params, Option: ddl.option, BackupStorage: src, MilvusStorage: src}, grpc: client, bulkInsertSem: semaphore.NewWeighted(1), logger: zap.NewNop(), collTargets: []collTarget{{target: target, collBackup: ddl.collBackup}}}
		args := original.newImportTaskArgs(original.collTargets[0], coll)
		planner := &grpcImportPlanner{args: args}
		group := func(path string, l0 bool) []dirGroup {
			return []dirGroup{{isL0: l0, dirs: []partitionDir{{insertLogDir: path, size: 1}}}}
		}
		plan := &collBinlogTask{ddl: ddl, sem: original.bulkInsertSem, logger: original.logger,
			parts:    []partBinlogTasks{{leveled: planner.planTasks(0, "", group("a", false)), l0: planner.planTasks(0, "", group("b", true))}},
			globalL0: planner.planTasks(-1, "", group("c", true)),
		}
		tr.SavePlan(original.savedPlan(&binlogPlan{strategy: StrategyPerSeg}))
		err := plan.Execute(t.Context())
		require.ErrorIs(t, err, assert.AnError)
		tr.SetFail(err)
	}
	jobs := tr.Snapshot().Colls[target].Jobs
	require.Len(t, jobs, 3)
	assert.Equal(t, jobstate.RestoreJobCompleted, jobs[0].State)
	assert.Equal(t, "2", jobs[1].ID)
	assert.Equal(t, jobstate.RestoreJobPending, jobs[2].State)

	// Cancel the first resume while B is still running on Milvus.
	resumeCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mockClient.EXPECT().GetBulkInsertState(mock.Anything, int64(2)).RunAndReturn(func(context.Context, int64) (*milvuspb.GetImportStateResponse, error) {
		cancel()
		return &milvuspb.GetImportStateResponse{Status: &commonpb.Status{}, State: commonpb.ImportState_ImportPending}, nil
	}).Once()
	require.NoError(t, Resume(resumeCtx, store, "job"))
	require.Eventually(t, func() bool { return tr.Snapshot().State == jobstate.RestoreStateFail }, 5*time.Second, time.Millisecond)

	assert.Equal(t, jobstate.RestoreDDLSuccess, tr.Snapshot().Colls[target].DDL.State)
	mockClient.EXPECT().GetBulkInsertState(mock.Anything, int64(2)).Return(complete, nil).Once()
	mockClient.EXPECT().GetBulkInsertState(mock.Anything, int64(3)).Return(complete, nil).Once()
	require.NoError(t, Resume(t.Context(), store, "job"))
	require.Eventually(t, func() bool { return tr.Snapshot().State == jobstate.RestoreStateSuccess }, 5*time.Second, time.Millisecond)
	got := tr.Snapshot()
	assert.Len(t, got.Colls[target].Jobs, 3)
}
