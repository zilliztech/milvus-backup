package restore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func newTestTask() *Task {
	store := jobstate.NewStore()
	return &Task{
		logger: zap.NewNop(),

		tracker: store.AddRestoreTask("task-1"),

		args: TaskArgs{
			TaskID: "task-1",
			Plan:   &Plan{},
			Params: &cfg.Config{},
			Store:  store,
		},
	}
}

// A format this version does not know has to be refused up front. Restoring it as binlogs
// would find no segments and report a successful restore of an empty collection.
func TestNewTask_RefusesUnknownFormat(t *testing.T) {
	args := TaskArgs{
		TaskID: "task-1",
		Backup: &backuppb.BackupInfo{Name: "mybackup", Format: "parquet"},
	}

	task, err := NewTask(t.Context(), args)
	assert.ErrorContains(t, err, "parquet")
	assert.Nil(t, task)
}

// A snapshot format backup resolves its bundles against the backup storage it is being read
// from, so that a backup moved to another bucket or prefix stays restorable.
func TestNewTask_SnapshotSource(t *testing.T) {
	milvusStorage := storage.NewMockClient(t)
	milvusStorage.EXPECT().Config().Return(storage.Config{
		Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
	})
	backupStorage := storage.NewMockClient(t)
	backupStorage.EXPECT().Config().Return(storage.Config{
		Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "backup-bucket",
	})

	args := TaskArgs{
		TaskID:        "task-1",
		Backup:        &backuppb.BackupInfo{Name: "mybackup", Format: meta.FormatSnapshot},
		Plan:          &Plan{},
		Option:        &Option{},
		Params:        &cfg.Config{},
		BackupDir:     "backup/mybackup/",
		BackupStorage: backupStorage,
		MilvusStorage: milvusStorage,
		Store:         jobstate.NewStore(),
	}

	task, err := NewTask(t.Context(), args)
	require.NoError(t, err)
	assert.Equal(t, meta.FormatSnapshot, task.format)
	// Both sides are the same backend, so the uri names only the bucket and Milvus
	// resolves it through its own storage config.
	assert.Equal(t, "s3://backup-bucket/backup/mybackup", task.snapshotSource.dirURI)
	// Both sides are the same backend, so Milvus reads with its own credential.
	assert.Empty(t, task.snapshotSource.externalSpec)
}

// A backup on another storage service than the target's cannot ride the restore
// api's server-side copy: the restore path stages the bundle into the milvus
// storage first, while the import path reads the source itself through the spec.
func TestNewTask_SnapshotCrossService(t *testing.T) {
	newArgs := func(strategy Strategy) TaskArgs {
		milvusStorage := storage.NewMockClient(t)
		milvusStorage.EXPECT().Config().Return(storage.Config{
			Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
		})
		backupStorage := storage.NewMockClient(t)
		backupStorage.EXPECT().Config().Return(storage.Config{
			Provider: cfg.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		})

		return TaskArgs{
			TaskID:        "task-1",
			Backup:        &backuppb.BackupInfo{Name: "mybackup", Format: meta.FormatSnapshot},
			Plan:          &Plan{},
			Option:        &Option{Strategy: strategy},
			Params:        &cfg.Config{},
			BackupDir:     "backup/mybackup/",
			BackupStorage: backupStorage,
			MilvusStorage: milvusStorage,
			Store:         jobstate.NewStore(),
		}
	}

	t.Run("AutoStages", func(t *testing.T) {
		task, err := NewTask(t.Context(), newArgs(StrategyAuto))
		require.NoError(t, err)
		assert.True(t, task.snapshotSource.staging)
	})

	t.Run("RestoreSnapStages", func(t *testing.T) {
		task, err := NewTask(t.Context(), newArgs(StrategyRestoreSnap))
		require.NoError(t, err)
		assert.True(t, task.snapshotSource.staging)
	})

	// The import path reads the source under the backup storage's own
	// credentials, spelled out in the external spec, so it stages nothing.
	t.Run("ImportSnapSendsSpec", func(t *testing.T) {
		task, err := NewTask(t.Context(), newArgs(StrategyImportSnap))
		require.NoError(t, err)
		assert.False(t, task.snapshotSource.staging)
		assert.Contains(t, task.snapshotSource.externalSpec, `"access_key_id":"ak"`)
	})
}

// Milvus restores a snapshot bundle on its own terms on the restore api path, so an
// option asking for anything else is refused rather than silently dropped. The import
// path creates the collection through the ordinary DDL task and honors them.
func TestCheckSnapshotSupport(t *testing.T) {
	// No snapshot data path honors these.
	t.Run("RefusedOnEveryStrategy", func(t *testing.T) {
		for _, strategy := range []Strategy{StrategyRestoreSnap, StrategyImportSnap} {
			assert.Error(t, checkSnapshotSupport(&Option{TruncateBinlogByTs: true}, strategy))
			assert.Error(t, checkSnapshotSupport(&Option{SecondaryEndpoints: []string{"http://secondary:19530"}}, strategy))
		}
	})

	// The restore api creates the collection itself, so these only work on the
	// import path.
	t.Run("RefusedOnTheRestorePathOnly", func(t *testing.T) {
		tests := []struct {
			name string
			opt  *Option
		}{
			{"SkipCreateCollection", &Option{SkipCreateCollection: true}},
			{"MetaOnly", &Option{MetaOnly: true}},
			{"EZKMapping", &Option{EZKMapping: map[string]string{"old": "new"}}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := checkSnapshotSupport(tt.opt, StrategyRestoreSnap)
				assert.ErrorContains(t, err, "import_snap")
				assert.NoError(t, checkSnapshotSupport(tt.opt, StrategyImportSnap))
			})
		}
	})

	// Renaming, filtering and dropping the target are all done before Milvus is asked to
	// restore, so they stay available.
	t.Run("SupportedOptions", func(t *testing.T) {
		opt := &Option{DropExistCollection: true, RestoreRBAC: true}
		assert.NoError(t, checkSnapshotSupport(opt, StrategyRestoreSnap))
		assert.NoError(t, checkSnapshotSupport(opt, StrategyImportSnap))
	})

	// Skipped params are dropped from the restored collection with delete_keys once the
	// job completes, so they stay available even though Milvus creates the collection.
	t.Run("SkipParams", func(t *testing.T) {
		opt := &Option{SkipParams: SkipParams{
			CollectionProperties: []string{"mmap.enabled"},
			FieldTypeParams:      []string{"mmap.enabled"},
			IndexParams:          []string{"mmap.enabled"},
		}}
		assert.NoError(t, checkSnapshotSupport(opt, StrategyRestoreSnap))
	})
}

// On the restore api path the index options are not refused: Milvus restores the
// bundle's own indexes either way, so there is nothing lost by ignoring them. The
// import path creates indexes through the DDL task, so they are honored there.
func TestSnapshotIgnoredOptions(t *testing.T) {
	opt := &Option{RebuildIndex: true, UseAutoIndex: true, DropExistCollection: true}
	assert.ElementsMatch(t, []string{"restore_index", "use_auto_index"}, snapshotIgnoredOptions(opt, StrategyRestoreSnap))
	assert.Empty(t, snapshotIgnoredOptions(opt, StrategyImportSnap))
	assert.Empty(t, snapshotIgnoredOptions(&Option{}, StrategyRestoreSnap))
}

// The secondaries option means something only on a server new enough to commit
// imports two-phase, and only when replication mirrors the target's import jobs
// to the secondaries; every mismatch fails before any DDL runs.
func TestTask_CheckImport2PCSetup(t *testing.T) {
	replicatingCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{{ClusterId: "a"}, {ClusterId: "b"}},
	}

	newTaskWithSecondaries := func(secondaries []string, grpcCli *milvus.MockGrpc) *Task {
		task := newTestTask()
		task.args.Option = &Option{SecondaryEndpoints: secondaries}
		task.grpc = grpcCli
		return task
	}

	t.Run("SecondariesOnOldServerFails", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.Import2PC).Return(false)

		err := newTaskWithSecondaries([]string{"http://secondary:19530"}, grpcCli).checkImport2PCSetup(t.Context())
		assert.ErrorContains(t, err, "3.0")
	})

	// An older server has neither the commit endpoint nor the rejection of
	// auto-commit imports, so nothing is probed.
	t.Run("NoSecondariesOnOldServerSkips", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.Import2PC).Return(false)

		assert.NoError(t, newTaskWithSecondaries(nil, grpcCli).checkImport2PCSetup(t.Context()))
	})

	t.Run("SecondariesWithoutReplicationFails", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.Import2PC).Return(true)
		grpcCli.EXPECT().GetReplicateConfiguration(mock.Anything).
			Return(&commonpb.ReplicateConfiguration{}, nil)

		err := newTaskWithSecondaries([]string{"http://secondary:19530"}, grpcCli).checkImport2PCSetup(t.Context())
		assert.ErrorContains(t, err, "not part of a replication topology")
	})

	t.Run("SecondariesWithReplicationOK", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.Import2PC).Return(true)
		grpcCli.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicatingCfg, nil)

		assert.NoError(t, newTaskWithSecondaries([]string{"http://secondary:19530"}, grpcCli).checkImport2PCSetup(t.Context()))
	})

	t.Run("NoSecondariesOnReplicatingServerFails", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.Import2PC).Return(true)
		grpcCli.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicatingCfg, nil)

		err := newTaskWithSecondaries(nil, grpcCli).checkImport2PCSetup(t.Context())
		assert.ErrorContains(t, err, "--secondaries")
	})

	// Without secondaries the replication probe is best-effort: a target that
	// cannot be asked restores as before, with a warning.
	t.Run("UnreadableReplicationConfig", func(t *testing.T) {
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.Import2PC).Return(true)
		grpcCli.EXPECT().GetReplicateConfiguration(mock.Anything).
			Return(nil, errors.New("permission denied"))

		assert.NoError(t, newTaskWithSecondaries(nil, grpcCli).checkImport2PCSetup(t.Context()))

		err := newTaskWithSecondaries([]string{"http://secondary:19530"}, grpcCli).checkImport2PCSetup(t.Context())
		assert.ErrorContains(t, err, "permission denied")
	})
}

func TestTask_CheckCollExist(t *testing.T) {
	testCases := []struct {
		has        bool
		skipCreate bool
		dropExist  bool

		ok bool
	}{
		{has: true, skipCreate: false, dropExist: false, ok: false},
		{has: true, skipCreate: true, dropExist: false, ok: true},
		{has: true, skipCreate: false, dropExist: true, ok: true},
		{has: true, skipCreate: true, dropExist: true, ok: false},
		{has: false, skipCreate: false, dropExist: false, ok: true},
		{has: false, skipCreate: true, dropExist: false, ok: false},
		{has: false, skipCreate: false, dropExist: true, ok: true},
		{has: false, skipCreate: true, dropExist: true, ok: false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("has:%v, skipCreate:%v, dropExist:%v", tc.has, tc.skipCreate, tc.dropExist), func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			opt := &Option{SkipCreateCollection: tc.skipCreate, DropExistCollection: tc.dropExist}

			cli.EXPECT().HasCollection(mock.Anything, "db1", "coll1").Return(tc.has, nil).Once()

			rt := &Task{args: TaskArgs{Option: opt}, grpc: cli, logger: zap.NewNop()}
			err := rt.checkCollExist(context.Background(), collref.New("db1", "coll1"))
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestTask_filterDBBackup(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		dbBackup := []*backuppb.DatabaseBackupInfo{{DbName: "db1"}, {DbName: "db2"}, {DbName: "db3"}}
		assert.ElementsMatch(t, dbBackup, task.filterDBBackup(dbBackup))
	})

	t.Run("Filter", func(t *testing.T) {
		plan := &Plan{BackupFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
			"db3": {AllowAll: true},
		}}}
		dbBackup := []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db2"},
			{DbName: "db3"},
		}
		expect := []*backuppb.DatabaseBackupInfo{
			{DbName: "db1"},
			{DbName: "db3"},
		}
		task := &Task{args: TaskArgs{Plan: plan}}
		assert.ElementsMatch(t, expect, task.filterDBBackup(dbBackup))
	})
}

func TestTask_filterCollBackup(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}

		assert.ElementsMatch(t, collBackup, task.filterCollBackup(collBackup))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &Plan{BackupFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{
				"coll1": {},
			}},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}
		expect := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
		}
		assert.ElementsMatch(t, expect, task.filterCollBackup(collBackup))
	})

	t.Run("AllowAll", func(t *testing.T) {
		p := &Plan{BackupFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collBackup := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
			{DbName: "db2", CollectionName: "coll1"},
		}
		expect := []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "coll1"},
			{DbName: "db1", CollectionName: "coll2"},
		}
		assert.ElementsMatch(t, expect, task.filterCollBackup(collBackup))
	})
}

func TestTask_filterDBTask(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		dbTasks := []*databaseTask{
			{targetName: "db1"},
			{targetName: "db2"},
			{targetName: "db3"},
		}
		assert.ElementsMatch(t, dbTasks, task.filterDBTask(dbTasks))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &Plan{TaskFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
			"db3": {AllowAll: true},
		}}}
		task := newTestTask()
		task.args.Plan = p
		dbTasks := []*databaseTask{
			{targetName: "db1"},
			{targetName: "db2"},
			{targetName: "db3"},
		}
		expect := []*databaseTask{
			{targetName: "db1"},
			{targetName: "db3"},
		}
		assert.ElementsMatch(t, expect, task.filterDBTask(dbTasks))
	})
}

func TestTask_filterCollTask(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		task := newTestTask()
		collTargets := []collTarget{
			{target: collref.New("db1", "coll1")},
			{target: collref.New("db1", "coll2")},
			{target: collref.New("db2", "coll1")},
		}
		assert.ElementsMatch(t, collTargets, task.filterCollTask(collTargets))
	})

	t.Run("Filter", func(t *testing.T) {
		p := &Plan{TaskFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}}},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collTargets := []collTarget{
			{target: collref.New("db1", "coll1")},
			{target: collref.New("db1", "coll2")},
			{target: collref.New("db2", "coll1")},
		}
		expect := []collTarget{
			{target: collref.New("db1", "coll1")},
		}
		assert.ElementsMatch(t, expect, task.filterCollTask(collTargets))
	})

	t.Run("AllowAll", func(t *testing.T) {
		p := &Plan{TaskFilter: filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {AllowAll: true},
		}}}
		task := newTestTask()
		task.args.Plan = p
		collTargets := []collTarget{
			{target: collref.New("db1", "coll1")},
			{target: collref.New("db1", "coll2")},
			{target: collref.New("db2", "coll1")},
		}
		expect := []collTarget{
			{target: collref.New("db1", "coll1")},
			{target: collref.New("db1", "coll2")},
		}
		assert.ElementsMatch(t, expect, task.filterCollTask(collTargets))
	})
}

func TestTask_newDBTask(t *testing.T) {
	t.Run("NoMapping", func(t *testing.T) {
		task := newTestTask()
		dbBak := &backuppb.DatabaseBackupInfo{DbName: "db1"}
		dbTasks := task.newDBTask(dbBak)
		assert.Len(t, dbTasks, 1)
		assert.Equal(t, "db1", dbTasks[0].targetName)
	})

	t.Run("WithMapping", func(t *testing.T) {
		p := &Plan{DBMapper: map[string][]DBMapping{"db1": {{Target: "db2"}, {Target: "db3"}}}}
		task := newTestTask()
		task.args.Plan = p
		dbBak := &backuppb.DatabaseBackupInfo{DbName: "db1"}
		dbTasks := task.newDBTask(dbBak)
		assert.Len(t, dbTasks, 2)
		names := lo.Map(dbTasks, func(task *databaseTask, _ int) string { return task.targetName })
		assert.ElementsMatch(t, []string{"db2", "db3"}, names)
	})
}

func TestTask_newCollTarget(t *testing.T) {
	mapper := NewMockCollMapper(t)

	collRef := collref.New("db1", "coll1")
	mapper.EXPECT().TargetNames(collRef).Return([]collref.Name{
		collref.New("db2", "coll2"),
		collref.New("db3", "coll3"),
	}).Once()

	task := newTestTask()
	task.args.Plan = &Plan{CollMapper: mapper}
	mgr := jobstate.NewStore()
	mgr.AddRestoreTask("task1")
	task.args.TaskID = "task1"
	task.args.Store = mgr

	dbBackup := &backuppb.DatabaseBackupInfo{DbName: "db1"}
	collBackup := &backuppb.CollectionBackupInfo{DbName: "db1", CollectionName: "coll1"}
	targets := task.newCollTarget(dbBackup, collBackup)
	assert.Len(t, targets, 2)
	names := lo.Map(targets, func(tgt collTarget, _ int) string { return tgt.target.String() })
	assert.ElementsMatch(t, []string{"db2.coll2", "db3.coll3"}, names)
}

func TestDefaultRenamer(t *testing.T) {
	r := NewDefaultCollMapper()

	collRef := collref.New("db1", "coll1")
	assert.ElementsMatch(t, []collref.Name{collRef}, r.TargetNames(collRef))
}

func TestSuffixRenamer(t *testing.T) {
	r := NewSuffixMapper("_bak")
	collRef := collref.New("db1", "coll1")
	expect := []collref.Name{collref.New("db1", "coll1_bak")}
	assert.ElementsMatch(t, expect, r.TargetNames(collRef))
}

func TestMapRenamer(t *testing.T) {
	t.Run("DBWildcard", func(t *testing.T) {
		r := &TableMapper{DBWildcard: map[string]string{"db1": "db2"}}

		expect := []collref.Name{collref.New("db2", "coll1")}
		in := collref.New("db1", "coll1")
		assert.ElementsMatch(t, expect, r.TargetNames(in))

		expect = []collref.Name{collref.New("db2", "coll2")}
		in = collref.New("db1", "coll2")
		assert.ElementsMatch(t, expect, r.TargetNames(in))

		expect = []collref.Name{collref.New("db3", "coll1")}
		in = collref.New("db3", "coll1")
		assert.Equal(t, expect, r.TargetNames(in))
	})

	t.Run("CollMapper", func(t *testing.T) {
		r := &TableMapper{NameMapping: map[string][]collref.Name{
			"db1.coll1": {
				collref.New("db2", "coll2"),
				collref.New("db3", "coll3"),
			},
		}}

		expect := []collref.Name{
			collref.New("db2", "coll2"),
			collref.New("db3", "coll3"),
		}
		in := collref.New("db1", "coll1")
		assert.ElementsMatch(t, expect, r.TargetNames(in))

		expect = []collref.Name{collref.New("db1", "coll2")}
		in = collref.New("db1", "coll2")
		assert.ElementsMatch(t, expect, r.TargetNames(in))
	})
}
