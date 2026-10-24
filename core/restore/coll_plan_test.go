package restore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// newSelectPlanTask builds a task whose plan selection sees the given format
// and strategy pin, with the wiring plan construction touches: both storages
// and the job tracker from newTestTask. The storage clients are local-provider
// ones — constructing them never dials, and their existence probes of the
// backup dir simply find nothing.
func newSelectPlanTask(t *testing.T, format string, strategy Strategy) *Task {
	task := newTestTask()
	task.format = format
	task.args.Option = &Option{Strategy: strategy}
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
		bp, ok := plan.(*binlogPlan)
		require.True(t, ok, "plan is %T, want *binlogPlan", plan)
		require.Len(t, bp.colls, 1)
		assert.IsType(t, &collDDLTask{}, bp.colls[0].ddl)
		require.Len(t, bp.colls[0].parts, 1)
		require.Len(t, bp.colls[0].parts[0].leveled, 1)
		assert.IsType(t, importTask, bp.colls[0].parts[0].leveled[0])
	}

	t.Run("BinlogAutoUsesV2WhenSupported", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyAuto)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportV2).Return(true)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaRESTFulTask{})
	})

	t.Run("BinlogAutoFallsBackToGrpc", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyAuto)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportV2).Return(false)
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaGRPCTask{})
	})

	t.Run("BinlogPerSeg", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyPerSeg)

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaGRPCTask{})
	})

	t.Run("BinlogMergedSeg", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyMergedSeg)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaRESTFulTask{})
	})

	t.Run("BinlogMetaOnlyBuildsNoDataTasks", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyPerSeg)
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
			_, err := newSelectPlanTask(t, meta.FormatBinlog, strategy).selectPlan(t.Context(), nil)
			assert.ErrorContains(t, err, "snapshot")
		}
	})

	t.Run("SnapshotAutoUsesRestoreApi", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyAuto)

		plan, err := task.selectPlan(t.Context(), nil)
		require.NoError(t, err)
		assert.IsType(t, &restoreSnapPlan{}, plan)
	})

	t.Run("SnapshotRestoreSnap", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyRestoreSnap)

		plan, err := task.selectPlan(t.Context(), nil)
		require.NoError(t, err)
		assert.IsType(t, &restoreSnapPlan{}, plan)
	})

	// The import plan is one DDL head and one snapshot import job per
	// collection: the api expands the bundle's partitions and segments itself.
	t.Run("SnapshotImportSnap", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyImportSnap)
		task.snapshotSource = snapshotSource{dirURI: "s3://backup-bucket/backup/mybackup"}
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
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json", imp.metadataURI)
		assert.JSONEq(t, `{"p1": "p1"}`, imp.partitionMapping)
	})

	t.Run("SnapshotImportSnapNeedsNewerServer", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyImportSnap)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportSnapshot).Return(false)
		task.grpc = grpcCli

		_, err := task.selectPlan(t.Context(), oneSnapCollTargets())
		assert.ErrorContains(t, err, "3.0.4")
	})

	t.Run("SnapshotImportSnapMetaOnlyBuildsNoDataTasks", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatSnapshot, StrategyImportSnap)
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
			_, err := newSelectPlanTask(t, meta.FormatSnapshot, strategy).selectPlan(t.Context(), nil)
			assert.ErrorContains(t, err, "binlog")
		}
	})

	// A secondary commit exists only on the restful import api, so auto
	// resolves to it directly — the preflight has already established the
	// target is new enough — and a per_seg pin is refused rather than silently
	// losing the two-phase commit.
	t.Run("BinlogAutoWithSecondariesUsesRestful", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyAuto)
		task.args.Option.SecondaryEndpoints = []string{"http://secondary:19530"}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(t.Context(), oneCollTargets())
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &importViaRESTFulTask{})
	})

	t.Run("BinlogPerSegWithSecondariesFails", func(t *testing.T) {
		task := newSelectPlanTask(t, meta.FormatBinlog, StrategyPerSeg)
		task.args.Option.SecondaryEndpoints = []string{"http://secondary:19530"}

		_, err := task.selectPlan(t.Context(), nil)
		assert.ErrorContains(t, err, "--secondaries")
	})
}

func TestTask_needStaging(t *testing.T) {
	newClient := func(bucket string) storage.Client {
		cli, err := storage.NewClient(context.Background(), storage.Config{
			Provider:   cfg.ProviderMinio,
			Endpoint:   "localhost:9000",
			Bucket:     bucket,
			Credential: storage.Credential{Type: storage.Static, AK: "a", SK: "b"},
		})
		require.NoError(t, err)
		return cli
	}
	newTask := func(backupBucket, milvusBucket string, streaming bool) *Task {
		task := newTestTask()
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
		task := newTestTask()
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
