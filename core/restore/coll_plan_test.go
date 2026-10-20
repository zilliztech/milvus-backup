package restore

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// newSelectPlanTask builds a task whose plan selection sees the given format
// and strategy pin, with no grpc client behind it: a case that reaches the
// client without setting one up fails the mock's unknown-call panic, which is
// the assertion that auto on a snapshot backup never probes the target.
func newSelectPlanTask(format string, strategy Strategy) *Task {
	task := newTestTask()
	task.format = format
	task.args.Option = &Option{Strategy: strategy}
	return task
}

// The plan is picked by the backup's format and the strategy: the format
// narrows which strategies exist, auto resolves a binlog backup against the
// target's v2 import support, and a pin outside the format's set is a loud
// error rather than a silent fallback.
func TestTask_selectPlan(t *testing.T) {
	// The two binlog data paths are one binlogPlan now; which import api the
	// plan runs on lives in the planner its factory mints.
	assertBinlogPlan := func(t *testing.T, plan collPlan, planner any) {
		bp, ok := plan.(*binlogPlan)
		require.True(t, ok, "plan is %T, want *binlogPlan", plan)
		assert.IsType(t, planner, bp.newPlanner(importTaskArgs{}))
	}

	t.Run("BinlogAutoUsesV2WhenSupported", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatBinlog, StrategyAuto)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportV2).Return(true)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(nil)
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &restfulImportPlanner{})
	})

	t.Run("BinlogAutoFallsBackToGrpc", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatBinlog, StrategyAuto)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.ImportV2).Return(false)
		task.grpc = grpcCli

		plan, err := task.selectPlan(nil)
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &grpcImportPlanner{})
	})

	t.Run("BinlogPerSeg", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatBinlog, StrategyPerSeg)

		plan, err := task.selectPlan(nil)
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &grpcImportPlanner{})
	})

	t.Run("BinlogMergedSeg", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatBinlog, StrategyMergedSeg)
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(nil)
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &restfulImportPlanner{})
	})

	t.Run("BinlogRejectsSnapshotStrategies", func(t *testing.T) {
		for _, strategy := range []Strategy{StrategyRestoreSnap, StrategyImportSnap} {
			_, err := newSelectPlanTask(meta.FormatBinlog, strategy).selectPlan(nil)
			assert.ErrorContains(t, err, "snapshot")
		}
	})

	t.Run("SnapshotAutoUsesRestoreApi", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatSnapshot, StrategyAuto)

		plan, err := task.selectPlan(nil)
		require.NoError(t, err)
		assert.IsType(t, &restoreSnapPlan{}, plan)
	})

	t.Run("SnapshotRestoreSnap", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatSnapshot, StrategyRestoreSnap)

		plan, err := task.selectPlan(nil)
		require.NoError(t, err)
		assert.IsType(t, &restoreSnapPlan{}, plan)
	})

	t.Run("SnapshotImportSnapNotImplemented", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatSnapshot, StrategyImportSnap)

		_, err := task.selectPlan(nil)
		assert.ErrorContains(t, err, "not implemented")
	})

	t.Run("SnapshotRejectsBinlogStrategies", func(t *testing.T) {
		for _, strategy := range []Strategy{StrategyPerSeg, StrategyMergedSeg} {
			_, err := newSelectPlanTask(meta.FormatSnapshot, strategy).selectPlan(nil)
			assert.ErrorContains(t, err, "binlog")
		}
	})

	// A peer commit exists only on the restful import api, so auto resolves to
	// it directly — the preflight has already established the target is new
	// enough — and a per_seg pin is refused rather than silently losing the
	// two-phase commit.
	t.Run("BinlogAutoWithPeerUsesRestful", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatBinlog, StrategyAuto)
		task.args.Option.PeerEndpoints = []string{"http://peer:19530"}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Maybe()
		task.grpc = grpcCli

		plan, err := task.selectPlan(nil)
		require.NoError(t, err)
		assertBinlogPlan(t, plan, &restfulImportPlanner{})
	})

	t.Run("BinlogPerSegWithPeerFails", func(t *testing.T) {
		task := newSelectPlanTask(meta.FormatBinlog, StrategyPerSeg)
		task.args.Option.PeerEndpoints = []string{"http://peer:19530"}

		_, err := task.selectPlan(nil)
		assert.ErrorContains(t, err, "--peers")
	})
}

// The shell registers the collection when it is built and walks it through
// executing → success/fail as the flow runs, whatever the flow is.
func TestNewCollPlanTask(t *testing.T) {
	newStore := func() *jobstate.Store {
		store := jobstate.NewStore()
		store.AddRestoreTask("task-1")
		return store
	}
	target := collref.New("db1", "coll1")

	t.Run("RegistersAndMarksSuccess", func(t *testing.T) {
		store := newStore()
		shell := newCollPlanTask(store, "task-1", target, 4096, func(ctx context.Context) error { return nil })
		require.NoError(t, shell(context.Background()))

		view, err := store.GetRestoreTask("task-1")
		require.NoError(t, err)
		coll := view.CollTasks()[target]
		assert.Equal(t, backuppb.RestoreTaskStateCode_SUCCESS, coll.StateCode())
		assert.Equal(t, int64(4096), coll.TotalSize())
	})

	t.Run("MarksFailAndReturnsError", func(t *testing.T) {
		store := newStore()
		shell := newCollPlanTask(store, "task-1", target, 0, func(ctx context.Context) error { return errors.New("boom") })
		err := shell(context.Background())
		assert.ErrorContains(t, err, "boom")

		view, verr := store.GetRestoreTask("task-1")
		require.NoError(t, verr)
		coll := view.CollTasks()[target]
		assert.Equal(t, backuppb.RestoreTaskStateCode_FAIL, coll.StateCode())
		assert.Contains(t, coll.ErrorMessage(), "boom")
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
