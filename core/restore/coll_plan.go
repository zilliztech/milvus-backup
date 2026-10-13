package restore

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
)

// collTarget is one collection the restore task will land: the backup's view
// of it, the (possibly renamed) target it lands on, and that target's
// overrides.
type collTarget struct {
	dbBackup     *backuppb.DatabaseBackupInfo
	collBackup   *backuppb.CollectionBackupInfo
	target       collref.Name
	collOverride CollOverride
}

// Strategy pins the data path a restore takes. Every value names both the
// backup format it applies to and the path it selects, so no value is
// ambiguous across formats: per_seg and merged_seg are binlog strategies,
// restore_snap and import_snap are snapshot ones. What carries the data in
// the first place is the backup's format, the other axis the task resolves
// at start.
//
//go:generate stringer -type=Strategy
type Strategy int

const (
	StrategyAuto Strategy = iota
	StrategyPerSeg
	StrategyMergedSeg
	StrategyRestoreSnap
	StrategyImportSnap
)

// collPlan restores every planned collection one way: from snapshot bundles
// through Milvus's restore api, or from binlog through the v1 grpc or v2
// restful import api. The task picks one plan per restore. The plans differ in
// what restoring one collection is — the snapshot path hands the whole
// collection to Milvus with no DDL of our own, the binlog paths run the DDL
// then the DML — not in how collections are scheduled among themselves.
type collPlan interface {
	Execute(ctx context.Context) error
}

// selectPlan composes the two option axes into the plan that runs: the
// backup's format narrows which strategies exist, and the strategy pin — or
// auto, resolved against the target's capabilities — picks among what remains.
// A pin outside the format's set is a loud error, not a silent fallback. It is
// the restore counterpart of the backup task's plan selection: there the flush
// strategy picks the orchestration, here the data path picks it.
func (t *Task) selectPlan(targets []collTarget) (collPlan, error) {
	switch strategy := t.args.Option.Strategy; strategy {
	case StrategyAuto:
		if t.format == meta.FormatSnapshot {
			// importSnapPlan has not landed, so a snapshot backup rides the
			// restore api regardless of the target's import support.
			t.logger.Info("use restore snap plan")
			return newRestoreSnapPlan(t, targets), nil
		}
		if t.grpc.HasFeature(milvus.ImportV2) {
			t.logger.Info("use binlog restful plan")
			return newBinlogRESTFulPlan(t, targets), nil
		}
		t.logger.Info("use binlog grpc plan")
		return newBinlogGRPCPlan(t, targets), nil
	case StrategyPerSeg:
		if t.format != meta.FormatBinlog {
			return nil, wrongFormat("per_seg", meta.FormatBinlog, t.format)
		}
		t.logger.Info("use binlog grpc plan")
		return newBinlogGRPCPlan(t, targets), nil
	case StrategyMergedSeg:
		if t.format != meta.FormatBinlog {
			return nil, wrongFormat("merged_seg", meta.FormatBinlog, t.format)
		}
		t.logger.Info("use binlog restful plan")
		return newBinlogRESTFulPlan(t, targets), nil
	case StrategyRestoreSnap:
		if t.format != meta.FormatSnapshot {
			return nil, wrongFormat("restore_snap", meta.FormatSnapshot, t.format)
		}
		t.logger.Info("use restore snap plan")
		return newRestoreSnapPlan(t, targets), nil
	case StrategyImportSnap:
		if t.format != meta.FormatSnapshot {
			return nil, wrongFormat("import_snap", meta.FormatSnapshot, t.format)
		}
		return nil, errors.New("restore: strategy import_snap is not implemented yet")
	default:
		return nil, fmt.Errorf("restore: unsupported strategy: %s", strategy)
	}
}

// wrongFormat rejects a strategy pin aimed at the other backup format.
func wrongFormat(strategy, want, got string) error {
	return fmt.Errorf("restore: strategy %s restores %s backups, but this backup was taken in the %s format", strategy, formatWord(want), formatWord(got))
}

// formatWord names a format for humans: the binlog format is the empty string,
// which reads badly in an error message.
func formatWord(format string) string {
	if format == meta.FormatBinlog {
		return "binlog"
	}

	return format
}

// collPlanTask restores one collection inside a plan.
type collPlanTask func(ctx context.Context) error

// runCollPlanTasks runs the per-collection tasks of a plan concurrently,
// capped by the collection concurrency.
func runCollPlanTasks(ctx context.Context, limit int, logger *zap.Logger, tasks []collPlanTask) error {
	logger.Info("start restore collection")

	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	for _, task := range tasks {
		g.Go(func() error {
			if err := task(subCtx); err != nil {
				return fmt.Errorf("restore: restore collection %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore: wait restore collections %w", err)
	}

	logger.Info("finish restore all collections")
	return nil
}

// newCollDMLTaskArgs assembles the DML task's arguments for one target; the
// same wiring serves both binlog plans. A local target resolves its storage
// directory two ways: the path milvus-backup reaches it at (rootPath) and the
// path the Milvus process itself resolves (localPath, falling back to
// rootPath).
func (t *Task) newCollDMLTaskArgs(tgt collTarget) collDMLTaskArgs {
	milvusLocalPath := t.args.Params.Milvus.Storage.LocalPath.Val
	if milvusLocalPath == "" {
		milvusLocalPath = t.args.Params.Milvus.Storage.RootPath.Val
	}

	return collDMLTaskArgs{
		taskID: t.args.TaskID,
		store:  t.args.Store,
		target: tgt.target,

		dbBackup:   tgt.dbBackup,
		collBackup: tgt.collBackup,

		option: t.args.Option,

		streaming:     t.streaming,
		keepTempFiles: t.args.Params.Restore.KeepTempFiles.Val,
		backupDir:     t.args.BackupDir,

		backupStorage: t.args.BackupStorage,
		milvusStorage: t.args.MilvusStorage,

		milvusRootPath:  t.args.Params.Milvus.Storage.RootPath.Val,
		milvusLocalPath: milvusLocalPath,

		copySem:       t.copySem,
		bulkInsertSem: t.bulkInsertSem,

		grpcCli:    t.grpc,
		restfulCli: t.restful,

		maxSegsPerImportJob: t.args.Params.Restore.MaxSegmentsPerImportJob.Val,
	}
}

// newBinlogCollPlanTask wraps one collection's binlog restore — DDL, then
// DML — with its task-manager state transitions.
func newBinlogCollPlanTask(store *jobstate.Store, taskID string, target collref.Name, ddl *collDDLTask, dml *collDMLTask) collPlanTask {
	logger := log.With(
		zap.String("restore_task_id", taskID),
		zap.String("target_coll", target.String()))

	return func(ctx context.Context) error {
		store.UpdateRestoreTask(taskID, jobstate.SetRestoreCollExecuting(target))

		if err := ddl.Execute(ctx); err != nil {
			err := fmt.Errorf("restore_collection: restore collection ddl: %w", err)
			logger.Error("restore collection failed", zap.Error(err))
			store.UpdateRestoreTask(taskID, jobstate.SetRestoreCollFail(target, err))
			return err
		}

		if err := dml.Execute(ctx); err != nil {
			err := fmt.Errorf("restore_collection: restore collection data: %w", err)
			logger.Error("restore collection failed", zap.Error(err))
			store.UpdateRestoreTask(taskID, jobstate.SetRestoreCollFail(target, err))
			return err
		}

		logger.Info("restore collection success")
		store.UpdateRestoreTask(taskID, jobstate.SetRestoreCollSuccess(target))
		return nil
	}
}

// binlogGRPCPlan restores every collection from a binlog backup through the
// v1 grpc bulk insert api: each collection runs its DDL, then its DML, one
// import job per partition dir.
type binlogGRPCPlan struct {
	tasks []collPlanTask
	limit int

	logger *zap.Logger
}

// newBinlogGRPCPlan expands each target into its per-collection restore — the
// DDL first, then the DML importing every partition dir as its own job through
// the v1 grpc bulk insert api — and reports each collection's state
// transitions.
func newBinlogGRPCPlan(t *Task, targets []collTarget) *binlogGRPCPlan {
	tasks := make([]collPlanTask, 0, len(targets))
	for _, tgt := range targets {
		ddl := newCollDDLTask(t.args.TaskID, t.args.Option, tgt.collOverride, tgt.collBackup, tgt.target, t.grpc)

		dml := newCollDMLTask(t.newCollDMLTaskArgs(tgt))
		dml.importer = newGRPCImportPlanner(dml)

		tasks = append(tasks, newBinlogCollPlanTask(t.args.Store, t.args.TaskID, tgt.target, ddl, dml))
	}

	return &binlogGRPCPlan{
		tasks: tasks,
		limit: t.args.Params.Restore.Concurrency.Collections.Val,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}
}

func (p *binlogGRPCPlan) Execute(ctx context.Context) error {
	return runCollPlanTasks(ctx, p.limit, p.logger, p.tasks)
}

// binlogRESTFulPlan restores every collection from a binlog backup through the
// v2 restful bulk insert api: each collection runs its DDL, then its DML, one
// import job per chunk of dirs.
type binlogRESTFulPlan struct {
	tasks []collPlanTask
	limit int

	logger *zap.Logger
}

// newBinlogRESTFulPlan expands each target into its per-collection restore —
// the DDL first, then the DML importing chunks of dirs through the v2 restful
// bulk insert api — and reports each collection's state transitions.
func newBinlogRESTFulPlan(t *Task, targets []collTarget) *binlogRESTFulPlan {
	// whether the target takes several L0 segments in one import job (2.6.5+)
	// rides the grpc client's version handshake.
	multiL0InOneJob := t.grpc.HasFeature(milvus.MultiL0InOneJob)

	tasks := make([]collPlanTask, 0, len(targets))
	for _, tgt := range targets {
		ddl := newCollDDLTask(t.args.TaskID, t.args.Option, tgt.collOverride, tgt.collBackup, tgt.target, t.grpc)

		dml := newCollDMLTask(t.newCollDMLTaskArgs(tgt))
		dml.importer = newRestfulImportPlanner(dml, multiL0InOneJob)

		tasks = append(tasks, newBinlogCollPlanTask(t.args.Store, t.args.TaskID, tgt.target, ddl, dml))
	}

	return &binlogRESTFulPlan{
		tasks: tasks,
		limit: t.args.Params.Restore.Concurrency.Collections.Val,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}
}

func (p *binlogRESTFulPlan) Execute(ctx context.Context) error {
	return runCollPlanTasks(ctx, p.limit, p.logger, p.tasks)
}

// restoreSnapPlan restores every collection from a snapshot backup by handing
// it to Milvus's restore api: Milvus creates the collection from the schema in
// the bundle, restores its indexes and partitions, and copies the data in. No
// DDL runs on our side, and no bytes move through this process. A restful
// sibling (importSnapPlan) will land beside it.
type restoreSnapPlan struct {
	tasks []collPlanTask
	limit int

	logger *zap.Logger
}

func newRestoreSnapPlan(t *Task, targets []collTarget) *restoreSnapPlan {
	tasks := make([]collPlanTask, 0, len(targets))
	for _, tgt := range targets {
		st := newCollSnapshotTask(collSnapshotTaskArgs{
			taskID:           t.args.TaskID,
			collBackup:       tgt.collBackup,
			target:           tgt.target,
			source:           t.snapshotSource,
			dropExist:        t.args.Option.DropExistCollection,
			maxShardNum:      t.args.Option.MaxShardNum,
			shardNumOverride: tgt.collOverride.ShardNum,
			descOverride:     tgt.collOverride.Description,
			skipParams:       t.args.Option.SkipParams,
			grpcCli:          t.grpc,
			store:            t.args.Store,
		})
		tasks = append(tasks, st.Execute)
	}

	return &restoreSnapPlan{
		tasks: tasks,
		limit: t.args.Params.Restore.Concurrency.Collections.Val,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}
}

func (p *restoreSnapPlan) Execute(ctx context.Context) error {
	return runCollPlanTasks(ctx, p.limit, p.logger, p.tasks)
}
