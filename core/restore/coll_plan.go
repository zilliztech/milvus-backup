package restore

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/tasklet"
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
// through Milvus's restore api, or from binlog through an import api. The task
// picks one plan per restore. The plans differ in what restoring one
// collection is — the snapshot path hands the whole collection to Milvus with
// no DDL of our own, the binlog path runs the DDL then imports the binlogs —
// not in how collections are scheduled among themselves.
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
	// A peer commit rides the restful import api: the v1 grpc import has no
	// two-phase commit at all. Auto resolves to the restful plan directly — the
	// preflight has already established the target is new enough.
	peerSet := len(t.args.Option.PeerEndpoints) != 0

	switch strategy := t.args.Option.Strategy; strategy {
	case StrategyAuto:
		if t.format == meta.FormatSnapshot {
			// importSnapPlan has not landed, so a snapshot backup rides the
			// restore api regardless of the target's import support.
			t.logger.Info("use restore snap plan")
			return newRestoreSnapPlan(t, targets), nil
		}
		if peerSet {
			t.logger.Info("use binlog restful plan with two-phase commit")
			return newBinlogPlan(t, targets, t.restfulPlannerFactory()), nil
		}
		if t.grpc.HasFeature(milvus.ImportV2) {
			t.logger.Info("use binlog restful plan")
			return newBinlogPlan(t, targets, t.restfulPlannerFactory()), nil
		}
		t.logger.Info("use binlog grpc plan")
		return newBinlogPlan(t, targets, grpcPlannerFactory), nil
	case StrategyPerSeg:
		if t.format != meta.FormatBinlog {
			return nil, wrongFormat("per_seg", meta.FormatBinlog, t.format)
		}
		if peerSet {
			return nil, errors.New("restore: --peers needs the restful import api for its two-phase " +
				"commit, but the per_seg strategy imports through the v1 grpc api; use merged_seg or auto")
		}
		t.logger.Info("use binlog grpc plan")
		return newBinlogPlan(t, targets, grpcPlannerFactory), nil
	case StrategyMergedSeg:
		if t.format != meta.FormatBinlog {
			return nil, wrongFormat("merged_seg", meta.FormatBinlog, t.format)
		}
		t.logger.Info("use binlog restful plan")
		return newBinlogPlan(t, targets, t.restfulPlannerFactory()), nil
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

// newImportTaskArgs assembles the import wiring of one target collection; the
// same wiring serves both binlog plans. A local target resolves its storage
// directory two ways: the path milvus-backup reaches it at (rootPath) and the
// path the Milvus process itself resolves (localPath, falling back to
// rootPath). Whether the restore stages data is a job-level invariant — both
// storages and the streaming flag are fixed for the whole job — so the
// decision is made here, once per collection, and the import tasks only read
// it.
func (t *Task) newImportTaskArgs(tgt collTarget) importTaskArgs {
	milvusLocalPath := t.args.Params.Milvus.Storage.LocalPath.Val
	if milvusLocalPath == "" {
		milvusLocalPath = t.args.Params.Milvus.Storage.RootPath.Val
	}

	src := collref.New(tgt.collBackup.GetDbName(), tgt.collBackup.GetCollectionName())
	logger := log.With(
		zap.String("restore_task_id", t.args.TaskID),
		zap.String("backup_coll", src.String()),
		zap.String("target_coll", tgt.target.String()))

	return importTaskArgs{
		taskID: t.args.TaskID,
		target: tgt.target,
		ezk:    t.mappedEZK(tgt.dbBackup),

		staging:       t.needStaging(),
		backupDir:     t.args.BackupDir,
		backupStorage: t.args.BackupStorage,
		copySem:       t.copySem,

		keepTempFiles:       t.args.Params.Restore.KeepTempFiles.Val,
		maxSegsPerImportJob: t.args.Params.Restore.MaxSegmentsPerImportJob.Val,

		milvusStorage:   t.args.MilvusStorage,
		milvusRootPath:  t.args.Params.Milvus.Storage.RootPath.Val,
		milvusLocalPath: milvusLocalPath,

		grpcCli:    t.grpc,
		restfulCli: t.restful,

		store:  t.args.Store,
		logger: logger,
	}
}

// needStaging decides whether the backup's data must be copied into the
// target's storage before the import: when backup and target share bucket and
// provider and the transfer is not streamed, Milvus imports the backup in
// place. The inputs are job-level invariants, so the tasks below only read the
// answer this computes once per collection.
func (t *Task) needStaging() bool {
	sameService := t.args.BackupStorage.Config().Bucket == t.args.MilvusStorage.Config().Bucket &&
		t.args.BackupStorage.Config().Provider == t.args.MilvusStorage.Config().Provider
	return !sameService || t.streaming
}

// mappedEZK resolves the EZK the target imports with: the option's mapping
// wins over the key the backup's database was encrypted with, which is the
// fallback. An unencrypted backup has no key to map.
func (t *Task) mappedEZK(dbBackup *backuppb.DatabaseBackupInfo) string {
	oldEZK := dbBackup.GetEzk()
	if oldEZK == "" {
		return ""
	}

	if len(t.args.Option.EZKMapping) > 0 {
		if newEZK, ok := t.args.Option.EZKMapping[oldEZK]; ok {
			return newEZK
		}
	}

	return oldEZK
}

// newCollPlanTask wraps one collection's restore flow with its task-manager
// registration and state transitions, so every plan's per-collection task
// reports the same way and the flow itself only does the work. Registration
// happens here, at plan build time: it declares the collections the restore
// will touch before anything runs.
func newCollPlanTask(store *jobstate.Store, taskID string, target collref.Name, size int64, flow func(ctx context.Context) error) collPlanTask {
	logger := log.With(
		zap.String("restore_task_id", taskID),
		zap.String("target_coll", target.String()))

	store.UpdateRestoreTask(taskID, jobstate.AddRestoreCollTask(target, size))

	return func(ctx context.Context) error {
		store.UpdateRestoreTask(taskID, jobstate.SetRestoreCollExecuting(target))

		if err := flow(ctx); err != nil {
			logger.Error("restore collection failed", zap.Error(err))
			store.UpdateRestoreTask(taskID, jobstate.SetRestoreCollFail(target, err))
			return err
		}

		logger.Info("restore collection success")
		store.UpdateRestoreTask(taskID, jobstate.SetRestoreCollSuccess(target))
		return nil
	}
}

// importPlannerFactory mints the per-collection import planner: grpc or
// restful, the only thing the binlog data paths ever differ in.
type importPlannerFactory func(args importTaskArgs) importPlanner

// grpcPlannerFactory plans one import job per partition dir through the v1
// grpc bulk insert api.
func grpcPlannerFactory(args importTaskArgs) importPlanner {
	return newGRPCImportPlanner(args)
}

// restfulPlannerFactory plans chunked import jobs through the v2 restful bulk
// insert api. Whether the target takes several L0 segments in one import job
// (2.6.5+) rides the grpc client's version handshake and is resolved once
// here, not per collection.
func (t *Task) restfulPlannerFactory() importPlannerFactory {
	multiL0InOneJob := t.grpc.HasFeature(milvus.MultiL0InOneJob)
	return func(args importTaskArgs) importPlanner {
		return newRestfulImportPlanner(args, multiL0InOneJob, t.peers)
	}
}

// binlogPlan restores every collection from a binlog backup through an import
// api: each collection runs its DDL, then imports its binlogs, with the
// importPlannerFactory choosing the protocol. The two binlog data paths differ
// only in that factory.
type binlogPlan struct {
	tasks []collPlanTask
	limit int

	// newPlanner is kept after construction: it is the plan's record of which
	// import protocol it runs on.
	newPlanner importPlannerFactory

	logger *zap.Logger
}

// newBinlogPlan expands each target into its per-collection restore — the DDL
// first, then the binlog import through the planner the factory mints.
func newBinlogPlan(t *Task, targets []collTarget, newPlanner importPlannerFactory) *binlogPlan {
	tasks := make([]collPlanTask, 0, len(targets))
	for _, tgt := range targets {
		ddl := newCollDDLTask(t.args.TaskID, t.args.Option, tgt.collOverride, tgt.collBackup, tgt.target, t.grpc)

		src := collref.New(tgt.collBackup.GetDbName(), tgt.collBackup.GetCollectionName())
		logger := log.With(
			zap.String("restore_task_id", t.args.TaskID),
			zap.String("backup_coll", src.String()),
			zap.String("target_coll", tgt.target.String()))

		source := newBinlogSource(tgt.collBackup, t.args.Option, t.args.BackupDir, t.args.BackupStorage, logger)
		planner := newPlanner(t.newImportTaskArgs(tgt))

		// The export-free binlog backup has no bundle size; the collection's
		// declared size is the sum of its partitions'.
		size := lo.SumBy(tgt.collBackup.GetPartitionBackups(), func(part *backuppb.PartitionBackupInfo) int64 {
			return part.GetSize()
		})

		flow := func(ctx context.Context) error {
			if err := ddl.Execute(ctx); err != nil {
				return fmt.Errorf("restore_collection: restore collection ddl: %w", err)
			}
			if err := restoreBinlogData(ctx, t.args.Option, source, planner, t.bulkInsertSem, logger); err != nil {
				return fmt.Errorf("restore_collection: restore collection data: %w", err)
			}
			return nil
		}

		tasks = append(tasks, newCollPlanTask(t.args.Store, t.args.TaskID, tgt.target, size, flow))
	}

	return &binlogPlan{
		tasks:      tasks,
		limit:      t.args.Params.Restore.Concurrency.Collections.Val,
		newPlanner: newPlanner,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}
}

func (p *binlogPlan) Execute(ctx context.Context) error {
	return runCollPlanTasks(ctx, p.limit, p.logger, p.tasks)
}

// restoreBinlogData imports one collection's binlog data: every partition's
// leveled segments first, its L0 segments after — L0 data must land after the
// leveled data it supersedes, so the two runs are ordered barriers rather than
// one concurrent pool — and the collection-global L0 segments last.
func restoreBinlogData(
	ctx context.Context,
	option *Option,
	source *binlogSource,
	planner importPlanner,
	sem tasklet.Semaphore,
	logger *zap.Logger,
) error {
	if option.MetaOnly {
		logger.Info("skip restore data")
		return nil
	}

	// restore all partition segment
	logger.Info("start restore partition segment", zap.Int("partition_num", len(source.collBackup.GetPartitionBackups())))
	g, subCtx := errgroup.WithContext(ctx)
	for _, part := range source.collBackup.GetPartitionBackups() {
		g.Go(func() error {
			if err := restorePartitionBinlogs(subCtx, source, planner, sem, part, logger); err != nil {
				return fmt.Errorf("restore_collection: restore partition: %w", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for partition restore: %w", err)
	}

	// restore all partition l0 segment
	logger.Info("start restore all partition L0 segment", zap.Int("l0_segments", len(source.collBackup.GetL0Segments())))
	l0Groups, err := source.l0DirGroups(source.collBackup.GetL0Segments())
	if err != nil {
		return fmt.Errorf("restore_collection: build L0 dir groups: %w", err)
	}
	l0Tasks := planner.planTasks("", l0Groups)
	if err := tasklet.RunConcurrent(ctx, sem, l0Tasks); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

// restorePartitionBinlogs imports the partition's leveled segments, then its
// L0 segments: L0 data must land after the leveled data it supersedes, so the
// two runs are ordered barriers rather than one concurrent pool.
func restorePartitionBinlogs(
	ctx context.Context,
	source *binlogSource,
	planner importPlanner,
	sem tasklet.Semaphore,
	part *backuppb.PartitionBackupInfo,
	logger *zap.Logger,
) error {
	logger.Info("start restore not L0 segment", zap.String("partition_name", part.GetPartitionName()))
	notL0Groups, err := source.notL0DirGroups(ctx, part)
	if err != nil {
		return fmt.Errorf("restore_collection: build not L0 dir groups: %w", err)
	}
	notL0Tasks := planner.planTasks(part.GetPartitionName(), notL0Groups)
	if err := tasklet.RunConcurrent(ctx, sem, notL0Tasks); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 segments: %w", err)
	}

	logger.Info("start restore L0 segment", zap.String("partition_name", part.GetPartitionName()))
	l0Segs := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
		return seg.IsL0
	})
	l0Groups, err := source.l0DirGroups(l0Segs)
	if err != nil {
		return fmt.Errorf("restore_collection: build L0 dir groups: %w", err)
	}
	l0Tasks := planner.planTasks(part.GetPartitionName(), l0Groups)
	if err := tasklet.RunConcurrent(ctx, sem, l0Tasks); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segments: %w", err)
	}

	return nil
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

		// The export job reported the bundle's size, index files included, so it
		// is what the restore job's progress is a fraction of.
		tasks = append(tasks, newCollPlanTask(t.args.Store, t.args.TaskID, tgt.target, tgt.collBackup.GetSize(), st.Execute))
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
