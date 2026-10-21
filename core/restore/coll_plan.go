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

// selectPlan composes the two option axes into the plan that runs: the
// backup's format narrows which strategies exist, and the strategy pin — or
// auto, resolved against the target's capabilities — picks among what remains.
// A pin outside the format's set is a loud error, not a silent fallback.
//
// The chosen plan comes back fully built: everything a restore will run is
// enumerated from the backup meta at selection time, so the work is known —
// and reportable, and resumable — before the first task executes. Building
// reads the backup bucket but never touches the target.
func (t *Task) selectPlan(ctx context.Context, targets []collTarget) (tasklet.Tasklet, error) {
	// A secondary commit rides the restful import api: the v1 grpc import has
	// no two-phase commit at all. Auto resolves to the restful plan directly —
	// the preflight has already established the target is new enough.
	hasSecondaries := len(t.args.Option.SecondaryEndpoints) != 0

	switch strategy := t.args.Option.Strategy; strategy {
	case StrategyAuto:
		if t.format == meta.FormatSnapshot {
			// The restore api is the fast path — a physical copy, indexes
			// carried over — so auto always takes it. The import path is
			// opt-in only, pinned with --strategy=import_snap.
			t.logger.Info("use restore snap plan")
			return newRestoreSnapPlan(t, targets), nil
		}
		if hasSecondaries {
			t.logger.Info("use binlog restful plan with two-phase commit")
			return t.newBinlogDataPlan(ctx, targets, t.restfulPlannerFactory())
		}
		if t.grpc.HasFeature(milvus.ImportV2) {
			t.logger.Info("use binlog restful plan")
			return t.newBinlogDataPlan(ctx, targets, t.restfulPlannerFactory())
		}
		t.logger.Info("use binlog grpc plan")
		return t.newBinlogDataPlan(ctx, targets, grpcPlannerFactory)
	case StrategyPerSeg:
		if t.format != meta.FormatBinlog {
			return nil, wrongFormat("per_seg", meta.FormatBinlog, t.format)
		}
		if hasSecondaries {
			return nil, errors.New("restore: --secondaries needs the restful import api for its two-phase " +
				"commit, but the per_seg strategy imports through the v1 grpc api; use merged_seg or auto")
		}
		t.logger.Info("use binlog grpc plan")
		return t.newBinlogDataPlan(ctx, targets, grpcPlannerFactory)
	case StrategyMergedSeg:
		if t.format != meta.FormatBinlog {
			return nil, wrongFormat("merged_seg", meta.FormatBinlog, t.format)
		}
		t.logger.Info("use binlog restful plan")
		return t.newBinlogDataPlan(ctx, targets, t.restfulPlannerFactory())
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
		t.logger.Info("use import snap plan")
		return t.newImportSnapPlanChecked(targets)
	default:
		return nil, fmt.Errorf("restore: unsupported strategy: %s", strategy)
	}
}

// newBinlogDataPlan picks the binlog-side plan: meta_only restores schemas
// only, so its plan is the binlog plan's DDL half and the import protocol
// never matters.
func (t *Task) newBinlogDataPlan(ctx context.Context, targets []collTarget, newPlanner importPlannerFactory) (tasklet.Tasklet, error) {
	if t.args.Option.MetaOnly {
		t.logger.Info("use meta only plan")
		return newMetaOnlyPlan(t, targets), nil
	}
	return newBinlogPlan(ctx, t, targets, newPlanner)
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

// newImportTaskArgs assembles the import wiring of one target collection; the
// same wiring serves both binlog plans. A local target resolves its storage
// directory two ways: the path milvus-backup reaches it at (rootPath) and the
// path the Milvus process itself resolves (localPath, falling back to
// rootPath). Whether the restore stages data is a job-level invariant — both
// storages and the streaming flag are fixed for the whole job — so the
// decision is made here, once per collection, and the import tasks only read
// it.
func (t *Task) newImportTaskArgs(tgt collTarget, jobs *jobstate.RestoreCollTracker) importTaskArgs {
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

		jobs:   jobs,
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
		return newRestfulImportPlanner(args, multiL0InOneJob, t.secondaries)
	}
}

// runCollTasks runs the per-collection nodes of a plan concurrently, capped
// by the collection concurrency.
func runCollTasks(ctx context.Context, logger *zap.Logger, limit int, colls []tasklet.Tasklet) error {
	logger.Info("start restore collection")

	if err := tasklet.RunConcurrentLimit(ctx, limit, colls); err != nil {
		return fmt.Errorf("restore: restore collection %w", err)
	}

	logger.Info("finish restore all collections")
	return nil
}

// metaOnlyPlan restores only the collections' schemas: the data half is never
// built, so the plan is the DDL tasks alone.
type metaOnlyPlan struct {
	colls []*collDDLTask
	limit int

	logger *zap.Logger
}

func newMetaOnlyPlan(t *Task, targets []collTarget) *metaOnlyPlan {
	colls := make([]*collDDLTask, 0, len(targets))
	for _, tgt := range targets {
		ddl, _ := t.newCollSchemaTask(tgt)
		colls = append(colls, ddl)
	}

	return &metaOnlyPlan{
		colls: colls,
		limit: t.args.Params.Restore.Concurrency.Collections.Val,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}
}

func (p *metaOnlyPlan) Execute(ctx context.Context) error {
	tasks := make([]tasklet.Tasklet, 0, len(p.colls))
	for _, coll := range p.colls {
		tasks = append(tasks, coll)
	}

	return runCollTasks(ctx, p.logger, p.limit, tasks)
}

// binlogPlan restores every collection from a binlog backup through an import
// api: each collection's node runs its DDL, then imports its binlogs, with the
// importPlannerFactory having chosen the protocol at build time. The two
// binlog data paths differ only in the import tasks the factory minted.
type binlogPlan struct {
	colls []*collBinlogTask
	limit int

	logger *zap.Logger
}

// newBinlogPlan expands every target into its collection node: the DDL and
// every import task of its data, all built now.
func newBinlogPlan(ctx context.Context, t *Task, targets []collTarget, newPlanner importPlannerFactory) (*binlogPlan, error) {
	colls := make([]*collBinlogTask, 0, len(targets))
	for _, tgt := range targets {
		coll, err := t.newCollBinlogTask(ctx, tgt, newPlanner)
		if err != nil {
			return nil, err
		}
		colls = append(colls, coll)
	}

	return &binlogPlan{
		colls: colls,
		limit: t.args.Params.Restore.Concurrency.Collections.Val,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}, nil
}

func (p *binlogPlan) Execute(ctx context.Context) error {
	tasks := make([]tasklet.Tasklet, 0, len(p.colls))
	for _, coll := range p.colls {
		tasks = append(tasks, coll)
	}

	return runCollTasks(ctx, p.logger, p.limit, tasks)
}

// collBinlogTask restores one collection from a binlog backup: its DDL, then
// every partition's binlogs, then the collection-global L0 segments. The whole
// shape is built before anything runs; Execute only walks it. The task holds
// no state handle of its own: the DDL task settles the schema half and each
// import task settles its job half, and the collection's state is derived from
// those halves.
type collBinlogTask struct {
	ddl tasklet.Tasklet

	parts    []partBinlogTasks
	globalL0 []tasklet.Tasklet

	sem tasklet.Semaphore

	logger *zap.Logger
}

// partBinlogTasks is one partition's import tasks, ordered: the leveled
// segments land before the L0 segments that supersede them.
type partBinlogTasks struct {
	leveled []tasklet.Tasklet
	l0      []tasklet.Tasklet
}

// newCollSchemaTask registers the collection's node — its declared size is the
// sum of its partitions', an export-free binlog backup having no bundle size —
// and builds its schema task: the shared head of the binlog and meta only
// plans.
func (t *Task) newCollSchemaTask(tgt collTarget) (*collDDLTask, *jobstate.RestoreCollTracker) {
	size := lo.SumBy(tgt.collBackup.GetPartitionBackups(), func(part *backuppb.PartitionBackupInfo) int64 {
		return part.GetSize()
	})
	collTracker := t.tracker.NewCollTracker(tgt.target, size)

	ddl := newCollDDLTask(t.args.TaskID, t.args.Option, tgt.collOverride, tgt.collBackup, tgt.target, t.grpc, collTracker.DDL())

	return ddl, collTracker
}

// newCollBinlogTask builds the whole node: the schema half plus every import
// task of the collection's data, enumerated from the backup meta now.
func (t *Task) newCollBinlogTask(ctx context.Context, tgt collTarget, newPlanner importPlannerFactory) (*collBinlogTask, error) {
	ddl, collTracker := t.newCollSchemaTask(tgt)

	src := collref.New(tgt.collBackup.GetDbName(), tgt.collBackup.GetCollectionName())
	logger := log.With(
		zap.String("restore_task_id", t.args.TaskID),
		zap.String("backup_coll", src.String()),
		zap.String("target_coll", tgt.target.String()))

	source := newBinlogSource(tgt.collBackup, t.args.Option, t.args.BackupDir, t.args.BackupStorage, logger)
	planner := newPlanner(t.newImportTaskArgs(tgt, collTracker))

	task := &collBinlogTask{
		ddl:    ddl,
		sem:    t.bulkInsertSem,
		logger: logger,
	}

	task.parts = make([]partBinlogTasks, 0, len(tgt.collBackup.GetPartitionBackups()))
	for _, part := range tgt.collBackup.GetPartitionBackups() {
		notL0Groups, err := source.notL0DirGroups(ctx, part)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: build not L0 dir groups: %w", err)
		}
		l0Segs := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
			return seg.IsL0
		})
		l0Groups, err := source.l0DirGroups(l0Segs)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: build L0 dir groups: %w", err)
		}

		task.parts = append(task.parts, partBinlogTasks{
			leveled: planner.planTasks(part.GetPartitionName(), notL0Groups),
			l0:      planner.planTasks(part.GetPartitionName(), l0Groups),
		})
	}

	globalL0Groups, err := source.l0DirGroups(tgt.collBackup.GetL0Segments())
	if err != nil {
		return nil, fmt.Errorf("restore_collection: build global L0 dir groups: %w", err)
	}
	task.globalL0 = planner.planTasks("", globalL0Groups)

	task.logger.Info("collection restore plan built",
		zap.Int("partitions", len(task.parts)),
		zap.Int("global_l0_tasks", len(task.globalL0)))

	return task, nil
}

func (t *collBinlogTask) Execute(ctx context.Context) error {
	if err := t.execute(ctx); err != nil {
		t.logger.Error("restore collection failed", zap.Error(err))
		return err
	}

	t.logger.Info("restore collection success")
	return nil
}

func (t *collBinlogTask) execute(ctx context.Context) error {
	if err := t.ddl.Execute(ctx); err != nil {
		return fmt.Errorf("restore_collection: restore collection ddl: %w", err)
	}

	// L0 data must land after the leveled data it supersedes, so the ordering
	// is structure rather than scheduling: partitions race one another, each
	// running its leveled tasks before its L0 tasks, and the collection-global
	// L0 segments wait for every partition.
	g, subCtx := errgroup.WithContext(ctx)
	for _, part := range t.parts {
		g.Go(func() error {
			return part.execute(subCtx, t.sem)
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait for partition restore: %w", err)
	}

	if err := tasklet.RunConcurrent(ctx, t.sem, t.globalL0); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

func (p *partBinlogTasks) execute(ctx context.Context, sem tasklet.Semaphore) error {
	if err := tasklet.RunConcurrent(ctx, sem, p.leveled); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 segments: %w", err)
	}
	if err := tasklet.RunConcurrent(ctx, sem, p.l0); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segments: %w", err)
	}

	return nil
}

// restoreSnapPlan restores every collection from a snapshot backup by handing
// it to Milvus's restore api: Milvus creates the collection from the schema in
// the bundle, restores its indexes and partitions, and copies the data in. No
// DDL runs on our side, and no bytes move through this process.
type restoreSnapPlan struct {
	colls []*collSnapshotTask
	limit int

	logger *zap.Logger
}

func newRestoreSnapPlan(t *Task, targets []collTarget) *restoreSnapPlan {
	colls := make([]*collSnapshotTask, 0, len(targets))
	for _, tgt := range targets {
		colls = append(colls, t.newRestoreSnapCollTask(tgt))
	}

	return &restoreSnapPlan{
		colls: colls,
		limit: t.args.Params.Restore.Concurrency.Collections.Val,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}
}

func (p *restoreSnapPlan) Execute(ctx context.Context) error {
	tasks := make([]tasklet.Tasklet, 0, len(p.colls))
	for _, coll := range p.colls {
		tasks = append(tasks, coll)
	}

	return runCollTasks(ctx, p.logger, p.limit, tasks)
}

// newRestoreSnapCollTask expands one target into its restore job on Milvus's
// restore api. Building it registers the collection; the task settles the
// collection's single job half itself.
func (t *Task) newRestoreSnapCollTask(tgt collTarget) *collSnapshotTask {
	// The export job reported the bundle's size, index files included, so it
	// is what the restore job's progress is a fraction of.
	collTracker := t.tracker.NewCollTracker(tgt.target, tgt.collBackup.GetSize())

	// A backup on another storage service than the target's is staged into the
	// milvus storage first: the restore api's server-side copy cannot cross
	// services, so milvus-backup streams the bundle over itself.
	var staging *snapshotStaging
	if t.snapshotSource.staging {
		milvusRootPath := t.args.Params.Milvus.Storage.RootPath.Val
		staging = &snapshotStaging{
			backupDir:      t.args.BackupDir,
			backupStorage:  t.args.BackupStorage,
			milvusStorage:  t.args.MilvusStorage,
			milvusRootPath: milvusRootPath,
			copySem:        t.copySem,
			keepTempFiles:  t.args.Params.Restore.KeepTempFiles.Val,
		}
	}

	return newCollSnapshotTask(collSnapshotTaskArgs{
		taskID:           t.args.TaskID,
		collBackup:       tgt.collBackup,
		target:           tgt.target,
		source:           t.snapshotSource,
		dropExist:        t.args.Option.DropExistCollection,
		maxShardNum:      t.args.Option.MaxShardNum,
		shardNumOverride: tgt.collOverride.ShardNum,
		descOverride:     tgt.collOverride.Description,
		skipParams:       t.args.Option.SkipParams,
		staging:          staging,
		grpcCli:          t.grpc,
		job:              collTracker.NewJob(tgt.collBackup.GetSize()),
	})
}

// newImportSnapPlanChecked gates the import snap plan on the target's support:
// snapshot-sourced imports landed in Milvus 3.0.4, and an older server would
// read the request as an ordinary path import and fail on the metadata URI.
func (t *Task) newImportSnapPlanChecked(targets []collTarget) (tasklet.Tasklet, error) {
	if !t.grpc.HasFeature(milvus.ImportSnapshot) {
		return nil, errors.New("restore: the import_snap strategy restores through the target's import api, " +
			"which needs a Milvus 3.0.4 or newer server")
	}

	if t.args.Option.MetaOnly {
		t.logger.Info("use meta only plan")
		return newMetaOnlyPlan(t, targets), nil
	}

	return newImportSnapPlan(t, targets)
}

// importSnapPlan restores every collection of a snapshot backup through the
// target's import api: each collection's node creates the schema, partitions and
// indexes through the ordinary DDL task, then submits one snapshot import job
// covering the whole bundle. Where the restore api hands the whole restore to
// Milvus, this path keeps the DDL on our side — shard overrides, skip_params and
// the index options all work — and trades the server-side copy for a row rewrite
// that folds the snapshot's L0 deletes and reads a source on another storage
// service through its external spec.
type importSnapPlan struct {
	colls []*collImportSnapTask
	limit int

	logger *zap.Logger
}

func newImportSnapPlan(t *Task, targets []collTarget) (*importSnapPlan, error) {
	colls := make([]*collImportSnapTask, 0, len(targets))
	for _, tgt := range targets {
		coll, err := t.newImportSnapCollTask(tgt)
		if err != nil {
			return nil, err
		}
		colls = append(colls, coll)
	}

	return &importSnapPlan{
		colls: colls,
		limit: t.args.Params.Restore.Concurrency.Collections.Val,

		logger: log.With(zap.String("task_id", t.args.TaskID)),
	}, nil
}

func (p *importSnapPlan) Execute(ctx context.Context) error {
	tasks := make([]tasklet.Tasklet, 0, len(p.colls))
	for _, coll := range p.colls {
		tasks = append(tasks, coll)
	}

	return runCollTasks(ctx, p.logger, p.limit, tasks)
}

// newImportSnapCollTask expands one target into its node: the DDL half, built
// like every other plan's, and the single snapshot import job behind it. The
// bundle's metadata path resolves against where the backup is read from, so a
// relocated backup imports from wherever it sits now.
func (t *Task) newImportSnapCollTask(tgt collTarget) (*collImportSnapTask, error) {
	collTracker := t.tracker.NewCollTracker(tgt.target, tgt.collBackup.GetSize())
	ddl := newCollDDLTask(t.args.TaskID, t.args.Option, tgt.collOverride, tgt.collBackup, tgt.target, t.grpc, collTracker.DDL())

	src := collref.New(tgt.collBackup.GetDbName(), tgt.collBackup.GetCollectionName())
	logger := log.With(
		zap.String("restore_task_id", t.args.TaskID),
		zap.String("backup_coll", src.String()),
		zap.String("target_coll", tgt.target.String()))

	metadataURI, err := t.snapshotSource.metadataURI(tgt.collBackup.GetSnapshotBackup().GetMetadataPath())
	if err != nil {
		return nil, err
	}
	partMapping, err := snapshotPartitionMapping(tgt.collBackup)
	if err != nil {
		return nil, err
	}

	imp := &snapshotImportTask{
		taskID:           t.args.TaskID,
		target:           tgt.target,
		metadataURI:      metadataURI,
		externalSpec:     t.snapshotSource.externalSpec,
		partitionMapping: partMapping,
		ezk:              t.mappedEZK(tgt.dbBackup),

		restfulCli: t.restful,
		job:        collTracker.NewJob(tgt.collBackup.GetSize()),

		logger: logger,
	}

	return &collImportSnapTask{ddl: ddl, imp: imp, logger: logger}, nil
}
