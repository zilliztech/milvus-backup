package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

type DBMapping struct {
	Target   string
	WithProp bool
}

type SkipParams struct {
	CollectionProperties []string

	FieldIndexParams []string
	FieldTypeParams  []string

	IndexParams []string
}

// CollOverride contains per-collection overrides for restore.
type CollOverride struct {
	ShardNum    int32
	Description string
}

type Plan struct {
	// BackupFilter filters databases and collections from the backup.
	// It is mainly for backward compatibility and can be
	// removed after the dbCollection parameter is completely deprecated.
	BackupFilter filter.Filter

	// mapping
	DBMapper   map[string][]DBMapping
	CollMapper CollMapper

	// CollOverrides contains per-collection overrides keyed by target "db.coll" string.
	CollOverrides map[string]CollOverride

	// TaskFilter filters databases and collections after mapping.
	TaskFilter filter.Filter
}

// CollMapper is the interface for renaming collection.
type CollMapper interface {
	// TargetNames renames the given collection name (database and collection) according to the renaming rules.
	TargetNames(collRef collref.Name) []collref.Name
}

var _ CollMapper = (*DefaultCollMapper)(nil)

// DefaultCollMapper is the default collMapper that returns the original name.
type DefaultCollMapper struct{}

func NewDefaultCollMapper() *DefaultCollMapper { return &DefaultCollMapper{} }
func (r *DefaultCollMapper) TargetNames(collRef collref.Name) []collref.Name {
	return []collref.Name{collRef}
}

var _ CollMapper = (*SuffixMapper)(nil)

// SuffixMapper mapping the collection by adding a suffix.
type SuffixMapper struct {
	suffix string
}

func NewSuffixMapper(suffix string) *SuffixMapper { return &SuffixMapper{suffix: suffix} }

func (s *SuffixMapper) TargetNames(collRef collref.Name) []collref.Name {
	return []collref.Name{collref.New(collRef.DBName(), collRef.CollName()+s.suffix)}
}

var _ CollMapper = (*TableMapper)(nil)

// TableMapper generates target collection names from source collection names by looking up a mapping table.
type TableMapper struct {
	DBWildcard  map[string]string         // dbName -> newDbName, from db1.*:db2.*
	NameMapping map[string][]collref.Name // dbName.collName -> newCollName, from db1.coll1:db2.coll2 and coll1:coll2
}

func (r *TableMapper) TargetNames(collRef collref.Name) []collref.Name {
	if newNames, ok := r.NameMapping[collRef.String()]; ok {
		return newNames
	}

	if newDBName, ok := r.DBWildcard[collRef.DBName()]; ok {
		return []collref.Name{collref.New(newDBName, collRef.CollName())}
	}

	return []collref.Name{collRef}
}

type Option struct {
	// Index related options
	DropExistIndex bool
	RebuildIndex   bool
	UseAutoIndex   bool

	// Collection schema related options
	DropExistCollection  bool
	SkipCreateCollection bool
	MaxShardNum          int32
	SkipParams           SkipParams

	// data related options
	MetaOnly bool
	// Strategy pins the data path the restore takes. The zero value auto
	// resolves it against the backup's format and the target's capabilities.
	Strategy           Strategy
	TruncateBinlogByTs bool

	// SecondaryEndpoints are the REST endpoints of the secondary clusters that
	// mirror the target's import jobs over the replication stream. When set,
	// every import job is committed two-phase: held at Uncommitted until every
	// secondary's mirror is there too, then committed on the target and all
	// secondaries. Empty for a plain restore.
	SecondaryEndpoints []string

	RestoreRBAC bool

	// EZKMapping maps old encryption keys to new ones.
	// During restore, the old EZK from backup is looked up in this map
	// and replaced with the new EZK if a match is found.
	EZKMapping map[string]string
}

type TaskArgs struct {
	TaskID string

	Backup *backuppb.BackupInfo

	Plan   *Plan
	Option *Option

	Params *cfg.Config

	BackupDir     string
	BackupStorage storage.Client
	MilvusStorage storage.Client

	Store *jobstate.Store
}

type Task struct {
	args TaskArgs

	// format is the backup's, which decides how every collection in it is restored.
	format string
	// snapshotSource is where the bundles are read from, and is only set for the
	// snapshot format.
	snapshotSource snapshotSource

	// streaming is decided once: it depends on the transfer policy and the two
	// backends, none of which vary per collection.
	streaming bool

	grpc    milvus.Grpc
	restful milvus.Restful
	// secondaries talk to the secondary clusters given with the secondaries
	// option; empty when the restore commits single-phase.
	secondaries []importSecondary

	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	// tracker is the job's write handle on the job state store, minted at
	// construction; the plans mint their per-collection handles from it.
	tracker *jobstate.RestoreTracker

	logger *zap.Logger
}

func NewTask(ctx context.Context, args TaskArgs) (*Task, error) {
	// Without this an unknown format would go down the binlog path, find no segments, and
	// report a successful restore of an empty collection.
	format := args.Backup.GetFormat()
	if format != meta.FormatBinlog && format != meta.FormatSnapshot {
		return nil, fmt.Errorf("restore: backup %s was taken in the %q format, which this version of milvus-backup cannot restore",
			args.Backup.GetName(), format)
	}

	logger := log.With(zap.String("backup_name", args.Backup.GetName()), zap.String("task_id", args.TaskID))

	task := &Task{
		args: args,

		format: format,

		copySem:       semaphore.NewWeighted(int64(args.Params.Transfer.Concurrency.Val)),
		bulkInsertSem: semaphore.NewWeighted(int64(args.Params.Restore.Concurrency.ImportJobs.Val)),

		logger: logger,
	}

	if format == meta.FormatSnapshot {
		// The import path is opt-in only, so the option checks key on the pin
		// itself; the target's capabilities are checked at plan time, once the
		// client is up.
		strategy := args.Option.Strategy
		if err := checkSnapshotSupport(args.Option, strategy); err != nil {
			return nil, err
		}
		if ignored := snapshotIgnoredOptions(args.Option, strategy); len(ignored) != 0 {
			logger.Warn("options do not apply to a snapshot format backup restored with the restore api and are ignored",
				zap.Strings("options", ignored))
		}

		source, err := newSnapshotSource(ctx, args.MilvusStorage.Config(), args.BackupStorage.Config(), args.BackupDir,
			strategy == StrategyImportSnap)
		if err != nil {
			return nil, err
		}
		task.snapshotSource = source
		logger.Info("restore from snapshot bundles",
			zap.String("source_uri", source.dirURI),
			zap.Bool("external_spec_set", source.externalSpec != ""),
			zap.Bool("source_sas_set", source.sourceSASSet),
			zap.Bool("staging", source.staging))
	} else {
		task.streaming = storage.UseStreaming(args.Params.Transfer.Mode.Val,
			args.BackupStorage.Config(), args.MilvusStorage.Config())
		logger.Info("resolved transfer mode",
			zap.String("transfer_mode", args.Params.Transfer.Mode.Val),
			zap.Bool("streaming", task.streaming))
	}

	task.tracker = args.Store.AddRestoreTask(args.TaskID)

	return task, nil
}

// checkSnapshotSupport refuses the options no snapshot data path can honor, and —
// keyed on the resolved strategy — the ones only the import path honors. The restore
// api path hands the whole restore to Milvus: it creates the collection from the
// schema in the bundle, restores its indexes and partitions, and copies the data in, so
// options that ask for something else would silently hand back a collection nobody
// asked for. The import path creates the collection through the ordinary DDL task and
// honors them. skip_params needs neither list: on the restore path the skipped keys
// are dropped with delete_keys once the job completes, on the import path they are
// simply not written, both matching what the binlog path achieves at creation time.
func checkSnapshotSupport(opt *Option, strategy Strategy) error {
	var unsupported []string
	if opt.TruncateBinlogByTs {
		unsupported = append(unsupported, "truncate_binlog_by_ts")
	}
	if len(opt.SecondaryEndpoints) != 0 {
		unsupported = append(unsupported, "secondaries")
	}
	// A description override stays available on both paths: the restore api applies it
	// with an AlterCollection once the job completes, the import path sets it at
	// creation time. A shard_num override is checked per collection, where the bundle's
	// shard count is known.
	if len(unsupported) != 0 {
		return fmt.Errorf("restore: %s cannot be used with a snapshot format backup",
			strings.Join(unsupported, ", "))
	}

	if strategy == StrategyImportSnap {
		return nil
	}
	var restoreOnly []string
	if opt.SkipCreateCollection {
		restoreOnly = append(restoreOnly, "skip_create_collection")
	}
	if opt.MetaOnly {
		restoreOnly = append(restoreOnly, "meta_only")
	}
	if len(opt.EZKMapping) != 0 {
		restoreOnly = append(restoreOnly, "ezk_mapping")
	}
	if len(restoreOnly) != 0 {
		return fmt.Errorf("restore: %s cannot be used with the restore api strategy; the import_snap strategy supports them, on a Milvus 3.0.4 or newer target",
			strings.Join(restoreOnly, ", "))
	}

	return nil
}

// snapshotIgnoredOptions names the options the restore api path does not read: Milvus
// restores the bundle's own indexes either way. The import path creates indexes through
// the ordinary DDL task, so the index options are honored there. A strategy pin that
// mismatches the snapshot format is not ignored but refused, at plan selection.
func snapshotIgnoredOptions(opt *Option, strategy Strategy) []string {
	if strategy == StrategyImportSnap {
		return nil
	}

	var ignored []string
	if opt.DropExistIndex {
		ignored = append(ignored, "drop_exist_index")
	}
	if opt.RebuildIndex {
		ignored = append(ignored, "restore_index")
	}
	if opt.UseAutoIndex {
		ignored = append(ignored, "use_auto_index")
	}

	return ignored
}

func (t *Task) newDBAndCollTasks(backup *backuppb.BackupInfo) ([]*databaseTask, []collTarget) {
	dbNames := lo.Map(backup.GetDatabaseBackups(), func(db *backuppb.DatabaseBackupInfo, _ int) string { return db.GetDbName() })
	t.logger.Info("databases in backup", zap.Strings("db_names", dbNames))
	collNames := lo.Map(backup.GetCollectionBackups(), func(coll *backuppb.CollectionBackupInfo, _ int) string {
		return collref.New(coll.GetDbName(), coll.GetCollectionName()).String()
	})
	t.logger.Info("collections in backup", zap.Strings("coll_names", collNames))

	// filter backup
	dbBackups := t.filterDBBackup(backup.GetDatabaseBackups())
	collBackups := t.filterCollBackup(backup.GetCollectionBackups())
	dbNames = lo.Map(dbBackups, func(db *backuppb.DatabaseBackupInfo, _ int) string { return db.GetDbName() })
	t.logger.Info("databases backup after filtering", zap.Strings("db_names", dbNames))
	collNames = lo.Map(collBackups, func(coll *backuppb.CollectionBackupInfo, _ int) string {
		return collref.New(coll.GetDbName(), coll.GetCollectionName()).String()
	})
	t.logger.Info("collections backup after filtering", zap.Strings("coll_names", collNames))

	// generate restore tasks
	dbTasks := t.newDBTasks(dbBackups)
	collTargets := t.newCollTargets(dbBackups, collBackups)
	dbNames = lo.Map(dbTasks, func(db *databaseTask, _ int) string { return db.targetName })
	t.logger.Info("databases task after mapping", zap.Strings("db_names", dbNames))
	collNames = lo.Map(collTargets, func(tgt collTarget, _ int) string { return tgt.target.String() })
	t.logger.Info("collections task after mapping", zap.Strings("colls", collNames))

	// filter task
	dbTasks = t.filterDBTask(dbTasks)
	collTargets = t.filterCollTask(collTargets)
	dbNames = lo.Map(dbTasks, func(db *databaseTask, _ int) string { return db.targetName })
	t.logger.Info("databases task after filtering", zap.Strings("db_names", dbNames))
	collNames = lo.Map(collTargets, func(tgt collTarget, _ int) string { return tgt.target.String() })
	t.logger.Info("collections task after filtering", zap.Strings("colls", collNames))

	return dbTasks, collTargets
}

func (t *Task) filterDBBackup(dbBackups []*backuppb.DatabaseBackupInfo) []*backuppb.DatabaseBackupInfo {
	return lo.Filter(dbBackups, func(dbBackup *backuppb.DatabaseBackupInfo, _ int) bool {
		return t.args.Plan.BackupFilter.AllowDB(dbBackup.GetDbName())
	})
}

func (t *Task) filterCollBackup(collBackups []*backuppb.CollectionBackupInfo) []*backuppb.CollectionBackupInfo {
	return lo.Filter(collBackups, func(collBackup *backuppb.CollectionBackupInfo, _ int) bool {
		collRef := collref.New(collBackup.GetDbName(), collBackup.GetCollectionName())
		return t.args.Plan.BackupFilter.AllowName(collRef)
	})
}

func (t *Task) newDBTask(dbBak *backuppb.DatabaseBackupInfo) []*databaseTask {
	mappings, ok := t.args.Plan.DBMapper[dbBak.GetDbName()]
	if !ok {
		t.logger.Debug("no mapping for database, restore directly", zap.String("db_name", dbBak.GetDbName()))
		task := newDatabaseTask(t.grpc, dbBak, dbBak.GetDbName())
		return []*databaseTask{task}
	}

	tasks := make([]*databaseTask, 0, len(mappings))
	for _, mapping := range mappings {
		t.logger.Debug("generate restore database task", zap.String("source", dbBak.GetDbName()), zap.String("target", mapping.Target))
		task := newDatabaseTask(t.grpc, dbBak, mapping.Target)
		tasks = append(tasks, task)
	}

	return tasks
}

func (t *Task) newDBTasks(dbBackups []*backuppb.DatabaseBackupInfo) []*databaseTask {
	dbTasks := make([]*databaseTask, 0, len(dbBackups))
	for _, dbBackup := range dbBackups {
		tasks := t.newDBTask(dbBackup)
		dbTasks = append(dbTasks, tasks...)
	}

	return dbTasks
}

func (t *Task) newCollTarget(dbBackup *backuppb.DatabaseBackupInfo, collBackup *backuppb.CollectionBackupInfo) []collTarget {
	source := collref.New(collBackup.GetDbName(), collBackup.GetCollectionName())
	targets := t.args.Plan.CollMapper.TargetNames(source)

	collTargets := make([]collTarget, 0, len(targets))
	for _, target := range targets {
		t.logger.Debug("generate restore collection task", zap.String("source", source.String()), zap.String("target", target.String()))

		collTargets = append(collTargets, collTarget{
			dbBackup:     dbBackup,
			collBackup:   collBackup,
			target:       target,
			collOverride: t.args.Plan.CollOverrides[target.String()],
		})
	}

	return collTargets
}

func (t *Task) newCollTargets(dbBackups []*backuppb.DatabaseBackupInfo, collBackups []*backuppb.CollectionBackupInfo) []collTarget {
	nameDBBackup := lo.SliceToMap(dbBackups, func(dbBackup *backuppb.DatabaseBackupInfo) (string, *backuppb.DatabaseBackupInfo) {
		return dbBackup.GetDbName(), dbBackup
	})

	collTargets := make([]collTarget, 0, len(collBackups))
	for _, collBackup := range collBackups {
		dbBackup := nameDBBackup[collBackup.GetDbName()]
		collTargets = append(collTargets, t.newCollTarget(dbBackup, collBackup)...)
	}

	return collTargets
}

func (t *Task) filterDBTask(dbTask []*databaseTask) []*databaseTask {
	return lo.Filter(dbTask, func(task *databaseTask, _ int) bool {
		return t.args.Plan.TaskFilter.AllowDB(task.targetName)
	})
}

func (t *Task) filterCollTask(collTargets []collTarget) []collTarget {
	return lo.Filter(collTargets, func(target collTarget, _ int) bool {
		return t.args.Plan.TaskFilter.AllowName(target.target)
	})
}

// checkCollsExist check if the collection exist in target milvus, if collection exist, return error.
func (t *Task) checkCollsExist(ctx context.Context, collTargets []collTarget) error {
	for _, tgt := range collTargets {
		if err := t.checkCollExist(ctx, tgt.target); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) checkCollExist(ctx context.Context, target collref.Name) error {
	has, err := t.grpc.HasCollection(ctx, target.DBName(), target.CollName())
	if err != nil {
		return fmt.Errorf("restore: check collection %w", err)
	}

	if t.args.Option.SkipCreateCollection && t.args.Option.DropExistCollection {
		return fmt.Errorf("restore: skip create and drop exist collection can not be true at the same time collection %s", target.String())
	}

	// collection not exist and not create collection
	if !has && t.args.Option.SkipCreateCollection {
		return fmt.Errorf("restore: collection not exist, database %s collection %s", target.DBName(), target.CollName())
	}

	// collection existed and not drop collection
	if has && !t.args.Option.SkipCreateCollection && !t.args.Option.DropExistCollection {
		return fmt.Errorf("restore: collection already exist, database %s collection %s", target.DBName(), target.CollName())
	}

	return nil
}

func (t *Task) initClients() error {
	grpcCli, err := milvus.NewGrpc(&t.args.Params.Milvus)
	if err != nil {
		return fmt.Errorf("restore: create grpc client: %w", err)
	}
	t.grpc = grpcCli

	restfulCli, err := milvus.NewRestful(&t.args.Params.Milvus)
	if err != nil {
		return fmt.Errorf("restore: create restful client: %w", err)
	}
	t.restful = restfulCli

	for _, endpoint := range t.args.Option.SecondaryEndpoints {
		// A secondary shares the target's credentials — a replication pair
		// practically always does — and differs only in the REST endpoint.
		secondaryCfg := t.args.Params.Milvus
		secondaryCfg.Rest.Endpoint.Val = endpoint
		secondaryCli, err := milvus.NewRestful(&secondaryCfg)
		if err != nil {
			return fmt.Errorf("restore: create secondary restful client for %s: %w", endpoint, err)
		}
		t.secondaries = append(t.secondaries, importSecondary{addr: endpoint, cli: secondaryCli})
	}

	return nil
}

func (t *Task) closeClients() {
	if t.grpc != nil {
		if err := t.grpc.Close(); err != nil {
			t.logger.Warn("close grpc client", zap.Error(err))
		}
	}
}

var _ tasklet.Tasklet = (*Task)(nil)

// Tracker hands the job the write handle the task registered at construction.
// The task settles it before Execute returns; the job that owns the run reads
// its terminal state through it.
func (t *Task) Tracker() *jobstate.RestoreTracker { return t.tracker }

func (t *Task) Execute(ctx context.Context) error {
	defer t.closeClients()
	if err := t.initClients(); err != nil {
		// Client construction fails before the job has a state to run in, but
		// the job still has to settle: a caller waiting on the store must not
		// be left watching a task that is never going to move again.
		t.tracker.SetFail(err)
		return err
	}

	if err := t.privateExecute(ctx); err != nil {
		t.logger.Error("restore task failed", zap.Error(err))
		t.tracker.SetFail(err)
		return fmt.Errorf("restore: execute %w", err)
	}

	t.logger.Info("restore task finished")
	t.tracker.SetSuccess()
	return nil
}

func (t *Task) privateExecute(ctx context.Context) error {
	t.tracker.SetExecuting()

	// The server is only known once the client is connected, so this cannot be part of
	// NewTask. Checking it once here beats one identical failure per collection.
	if t.format == meta.FormatSnapshot && !t.grpc.HasFeature(milvus.Snapshot) {
		return fmt.Errorf("restore: a snapshot format backup needs a milvus 3.0 or newer server")
	}

	// Binlog restores import through jobs, which is where the two-phase commit
	// contract lives; the snapshot path runs no import job at all.
	if t.format == meta.FormatBinlog {
		if err := t.checkImport2PCSetup(ctx); err != nil {
			return err
		}
	}

	dbTasks, collTargets := t.newDBAndCollTasks(t.args.Backup)

	// Selecting the plan before anything touches the target keeps a strategy
	// pin that mismatches the backup a pure validation failure, with no RBAC
	// or database side effects behind it. Building the plan reads the backup
	// bucket — the whole work tree is enumerated here — but writes nothing.
	plan, err := t.selectPlan(ctx, collTargets)
	if err != nil {
		return fmt.Errorf("restore: select plan: %w", err)
	}

	if err := t.restoreRBAC(ctx); err != nil {
		return fmt.Errorf("restore: restore rbac %w", err)
	}

	if err := t.runDBTasks(ctx, dbTasks); err != nil {
		return fmt.Errorf("restore: run database task %w", err)
	}

	if err := t.prepareDB(ctx, collTargets); err != nil {
		return fmt.Errorf("restore: prepare database %w", err)
	}

	if err := t.checkCollsExist(ctx, collTargets); err != nil {
		return fmt.Errorf("restore: check collection exist %w", err)
	}

	if err := plan.Execute(ctx); err != nil {
		return fmt.Errorf("restore: run collection plan %w", err)
	}

	return nil
}

// checkImport2PCSetup validates the secondaries option against what the target
// cluster is. Two-phase commit exists only on servers new enough to have it,
// and means something only when the target's import jobs are mirrored to a
// secondary by replication; every mismatch fails here, before any DDL runs,
// rather than as one rejected import job per collection.
func (t *Task) checkImport2PCSetup(ctx context.Context) error {
	hasSecondaries := len(t.args.Option.SecondaryEndpoints) != 0
	has2PC := t.grpc.HasFeature(milvus.Import2PC)

	if hasSecondaries && !has2PC {
		return fmt.Errorf("restore: --secondaries asks for two-phase committed imports, which need a " +
			"Milvus 3.0 or newer target; this server is older")
	}
	if !hasSecondaries && !has2PC {
		// An older server has neither the commit endpoint nor the rejection of
		// auto-commit imports in a replication topology, so nothing applies.
		return nil
	}

	repCfg, err := t.grpc.GetReplicateConfiguration(ctx)
	if err != nil {
		if hasSecondaries {
			return fmt.Errorf("restore: read the target's replication configuration to validate --secondaries: %w", err)
		}
		t.logger.Warn("cannot read the target's replication configuration, skipping the two-phase commit check",
			zap.Error(err))
		return nil
	}
	// The same shape the server itself checks: a topology edge, or more than
	// one known cluster.
	replicating := repCfg != nil &&
		(len(repCfg.GetCrossClusterTopology()) > 0 || len(repCfg.GetClusters()) > 1)

	if hasSecondaries && !replicating {
		return fmt.Errorf("restore: --secondaries is set (%s) but the target is not part of a replication "+
			"topology, so no cluster will ever mirror its import jobs for the secondaries to commit; "+
			"drop --secondaries, or set up replication first", strings.Join(t.args.Option.SecondaryEndpoints, ","))
	}
	if !hasSecondaries && replicating {
		return fmt.Errorf("restore: the target is part of a replication topology, where Milvus " +
			"rejects auto-commit import jobs; pass --secondaries with the secondaries' REST " +
			"endpoints so imports commit two-phase on all ends (the target also needs " +
			"dataCoord.import.enableInReplicatingCluster=true to accept import jobs at all)")
	}

	return nil
}

func (t *Task) restoreRBAC(ctx context.Context) error {
	if !t.args.Option.RestoreRBAC {
		t.logger.Info("skip restore RBAC")
		return nil
	}

	curRBAC, err := t.grpc.BackupRBAC(ctx)
	if err != nil {
		return fmt.Errorf("restore: get current rbac: %w", err)
	}

	users := conv.Users(t.args.Backup.GetRbacMeta().GetUsers(), curRBAC.GetRBACMeta().GetUsers())
	roles := conv.Roles(t.args.Backup.GetRbacMeta().GetRoles(), curRBAC.GetRBACMeta().GetRoles())
	grants := conv.Grants(t.args.Backup.GetRbacMeta().GetGrants(), curRBAC.GetRBACMeta().GetGrants())
	privilegeGroups := conv.PrivilegeGroups(t.args.Backup.GetRbacMeta().GetPrivilegeGroups(), curRBAC.GetRBACMeta().GetPrivilegeGroups())

	rbacMeta := &milvuspb.RBACMeta{
		Users:           users,
		Roles:           roles,
		Grants:          grants,
		PrivilegeGroups: privilegeGroups,
	}

	t.logger.Info("insert rbac to milvus",
		zap.Int("users", len(users)),
		zap.Int("roles", len(roles)),
		zap.Int("grants", len(grants)),
		zap.Int("privilege_groups", len(privilegeGroups)))

	if err := t.grpc.RestoreRBAC(ctx, rbacMeta); err != nil {
		return fmt.Errorf("restore: restore rbac: %w", err)
	}

	return nil
}

func (t *Task) runDBTasks(ctx context.Context, dbTasks []*databaseTask) error {
	t.logger.Info("start restore database")

	// if the database is existed, skip restore
	dbs, err := t.grpc.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}
	t.logger.Debug("list databases", zap.Strings("databases", dbs))
	dbInTarget := lo.SliceToMap(dbs, func(db string) (string, struct{}) { return db, struct{}{} })

	for _, dbTask := range dbTasks {
		// We do not support db renaming yet.
		// The db in the source backup cannot be repeated,
		// so there is no need to deduplicate targetDBName.
		if _, ok := dbInTarget[dbTask.targetName]; ok {
			t.logger.Info("skip restore database", zap.String("db_name", dbTask.targetName))
			continue
		}

		if err := dbTask.Execute(ctx); err != nil {
			return fmt.Errorf("restore: restore database %w", err)
		}
		t.logger.Debug("finish restore database", zap.String("db_name", dbTask.targetName))
	}

	t.logger.Info("finish restore all database")

	return nil
}

// prepareDB create database if not exist, for restore collection task.
func (t *Task) prepareDB(ctx context.Context, collTargets []collTarget) error {
	dbInTarget, err := t.grpc.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}

	dbs := make(map[string]struct{})
	for _, tgt := range collTargets {
		dbs[tgt.target.DBName()] = struct{}{}
	}
	dbsNeedToRestores := lo.Keys(dbs)

	dbNotInTarget := lo.Without(dbsNeedToRestores, dbInTarget...)
	for _, db := range dbNotInTarget {
		if err := t.grpc.CreateDatabase(ctx, db); err != nil {
			return fmt.Errorf("restore: create database %w", err)
		}
		t.logger.Info("create db done", zap.String("database", db))
	}

	return nil
}
