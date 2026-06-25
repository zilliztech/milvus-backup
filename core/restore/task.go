package restore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/breakpoint"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
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

	// CollOverrides contains per-collection overrides keyed by target namespace string.
	CollOverrides map[string]CollOverride

	// TaskFilter filters databases and collections after mapping.
	TaskFilter filter.Filter
}

// CollMapper is the interface for renaming collection.
type CollMapper interface {
	// TagetNS renames the given namespace (database and collection) according to the renaming rules.
	TagetNS(ns namespace.NS) []namespace.NS
}

var _ CollMapper = (*DefaultCollMapper)(nil)

// DefaultCollMapper is the default collMapper that returns the original namespace.
type DefaultCollMapper struct{}

func NewDefaultCollMapper() *DefaultCollMapper                      { return &DefaultCollMapper{} }
func (r *DefaultCollMapper) TagetNS(ns namespace.NS) []namespace.NS { return []namespace.NS{ns} }

var _ CollMapper = (*SuffixMapper)(nil)

// SuffixMapper mapping the collection by adding a suffix.
type SuffixMapper struct {
	suffix string
}

func NewSuffixMapper(suffix string) *SuffixMapper { return &SuffixMapper{suffix: suffix} }

func (s *SuffixMapper) TagetNS(ns namespace.NS) []namespace.NS {
	return []namespace.NS{namespace.New(ns.DBName(), ns.CollName()+s.suffix)}
}

var _ CollMapper = (*TableMapper)(nil)

// TableMapper generates target namespace from source namespace by lookup a mapping table.
type TableMapper struct {
	DBWildcard map[string]string         // dbName -> newDbName, from db1.*:db2.*
	NSMapping  map[string][]namespace.NS // dbName.collName -> newCollName, from db1.coll1:db2.coll2 and coll1:coll2
}

func (r *TableMapper) TagetNS(ns namespace.NS) []namespace.NS {
	if newNSes, ok := r.NSMapping[ns.String()]; ok {
		return newNSes
	}

	if newDBName, ok := r.DBWildcard[ns.DBName()]; ok {
		return []namespace.NS{namespace.New(newDBName, ns.CollName())}
	}

	return []namespace.NS{ns}
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
	MetaOnly           bool
	UseV2Restore       bool
	TruncateBinlogByTs bool

	RestoreRBAC bool

	// EZKMapping maps old encryption keys to new ones.
	// During restore, the old EZK from backup is looked up in this map
	// and replaced with the new EZK if a match is found.
	EZKMapping map[string]string

	// ── Resumable restore (breakpoint) options ──
	// All of the following only take effect when a breakpoint tracker is
	// attached to the task (i.e. --breakpoint was supplied). With no
	// breakpoint the restore behaves exactly as before.

	// SegmentsPerBatch caps how many segments go into a single import job.
	// Smaller value => smaller blast radius per failure. <=0 keeps the
	// historical default (_bulkInsertRestfulAPIChunkSize).
	SegmentsPerBatch int
	// MaxRetry is the number of extra attempts for a failed import job
	// (with exponential backoff). 0 means no retry (issue once).
	MaxRetry int
	// RetryBaseBackoff / RetryMaxBackoff bound the backoff between retries.
	RetryBaseBackoff time.Duration
	RetryMaxBackoff  time.Duration
	// Resume, when true, skips segments already recorded complete in the
	// breakpoint ledger and reconciles in-flight jobs against Milvus. It
	// also switches collection DDL to "create if absent, never drop".
	Resume bool
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

	TaskMgr *taskmgr.Mgr

	// BreakpointPath, when non-empty, enables resumable restore: progress is
	// persisted to this local JSON file and (with Option.Resume) read back to
	// skip already-imported segments.
	BreakpointPath string
}

type Task struct {
	args TaskArgs

	grpc    milvus.Grpc
	restful milvus.Restful

	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	// bp is the resume ledger; nil when BreakpointPath is empty (feature off).
	bp *breakpoint.Tracker

	logger *zap.Logger
}

func NewTask(args TaskArgs) (*Task, error) {
	logger := log.With(zap.String("backup_name", args.Backup.GetName()), zap.String("task_id", args.TaskID))

	args.TaskMgr.AddRestoreTask(args.TaskID)

	var bp *breakpoint.Tracker
	if args.BreakpointPath != "" {
		var err error
		bp, err = breakpoint.Open(args.BreakpointPath, args.TaskID, args.Backup.GetName())
		if err != nil {
			return nil, fmt.Errorf("restore: open breakpoint ledger: %w", err)
		}
		logger.Info("breakpoint enabled", zap.String("path", args.BreakpointPath),
			zap.String("restore_id", bp.RestoreID()), zap.Bool("resume", args.Option.Resume))
	}

	return &Task{
		args: args,

		copySem:       semaphore.NewWeighted(int64(args.Params.Backup.Parallelism.CopyData.Val)),
		bulkInsertSem: semaphore.NewWeighted(int64(args.Params.Backup.Parallelism.ImportJob.Val)),

		bp: bp,

		logger: logger,
	}, nil
}

func (t *Task) newDBAndCollTasks(backup *backuppb.BackupInfo) ([]*databaseTask, []*collTask) {
	dbNames := lo.Map(backup.GetDatabaseBackups(), func(db *backuppb.DatabaseBackupInfo, _ int) string { return db.GetDbName() })
	t.logger.Info("databases in backup", zap.Strings("db_names", dbNames))
	collNSs := lo.Map(backup.GetCollectionBackups(), func(coll *backuppb.CollectionBackupInfo, _ int) string {
		return namespace.New(coll.GetDbName(), coll.GetCollectionName()).String()
	})
	t.logger.Info("collections in backup", zap.Strings("coll_names", collNSs))

	// filter backup
	dbBackups := t.filterDBBackup(backup.GetDatabaseBackups())
	collBackups := t.filterCollBackup(backup.GetCollectionBackups())
	dbNames = lo.Map(dbBackups, func(db *backuppb.DatabaseBackupInfo, _ int) string { return db.GetDbName() })
	t.logger.Info("databases backup after filtering", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collBackups, func(coll *backuppb.CollectionBackupInfo, _ int) string {
		return namespace.New(coll.GetDbName(), coll.GetCollectionName()).String()
	})
	t.logger.Info("collections backup after filtering", zap.Strings("coll_names", collNSs))

	// generate restore tasks
	dbTasks := t.newDBTasks(dbBackups)
	collTasks := t.newCollTasks(dbBackups, collBackups)
	dbNames = lo.Map(dbTasks, func(db *databaseTask, _ int) string { return db.targetName })
	t.logger.Info("databases task after mapping", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collTasks, func(coll *collTask, _ int) string { return coll.targetNS.String() })
	t.logger.Info("collections task after mapping", zap.Strings("ns", collNSs))

	// filter task
	dbTasks = t.filterDBTask(dbTasks)
	collTasks = t.filterCollTask(collTasks)
	dbNames = lo.Map(dbTasks, func(db *databaseTask, _ int) string { return db.targetName })
	t.logger.Info("databases task after filtering", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collTasks, func(coll *collTask, _ int) string { return coll.targetNS.String() })
	t.logger.Info("collections task after filtering", zap.Strings("coll_names", collNSs))

	return dbTasks, collTasks
}

func (t *Task) filterDBBackup(dbBackups []*backuppb.DatabaseBackupInfo) []*backuppb.DatabaseBackupInfo {
	return lo.Filter(dbBackups, func(dbBackup *backuppb.DatabaseBackupInfo, _ int) bool {
		return t.args.Plan.BackupFilter.AllowDB(dbBackup.GetDbName())
	})
}

func (t *Task) filterCollBackup(collBackups []*backuppb.CollectionBackupInfo) []*backuppb.CollectionBackupInfo {
	return lo.Filter(collBackups, func(collBackup *backuppb.CollectionBackupInfo, _ int) bool {
		ns := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())
		return t.args.Plan.BackupFilter.AllowNS(ns)
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

func (t *Task) newCollTask(dbBackup *backuppb.DatabaseBackupInfo, collBackup *backuppb.CollectionBackupInfo) []*collTask {
	sourceNS := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())
	targetNSes := t.args.Plan.CollMapper.TagetNS(sourceNS)

	tasks := make([]*collTask, 0, len(targetNSes))
	for _, targetNS := range targetNSes {
		t.logger.Debug("generate restore collection task", zap.String("source", sourceNS.String()), zap.String("target", targetNS.String()))
		args := collTaskArgs{
			taskID:        t.args.TaskID,
			taskMgr:       t.args.TaskMgr,
			targetNS:      targetNS,
			dbBackup:      dbBackup,
			collBackup:    collBackup,
			option:        t.args.Option,
			collOverride:  t.args.Plan.CollOverrides[targetNS.String()],
			crossStorage:  t.args.Params.Minio.CrossStorage.Val,
			keepTempFiles: t.args.Params.Backup.KeepTempFiles.Val,
			backupDir:     t.args.BackupDir,
			backupStorage: t.args.BackupStorage,
			milvusStorage: t.args.MilvusStorage,
			copySem:       t.copySem,
			bulkInsertSem: t.bulkInsertSem,
			grpcCli:       t.grpc,
			restfulCli:    t.restful,
			bp:            t.bp,
		}

		tasks = append(tasks, newCollTask(args))
	}

	return tasks
}

func (t *Task) newCollTasks(dbBackups []*backuppb.DatabaseBackupInfo, collBackups []*backuppb.CollectionBackupInfo) []*collTask {
	nameDBBackup := lo.SliceToMap(dbBackups, func(dbBackup *backuppb.DatabaseBackupInfo) (string, *backuppb.DatabaseBackupInfo) {
		return dbBackup.GetDbName(), dbBackup
	})

	collTasks := make([]*collTask, 0, len(collBackups))
	for _, collBackup := range collBackups {
		dbBackup := nameDBBackup[collBackup.GetDbName()]
		tasks := t.newCollTask(dbBackup, collBackup)
		collTasks = append(collTasks, tasks...)
	}

	return collTasks
}

func (t *Task) filterDBTask(dbTask []*databaseTask) []*databaseTask {
	return lo.Filter(dbTask, func(task *databaseTask, _ int) bool {
		return t.args.Plan.TaskFilter.AllowDB(task.targetName)
	})
}

func (t *Task) filterCollTask(collTasks []*collTask) []*collTask {
	return lo.Filter(collTasks, func(task *collTask, _ int) bool {
		return t.args.Plan.TaskFilter.AllowNS(task.targetNS)
	})
}

// checkCollsExist check if the collection exist in target milvus, if collection exist, return error.
func (t *Task) checkCollsExist(ctx context.Context, collTasks []*collTask) error {
	for _, collTask := range collTasks {
		if err := t.checkCollExist(ctx, collTask); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) checkCollExist(ctx context.Context, task *collTask) error {
	// Resume tolerates a pre-existing collection: it was created by an earlier
	// run of this restore and may already hold imported data. createColl/
	// dropExistedColl handle the "create if absent, never drop" behavior.
	if t.args.Option.Resume {
		return nil
	}

	has, err := t.grpc.HasCollection(ctx, task.targetNS.DBName(), task.targetNS.CollName())
	if err != nil {
		return fmt.Errorf("restore: check collection %w", err)
	}

	if t.args.Option.SkipCreateCollection && t.args.Option.DropExistCollection {
		return fmt.Errorf("restore: skip create and drop exist collection can not be true at the same time collection %s", task.targetNS.String())
	}

	// collection not exist and not create collection
	if !has && t.args.Option.SkipCreateCollection {
		return fmt.Errorf("restore: collection not exist, database %s collection %s", task.targetNS.DBName(), task.targetNS.CollName())
	}

	// collection existed and not drop collection
	if has && !t.args.Option.SkipCreateCollection && !t.args.Option.DropExistCollection {
		return fmt.Errorf("restore: collection already exist, database %s collection %s", task.targetNS.DBName(), task.targetNS.CollName())
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

	return nil
}

func (t *Task) closeClients() {
	if t.grpc != nil {
		if err := t.grpc.Close(); err != nil {
			t.logger.Warn("close grpc client", zap.Error(err))
		}
	}
}

func (t *Task) Execute(ctx context.Context) error {
	defer t.closeClients()
	if err := t.initClients(); err != nil {
		return err
	}

	if err := t.privateExecute(ctx); err != nil {
		t.logger.Error("restore task failed", zap.Error(err))
		t.args.TaskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("restore: execute %w", err)
	}

	t.logger.Info("restore task finished")
	t.args.TaskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreSuccess())
	return nil
}

func (t *Task) privateExecute(ctx context.Context) error {
	t.args.TaskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreExecuting())

	if t.bp != nil && t.args.Option.Resume {
		if err := t.reconcileInflight(ctx); err != nil {
			return fmt.Errorf("restore: reconcile breakpoint inflight jobs: %w", err)
		}
	}

	dbTasks, collTasks := t.newDBAndCollTasks(t.args.Backup)

	if err := t.restoreRBAC(ctx); err != nil {
		return fmt.Errorf("restore: restore rbac %w", err)
	}

	if err := t.runDBTasks(ctx, dbTasks); err != nil {
		return fmt.Errorf("restore: run database task %w", err)
	}

	if err := t.prepareDB(ctx, collTasks); err != nil {
		return fmt.Errorf("restore: prepare database %w", err)
	}

	if err := t.checkCollsExist(ctx, collTasks); err != nil {
		return fmt.Errorf("restore: check collection exist %w", err)
	}

	if err := t.runCollTasks(ctx, collTasks); err != nil {
		return fmt.Errorf("restore: run collection task %w", err)
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
func (t *Task) prepareDB(ctx context.Context, collTasks []*collTask) error {
	dbInTarget, err := t.grpc.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}

	dbs := make(map[string]struct{})
	for _, collTask := range collTasks {
		dbs[collTask.targetNS.DBName()] = struct{}{}
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

// reconcileInflight resolves jobs that were issued in a previous run but whose
// terminal state was never persisted (e.g. the tool was killed right after the
// job completed). For each it queries Milvus: a completed job is promoted to the
// completed set so its segments are NOT re-imported (which would duplicate
// data); a failed/missing job is dropped so its segments fall back into the
// to-do set (re-import is safe — a non-completed import commits no visible
// data); an in-progress job is polled to a terminal state first.
func (t *Task) reconcileInflight(ctx context.Context) error {
	inflight := t.bp.Inflight()
	if len(inflight) == 0 {
		return nil
	}
	t.logger.Info("reconciling breakpoint inflight jobs", zap.Int("count", len(inflight)))

	for jobID, infl := range inflight {
		ns, err := namespace.Parse(infl.NS)
		if err != nil {
			return fmt.Errorf("reconcile: parse ns %q: %w", infl.NS, err)
		}

		state, reason, err := t.waitImportTerminal(ctx, ns.DBName(), jobID)
		if err != nil {
			t.logger.Warn("reconcile: cannot resolve inflight job, its segments will be re-imported",
				zap.String("job_id", jobID), zap.Error(err))
			if ferr := t.bp.Fail(jobID); ferr != nil {
				return ferr
			}
			continue
		}

		if state == string(milvus.ImportStateCompleted) {
			t.logger.Info("reconcile: inflight job already completed, keeping its segments",
				zap.String("job_id", jobID))
			if cerr := t.bp.Complete(jobID); cerr != nil {
				return cerr
			}
			continue
		}

		t.logger.Info("reconcile: inflight job not completed, dropping",
			zap.String("job_id", jobID), zap.String("state", state), zap.String("reason", reason))
		if ferr := t.bp.Fail(jobID); ferr != nil {
			return ferr
		}
	}

	return nil
}

// waitImportTerminal polls an import job until it reaches Completed or Failed.
func (t *Task) waitImportTerminal(ctx context.Context, db, jobID string) (string, string, error) {
	for {
		resp, err := t.restful.GetBulkInsertState(ctx, db, jobID)
		if err != nil {
			return "", "", err
		}
		switch resp.Data.State {
		case string(milvus.ImportStateCompleted):
			return resp.Data.State, "", nil
		case string(milvus.ImportStateFailed):
			return resp.Data.State, resp.Data.Reason, nil
		default:
			select {
			case <-ctx.Done():
				return "", "", ctx.Err()
			case <-time.After(_bulkInsertCheckInterval):
			}
		}
	}
}

func (t *Task) runCollTasks(ctx context.Context, collTasks []*collTask) error {
	t.logger.Info("start restore collection")

	// In resumable mode a single collection's failure must not abort the
	// siblings: every collection that can make progress should, and the
	// breakpoint records what is left for the next --resume run.
	isolate := t.bp != nil
	var (
		mu     sync.Mutex
		failed []string
	)

	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(t.args.Params.Backup.Parallelism.RestoreCollection.Val)
	for _, collTask := range collTasks {
		g.Go(func() error {
			if err := collTask.Execute(subCtx); err != nil {
				if isolate {
					mu.Lock()
					failed = append(failed, fmt.Sprintf("%s: %v", collTask.targetNS.String(), err))
					mu.Unlock()
					t.logger.Error("restore collection failed; other collections continue",
						zap.String("ns", collTask.targetNS.String()), zap.Error(err))
					return nil
				}
				return fmt.Errorf("restore: restore collection %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore: wait restore collections %w", err)
	}

	if len(failed) > 0 {
		return fmt.Errorf("restore: %d collection(s) did not fully complete, re-run with --resume to continue; failures: %s",
			len(failed), strings.Join(failed, "; "))
	}

	t.logger.Info("finish restore all collections")
	return nil
}
