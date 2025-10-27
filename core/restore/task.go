package restore

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

type DBMapping struct {
	Target   string
	WithProp bool
}

type CollFilter struct {
	AllowAll bool
	CollName map[string]struct{}
}

type SkipParams struct {
	CollectionProperties []string

	FieldIndexParams []string
	FieldTypeParams  []string

	IndexParams []string
}

type Plan struct {
	// There is no need to do filtering before mapping.
	// It is mainly for backward compatibility and can be
	// removed after the dbCollection parameter is completely deprecated.
	DBBackupFilter   map[string]struct{}
	CollBackupFilter map[string]CollFilter

	// mapping
	DBMapper   map[string][]DBMapping
	CollMapper CollMapper

	// filter after mapping
	DBTaskFilter   map[string]struct{}
	CollTaskFilter map[string]CollFilter // dbName -> collFilter
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
}

type TaskArgs struct {
	TaskID string

	Backup *backuppb.BackupInfo

	Plan   *Plan
	Option *Option

	Params *paramtable.BackupParams

	BackupDir      string
	BackupRootPath string
	BackupStorage  storage.Client
	MilvusStorage  storage.Client

	Grpc    milvus.Grpc
	Restful milvus.Restful
}

type Task struct {
	logger *zap.Logger
	taskID string

	backup *backuppb.BackupInfo

	dbTasks   []*databaseTask
	collTasks []*collectionTask

	plan   *Plan
	option *Option

	params *paramtable.BackupParams

	taskMgr *taskmgr.Mgr

	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	backupStorage storage.Client
	milvusStorage storage.Client

	grpc    milvus.Grpc
	restful milvus.Restful

	backupDir string
}

func NewTask(args TaskArgs) (*Task, error) {
	logger := log.L().With(zap.String("backup_name", args.Backup.GetName()))

	return &Task{
		taskID: args.TaskID,

		logger: logger,

		backup: args.Backup,
		plan:   args.Plan,

		option: args.Option,
		params: args.Params,

		taskMgr: taskmgr.DefaultMgr,

		copySem:       semaphore.NewWeighted(args.Params.BackupCfg.BackupCopyDataParallelism),
		bulkInsertSem: semaphore.NewWeighted(args.Params.BackupCfg.ImportJobParallelism),

		backupStorage: args.BackupStorage,
		milvusStorage: args.MilvusStorage,

		grpc:    args.Grpc,
		restful: args.Restful,

		backupDir: args.BackupDir,
	}, nil
}

func (t *Task) newDBAndCollTasks(backup *backuppb.BackupInfo) ([]*databaseTask, []*collectionTask) {
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
	collTasks := t.newCollTasks(collBackups)
	dbNames = lo.Map(dbTasks, func(db *databaseTask, _ int) string { return db.targetName })
	t.logger.Info("databases task after mapping", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collTasks, func(coll *collectionTask, _ int) string { return coll.targetNS.String() })
	t.logger.Info("collections task after mapping", zap.Strings("ns", collNSs))

	// filter task
	dbTasks = t.filterDBTask(dbTasks)
	collTasks = t.filterCollTask(collTasks)
	dbNames = lo.Map(dbTasks, func(db *databaseTask, _ int) string { return db.targetName })
	t.logger.Info("databases task after filtering", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collTasks, func(coll *collectionTask, _ int) string { return coll.targetNS.String() })
	t.logger.Info("collections task after filtering", zap.Strings("coll_names", collNSs))

	return dbTasks, collTasks
}

func (t *Task) filterDBBackup(dbBackups []*backuppb.DatabaseBackupInfo) []*backuppb.DatabaseBackupInfo {
	if len(t.plan.DBBackupFilter) == 0 {
		return dbBackups
	}

	return lo.Filter(dbBackups, func(dbBackup *backuppb.DatabaseBackupInfo, _ int) bool {
		_, ok := t.plan.DBBackupFilter[dbBackup.GetDbName()]
		return ok
	})
}

func (t *Task) filterCollBackup(collBackups []*backuppb.CollectionBackupInfo) []*backuppb.CollectionBackupInfo {
	if len(t.plan.CollBackupFilter) == 0 {
		return collBackups
	}

	return lo.Filter(collBackups, func(collBackup *backuppb.CollectionBackupInfo, _ int) bool {
		filter, ok := t.plan.CollBackupFilter[collBackup.GetDbName()]
		if !ok {
			return false
		}

		if filter.AllowAll {
			return true
		}

		_, ok = filter.CollName[collBackup.GetCollectionName()]
		return ok
	})
}

func (t *Task) newDBTask(dbBak *backuppb.DatabaseBackupInfo) []*databaseTask {
	mappings, ok := t.plan.DBMapper[dbBak.GetDbName()]
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
	var dbTasks []*databaseTask
	for _, dbBackup := range dbBackups {
		tasks := t.newDBTask(dbBackup)
		dbTasks = append(dbTasks, tasks...)
	}

	return dbTasks
}

func (t *Task) newCollTask(collBackup *backuppb.CollectionBackupInfo) []*collectionTask {
	sourceNS := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())
	targetNSes := t.plan.CollMapper.TagetNS(sourceNS)

	tasks := make([]*collectionTask, 0, len(targetNSes))
	for _, targetNS := range targetNSes {
		t.logger.Debug("generate restore collection task", zap.String("source", sourceNS.String()), zap.String("target", targetNS.String()))
		args := collectionTaskArgs{
			taskID:         t.taskID,
			taskMgr:        t.taskMgr,
			targetNS:       targetNS,
			collBackup:     collBackup,
			option:         t.option,
			backupRootPath: t.backupDir,
			crossStorage:   t.params.MinioCfg.CrossStorage,
			keepTempFiles:  t.params.BackupCfg.KeepTempFiles,
			backupDir:      t.backupDir,
			backupStorage:  t.backupStorage,
			milvusStorage:  t.milvusStorage,
			copySem:        t.copySem,
			bulkInsertSem:  t.bulkInsertSem,
			grpcCli:        t.grpc,
			restfulCli:     t.restful,
		}

		tasks = append(tasks, newCollectionTask(args))
	}

	return tasks
}

func (t *Task) newCollTasks(collBackups []*backuppb.CollectionBackupInfo) []*collectionTask {
	var dbNameCollNameTask []*collectionTask
	for _, collBackup := range collBackups {
		tasks := t.newCollTask(collBackup)
		dbNameCollNameTask = append(dbNameCollNameTask, tasks...)
	}

	return dbNameCollNameTask
}

func (t *Task) filterDBTask(dbTask []*databaseTask) []*databaseTask {
	if len(t.plan.DBTaskFilter) == 0 {
		return dbTask
	}

	return lo.Filter(dbTask, func(task *databaseTask, _ int) bool {
		_, ok := t.plan.DBTaskFilter[task.targetName]
		return ok
	})
}

func (t *Task) filterCollTask(collTask []*collectionTask) []*collectionTask {
	if len(t.plan.CollTaskFilter) == 0 {
		return collTask
	}

	return lo.Filter(collTask, func(task *collectionTask, _ int) bool {
		filter, ok := t.plan.CollTaskFilter[task.targetNS.DBName()]
		if !ok {
			return false
		}

		if filter.AllowAll {
			return true
		}

		_, ok = filter.CollName[task.targetNS.CollName()]
		return ok
	})
}

// checkCollsExist check if the collection exist in target milvus, if collection exist, return error.
func (t *Task) checkCollsExist(ctx context.Context) error {
	for _, collTask := range t.collTasks {
		if err := t.checkCollExist(ctx, collTask); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) checkCollExist(ctx context.Context, task *collectionTask) error {
	has, err := t.grpc.HasCollection(ctx, task.targetNS.DBName(), task.targetNS.CollName())
	if err != nil {
		return fmt.Errorf("restore: check collection %w", err)
	}

	if t.option.SkipCreateCollection && t.option.DropExistCollection {
		return fmt.Errorf("restore: skip create and drop exist collection can not be true at the same time collection %s", task.targetNS.String())
	}

	// collection not exist and not create collection
	if !has && t.option.SkipCreateCollection {
		return fmt.Errorf("restore: collection not exist, database %s collection %s", task.targetNS.DBName(), task.targetNS.CollName())
	}

	// collection existed and not drop collection
	if has && !t.option.SkipCreateCollection && !t.option.DropExistCollection {
		return fmt.Errorf("restore: collection already exist, database %s collection %s", task.targetNS.DBName(), task.targetNS.CollName())
	}

	return nil
}

func (t *Task) Prepare(_ context.Context) error {
	t.taskMgr.AddRestoreTask(t.taskID)

	dbTasks, collTasks := t.newDBAndCollTasks(t.backup)
	t.dbTasks = dbTasks
	t.collTasks = collTasks

	return nil
}

func (t *Task) Execute(ctx context.Context) error {
	if err := t.privateExecute(ctx); err != nil {
		t.logger.Error("restore task failed", zap.Error(err))
		t.taskMgr.UpdateRestoreTask(t.taskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("restore: execute %w", err)
	}

	t.logger.Info("restore task finished")
	t.taskMgr.UpdateRestoreTask(t.taskID, taskmgr.SetRestoreSuccess())
	return nil
}

func (t *Task) privateExecute(ctx context.Context) error {
	if err := t.runRBACTask(ctx); err != nil {
		return err
	}

	t.taskMgr.UpdateRestoreTask(t.taskID, taskmgr.SetRestoreExecuting())

	if err := t.runDBTasks(ctx); err != nil {
		return fmt.Errorf("restore: run database task %w", err)
	}

	if err := t.prepareDB(ctx); err != nil {
		return fmt.Errorf("restore: prepare database %w", err)
	}

	if err := t.checkCollsExist(ctx); err != nil {
		return fmt.Errorf("restore: check collection exist %w", err)
	}

	if err := t.runCollTasks(ctx); err != nil {
		return fmt.Errorf("restore: run collection task %w", err)
	}

	return nil
}

func (t *Task) runRBACTask(ctx context.Context) error {
	if !t.option.RestoreRBAC {
		t.logger.Info("skip restore RBAC")
		return nil
	}

	t.logger.Info("start restore RBAC")
	rt := NewRBACTask(t.grpc, t.backup.GetRbacMeta())
	if err := rt.Execute(ctx); err != nil {
		return fmt.Errorf("restore: restore RBAC %w", err)
	}

	return nil
}

func (t *Task) runDBTasks(ctx context.Context) error {
	t.logger.Info("start restore database")

	// if the database is existed, skip restore
	dbs, err := t.grpc.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}
	t.logger.Debug("list databases", zap.Strings("databases", dbs))
	dbInTarget := lo.SliceToMap(dbs, func(db string) (string, struct{}) { return db, struct{}{} })

	for _, dbTask := range t.dbTasks {
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
func (t *Task) prepareDB(ctx context.Context) error {
	dbInTarget, err := t.grpc.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}

	dbs := make(map[string]struct{})
	for _, collTask := range t.collTasks {
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

func (t *Task) runCollTasks(ctx context.Context) error {
	t.logger.Info("start restore collection")

	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(t.params.BackupCfg.RestoreParallelism)
	for _, collTask := range t.collTasks {
		g.Go(func() error {
			logger := t.logger.With(zap.String("target_ns", collTask.targetNS.String()))
			if err := collTask.Execute(subCtx); err != nil {
				logger.Error("restore coll failed", zap.Error(err))
				return fmt.Errorf("restore: restore collection %w", err)
			}

			logger.Info("finish restore collection")
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("restore: wait collection worker pool %w", err)
	}

	task, err := t.taskMgr.GetRestoreTask(t.taskID)
	if err != nil {
		return fmt.Errorf("restore: get restore task %w", err)
	}
	duration := time.Since(task.StartTime())
	t.logger.Info("finish restore all collections", zap.Int("collection_num", len(t.collTasks)),
		zap.Duration("duration", duration))
	return nil
}
