package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/meta/taskmgr"
	"github.com/zilliztech/milvus-backup/core/namespace"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type Task struct {
	logger *zap.Logger

	request *backuppb.RestoreBackupRequest

	backup *backuppb.BackupInfo

	params  *paramtable.BackupParams
	taskMgr *taskmgr.Mgr

	task *backuppb.RestoreBackupTask

	backupStorage storage.ChunkManager
	milvusStorage storage.ChunkManager

	grpc    client.Grpc
	restful client.Restful

	backupBucketName string
	backupPath       string
}

func NewTask(
	request *backuppb.RestoreBackupRequest,
	backupPath string,
	backupBucketName string,
	params *paramtable.BackupParams,
	info *backuppb.BackupInfo,
	backupStorage storage.ChunkManager,
	milvusStorage storage.ChunkManager,
	grpcCli client.Grpc,
	restfulCli client.Restful,
) *Task {
	logger := log.L().With(
		zap.String("backup_name", info.GetName()),
		zap.String("backup_path", backupPath),
		zap.String("backup_bucket_name", backupBucketName),
		zap.String("request_id", request.GetRequestId()))

	return &Task{
		logger: logger,

		request: request,
		backup:  info,

		params:  params,
		taskMgr: taskmgr.DefaultMgr,

		backupStorage: backupStorage,
		milvusStorage: milvusStorage,

		grpc:    grpcCli,
		restful: restfulCli,

		backupBucketName: backupBucketName,
		backupPath:       backupPath,
	}
}

func (t *Task) getDBNameDBBackup() map[string]*backuppb.DatabaseBackupInfo {
	bakDB := make(map[string]*backuppb.DatabaseBackupInfo, len(t.backup.GetDatabaseBackups()))
	for _, dbBackup := range t.backup.GetDatabaseBackups() {
		bakDB[dbBackup.GetDbName()] = dbBackup
	}

	return bakDB
}

func (t *Task) getDBNameCollNameCollBackup() map[string]map[string]*backuppb.CollectionBackupInfo {
	bakDBColl := make(map[string]map[string]*backuppb.CollectionBackupInfo, len(t.backup.GetCollectionBackups()))
	for _, collectionBackup := range t.backup.GetCollectionBackups() {
		dbName := collectionBackup.GetDbName()
		if dbName == "" {
			dbName = namespace.DefaultDBName
		}
		if _, ok := bakDBColl[dbName]; ok {
			bakDBColl[dbName][collectionBackup.GetCollectionName()] = collectionBackup
		} else {
			bakDBColl[dbName] = make(map[string]*backuppb.CollectionBackupInfo)
			bakDBColl[dbName][collectionBackup.GetCollectionName()] = collectionBackup
		}
	}

	return bakDBColl
}

func (t *Task) filterDBBackup(dbCollections meta.DbCollections) []*backuppb.DatabaseBackupInfo {
	bakDB := t.getDBNameDBBackup()

	needRestoreDBs := make([]*backuppb.DatabaseBackupInfo, 0, len(bakDB))
	for db := range dbCollections {
		dbBackup, ok := bakDB[db]
		if !ok {
			t.logger.Warn("database not exist in backup", zap.String("db_name", db))
			continue
		}
		needRestoreDBs = append(needRestoreDBs, dbBackup)
	}

	return needRestoreDBs
}

func (t *Task) filterCollBackup(dbCollections meta.DbCollections) ([]*backuppb.CollectionBackupInfo, error) {
	bakDBColl := t.getDBNameCollNameCollBackup()

	needRestoreColls := make([]*backuppb.CollectionBackupInfo, 0, len(bakDBColl))
	for db, colls := range dbCollections {
		collNameColl, ok := bakDBColl[db]
		// if colls is not empty, it means only the specified collections need to be restored,
		// so we need to check if the specified collections exist in the backup
		if len(colls) != 0 && !ok {
			return nil, fmt.Errorf("restore: database %s not exist in backup", db)
		}

		// if colls is empty, it means all collections in the database need to be restored
		if len(colls) == 0 {
			needRestoreColls = append(needRestoreColls, lo.Values(collNameColl)...)
			continue
		}

		// if colls is not empty, it means only the specified collections need to be restored
		for _, coll := range colls {
			collBackup, has := collNameColl[coll]
			if !has {
				return nil, fmt.Errorf("restore: collection %s not exist in backup", coll)
			}
			needRestoreColls = append(needRestoreColls, collBackup)
		}
	}

	return needRestoreColls, nil
}

func (t *Task) filterBackupByDBCollections(dbCollectionsStr string) ([]*backuppb.DatabaseBackupInfo, []*backuppb.CollectionBackupInfo, error) {
	var dbCollections meta.DbCollections
	if err := json.Unmarshal([]byte(dbCollectionsStr), &dbCollections); err != nil {
		return nil, nil, fmt.Errorf("restore: unmarshal dbCollections %w", err)
	}

	dbBackups := t.filterDBBackup(dbCollections)

	collBackups, err := t.filterCollBackup(dbCollections)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: filter collection backups by dbColletions %w", err)
	}

	return dbBackups, collBackups, nil
}

func (t *Task) filterBackupByCollectionNames(collectionNames []string) ([]*backuppb.DatabaseBackupInfo, []*backuppb.CollectionBackupInfo, error) {
	dbCollections := meta.DbCollections{}
	// dbAndCollName format: dbName.collectionName
	// 1. if dbAndCollName contains ".", split it into dbName and collectionName
	// 2. if dbAndCollName does not contain ".", set dbName to default
	for _, dbAndCollName := range collectionNames {
		if strings.Contains(dbAndCollName, ".") {
			split := strings.Split(dbAndCollName, ".")
			if len(split) != 2 {
				return nil, nil, fmt.Errorf("restore: dbAndCollName %s format error", dbAndCollName)
			}
			dbCollections[split[0]] = append(dbCollections[split[0]], split[1])
		} else {
			dbCollections[namespace.DefaultDBName] = append(dbCollections[namespace.DefaultDBName], dbAndCollName)
		}
	}

	dbBackups := t.filterDBBackup(dbCollections)

	collBackups, err := t.filterCollBackup(dbCollections)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: filter collection backups by collection names %w", err)
	}

	return dbBackups, collBackups, nil
}

func (t *Task) filterBackup() ([]*backuppb.DatabaseBackupInfo, []*backuppb.CollectionBackupInfo, error) {
	dbCollectionsStr := utils.GetDBCollections(t.request.GetDbCollections())
	if dbCollectionsStr != "" {
		dbBackups, collBackups, err := t.filterBackupByDBCollections(dbCollectionsStr)
		if err != nil {
			return nil, nil, fmt.Errorf("restore: filter backup by dbCollections %w", err)
		}
		return dbBackups, collBackups, nil
	}

	if len(t.request.GetCollectionNames()) > 0 {
		dbBackups, collBackups, err := t.filterBackupByCollectionNames(t.request.GetCollectionNames())
		if err != nil {
			return nil, nil, fmt.Errorf("restore: filter backup by collection names %w", err)
		}
		return dbBackups, collBackups, nil
	}

	// if dbCollectionsStr is empty and collectionNames is empty,
	// it means all collections in the backup need to be restored
	dbBackups := t.getDBNameDBBackup()
	var collBackups []*backuppb.CollectionBackupInfo
	for _, colls := range t.getDBNameCollNameCollBackup() {
		collBackups = append(collBackups, lo.Values(colls)...)
	}
	return lo.Values(dbBackups), collBackups, nil
}

func (t *Task) newRenamer() (nsRenamer, error) {
	if len(t.request.GetCollectionRenames()) != 0 {
		t.logger.Info("new renamer from collection renames", zap.Any("renames", t.request.GetCollectionRenames()))
		renamer, err := newMapRenamer(t.request.GetCollectionRenames())
		if err != nil {
			return nil, fmt.Errorf("restore: create map renamer %w", err)
		}
		return renamer, nil
	}

	if t.request.GetCollectionSuffix() != "" {
		t.logger.Info("new renamer from collection suffix", zap.String("suffix", t.request.GetCollectionSuffix()))
		renamer := newSuffixNameRenamer(t.request.GetCollectionSuffix())
		return renamer, nil
	}

	t.logger.Info("new default renamer")
	return newDefaultNSRenamer(), nil
}

func (t *Task) newCollTaskPB(targetNS namespace.NS, bak *backuppb.CollectionBackupInfo) (*backuppb.RestoreCollectionTask, error) {
	if bak.GetCollectionName() == "" {
		return nil, fmt.Errorf("restore: collection name is empty")
	}

	size := lo.SumBy(bak.GetPartitionBackups(), func(partition *backuppb.PartitionBackupInfo) int64 { return partition.GetSize() })

	collTaskPB := &backuppb.RestoreCollectionTask{
		Id:                   uuid.NewString(),
		StartTime:            time.Now().Unix(),
		CollBackup:           bak,
		TargetDbName:         targetNS.DBName(),
		TargetCollectionName: targetNS.CollName(),
		ToRestoreSize:        size,
		MetaOnly:             t.request.GetMetaOnly(),
		RestoreIndex:         t.request.GetRestoreIndex(),
		UseAutoIndex:         t.request.GetUseAutoIndex(),
		DropExistCollection:  t.request.GetDropExistCollection(),
		DropExistIndex:       t.request.GetDropExistIndex(),
		SkipCreateCollection: t.request.GetSkipCreateCollection(),
		MaxShardNum:          t.request.GetMaxShardNum(),
		SkipParams:           t.request.GetSkipParams(),
		UseV2Restore:         t.request.GetUseV2Restore(),
		TruncateBinlogByTs:   t.request.GetTruncateBinlogByTs(),
	}

	return collTaskPB, nil
}

func (t *Task) newDBTaskPB(bak *backuppb.DatabaseBackupInfo) (*backuppb.RestoreDatabaseTask, error) {
	if bak.GetDbName() == "" {
		return nil, fmt.Errorf("restore: database name is empty")
	}

	// currently we can not support rename database
	dbTaskPB := &backuppb.RestoreDatabaseTask{
		Id:           uuid.NewString(),
		DbBackup:     bak,
		TargetDbName: bak.GetDbName(),
	}

	return dbTaskPB, nil
}

func (t *Task) newRestoreTaskPB() (*backuppb.RestoreBackupTask, error) {
	dbBackups, collBackups, err := t.filterBackup()
	if err != nil {
		return nil, fmt.Errorf("restore: filter backup %w", err)
	}

	dbTasks, collTasks, err := t.renameAndFilter(dbBackups, collBackups)
	if err != nil {
		return nil, fmt.Errorf("restore: rename and filter backck to build task %w", err)
	}

	size := lo.SumBy(collTasks, func(coll *backuppb.RestoreCollectionTask) int64 { return coll.ToRestoreSize })
	task := &backuppb.RestoreBackupTask{
		Id:                     t.request.GetId(),
		StartTime:              time.Now().Unix(),
		ToRestoreSize:          size,
		DatabaseRestoreTasks:   dbTasks,
		CollectionRestoreTasks: collTasks,
	}

	return task, nil
}

// renameAndFilter rename db and collection in need to restore, by db_collections_after_rename.
// and then build restore task.
func (t *Task) renameAndFilter(dbBackups []*backuppb.DatabaseBackupInfo, collBackups []*backuppb.CollectionBackupInfo) ([]*backuppb.RestoreDatabaseTask, []*backuppb.RestoreCollectionTask, error) {
	dbNameDBTask, err := t.newDBTaskPBs(dbBackups)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: create database task %w", err)
	}

	dbNameCollNameTask, err := t.newCollTaskPBs(collBackups)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: create collection task %w", err)
	}

	dbCollectionsStr := utils.GetDBCollections(t.request.GetDbCollectionsAfterRename())
	if dbCollectionsStr == "" {
		dbTasks := lo.Values(dbNameDBTask)
		var collTasks []*backuppb.RestoreCollectionTask
		for _, collNameColl := range dbNameCollNameTask {
			collTasks = append(collTasks, lo.Values(collNameColl)...)
		}
		return dbTasks, collTasks, nil
	}

	dbTask, collTask, err := t.filterTask(dbCollectionsStr, dbNameDBTask, dbNameCollNameTask)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: filter task %w", err)
	}

	return dbTask, collTask, nil
}

func (t *Task) filterTask(dbCollectionsStr string, dbNameDBTask map[string]*backuppb.RestoreDatabaseTask, dbNameCollNameTask map[string]map[string]*backuppb.RestoreCollectionTask) ([]*backuppb.RestoreDatabaseTask, []*backuppb.RestoreCollectionTask, error) {
	var dbCollections meta.DbCollections
	if err := json.Unmarshal([]byte(dbCollectionsStr), &dbCollections); err != nil {
		return nil, nil, fmt.Errorf("restore: filter task unmarshal dbCollections %w", err)
	}

	dbTasks := t.filterDBTask(dbCollections, dbNameDBTask)

	collTasks, err := t.filterCollTask(dbCollections, dbNameCollNameTask)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: filter collection task %w", err)
	}

	return dbTasks, collTasks, nil
}

func (t *Task) filterDBTask(dbCollections meta.DbCollections, dbNameDBTask map[string]*backuppb.RestoreDatabaseTask) []*backuppb.RestoreDatabaseTask {
	needRestoreDBs := make([]*backuppb.RestoreDatabaseTask, 0, len(dbCollections))
	for dbName, task := range dbNameDBTask {
		if _, ok := dbCollections[dbName]; ok {
			needRestoreDBs = append(needRestoreDBs, task)
		} else {
			t.logger.Info("skip restore database", zap.String("db_name", dbName))
		}
	}

	return needRestoreDBs
}

func (t *Task) filterCollTask(dbCollections meta.DbCollections, dbNameCollNameTask map[string]map[string]*backuppb.RestoreCollectionTask) ([]*backuppb.RestoreCollectionTask, error) {
	needRestoreColls := make([]*backuppb.RestoreCollectionTask, 0, len(dbCollections))
	for dbName, collNames := range dbCollections {
		// means restore all collections in the database
		if len(collNames) == 0 {
			needRestoreColls = append(needRestoreColls, lo.Values(dbNameCollNameTask[dbName])...)
		}

		for _, collName := range collNames {
			if task, ok := dbNameCollNameTask[dbName][collName]; ok {
				needRestoreColls = append(needRestoreColls, task)
			} else {
				return nil, fmt.Errorf("restore: collection %s not exist in backup after rename", collName)
			}
		}
	}

	return needRestoreColls, nil
}

func (t *Task) newDBTaskPBs(dbBackups []*backuppb.DatabaseBackupInfo) (map[string]*backuppb.RestoreDatabaseTask, error) {
	dbNameDBTask := make(map[string]*backuppb.RestoreDatabaseTask, len(dbBackups))
	for _, dbBackup := range dbBackups {
		dbTask, err := t.newDBTaskPB(dbBackup)
		if err != nil {
			return nil, fmt.Errorf("restore: create database task %w", err)
		}
		dbNameDBTask[dbTask.GetTargetDbName()] = dbTask
	}

	return dbNameDBTask, nil
}

func (t *Task) newCollTaskPBs(collBackups []*backuppb.CollectionBackupInfo) (map[string]map[string]*backuppb.RestoreCollectionTask, error) {
	renamer, err := t.newRenamer()
	if err != nil {
		return nil, fmt.Errorf("restore: create renamer %w", err)
	}
	dbNameCollNameTask := make(map[string]map[string]*backuppb.RestoreCollectionTask)
	for _, collBackup := range collBackups {
		sourceNS := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())
		targetNS := renamer.rename(sourceNS)

		t.logger.Info("restore: rename collection",
			zap.String("source", sourceNS.String()),
			zap.String("target", targetNS.String()))
		collTask, err := t.newCollTaskPB(targetNS, collBackup)
		if err != nil {
			return nil, fmt.Errorf("restore: create collection task %w", err)
		}
		if _, ok := dbNameCollNameTask[collTask.GetTargetDbName()]; ok {
			dbNameCollNameTask[collTask.GetTargetDbName()][collTask.GetTargetCollectionName()] = collTask
		} else {
			dbNameCollNameTask[collTask.GetTargetDbName()] = make(map[string]*backuppb.RestoreCollectionTask)
			dbNameCollNameTask[collTask.GetTargetDbName()][collTask.GetTargetCollectionName()] = collTask
		}
	}

	return dbNameCollNameTask, nil
}

// checkCollsExist check if the collection exist in target milvus, if collection exist, return error.
func (t *Task) checkCollsExist(ctx context.Context, task *backuppb.RestoreBackupTask) error {
	for _, collTask := range task.GetCollectionRestoreTasks() {
		if err := t.checkCollExist(ctx, collTask); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) checkCollExist(ctx context.Context, task *backuppb.RestoreCollectionTask) error {
	has, err := t.grpc.HasCollection(ctx, task.GetTargetDbName(), task.GetTargetCollectionName())
	if err != nil {
		return fmt.Errorf("restore: check collection %w", err)
	}

	if task.SkipCreateCollection && task.DropExistCollection {
		return fmt.Errorf("restore: skip create and drop exist collection can not be true at the same time collection %s", task.GetTargetCollectionName())
	}

	// collection not exist and not create collection
	if !has && task.GetSkipCreateCollection() {
		return fmt.Errorf("restore: collction not exist, database %s collection %s", task.GetTargetDbName(), task.GetTargetCollectionName())
	}

	// collection existed and not drop collection
	if has && !task.GetSkipCreateCollection() && !task.GetDropExistCollection() {
		return fmt.Errorf("restore: collection already exist, database %s collection %s", task.GetTargetDbName(), task.GetTargetCollectionName())
	}

	return nil
}

func (t *Task) Prepare(_ context.Context) error {
	task, err := t.newRestoreTaskPB()
	if err != nil {
		return fmt.Errorf("restore: create restore task %w", err)
	}

	t.task = task

	t.taskMgr.AddRestoreTask(task.GetId(), task.GetToRestoreSize())
	opts := make([]taskmgr.RestoreTaskOpt, 0, len(task.GetCollectionRestoreTasks()))
	for _, collTask := range task.GetCollectionRestoreTasks() {
		targetNS := namespace.New(collTask.GetTargetDbName(), collTask.GetTargetCollectionName())
		opts = append(opts, taskmgr.AddRestoreCollTask(targetNS, collTask.GetToRestoreSize()))
	}
	t.taskMgr.UpdateRestoreTask(task.GetId(), opts...)

	return nil
}

func (t *Task) Execute(ctx context.Context) error {
	if err := t.privateExecute(ctx, t.task); err != nil {
		t.logger.Error("restore task failed", zap.Error(err))
		t.taskMgr.UpdateRestoreTask(t.task.GetId(), taskmgr.SetRestoreFail(err))
		return fmt.Errorf("restore: execute %w", err)
	}

	t.logger.Info("restore task finished")
	t.taskMgr.UpdateRestoreTask(t.task.GetId(), taskmgr.SetRestoreSuccess())
	return nil
}

func (t *Task) privateExecute(ctx context.Context, task *backuppb.RestoreBackupTask) error {
	if err := t.runRBACTask(ctx); err != nil {
		return err
	}

	t.taskMgr.UpdateRestoreTask(task.GetId(), taskmgr.SetRestoreExecuting())

	if err := t.runDBTask(ctx, task); err != nil {
		return fmt.Errorf("restore: run database task %w", err)
	}

	if err := t.prepareDB(ctx, task); err != nil {
		return fmt.Errorf("restore: prepare database %w", err)
	}

	if err := t.checkCollsExist(ctx, task); err != nil {
		return fmt.Errorf("restore: check collection exist %w", err)
	}

	if err := t.runCollTask(ctx, task); err != nil {
		return fmt.Errorf("restore: run collection task %w", err)
	}

	return nil
}

func (t *Task) runRBACTask(ctx context.Context) error {
	if !t.request.GetRbac() {
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

func (t *Task) runDBTask(ctx context.Context, task *backuppb.RestoreBackupTask) error {
	t.logger.Info("start restore database")

	// if the database is existed, skip restore
	dbs, err := t.grpc.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}
	t.logger.Debug("list databases", zap.Strings("databases", dbs))
	dbInTarget := lo.SliceToMap(dbs, func(db string) (string, struct{}) { return db, struct{}{} })

	for _, dbTask := range task.GetDatabaseRestoreTasks() {
		// We do not support db renaming yet.
		// The db in the source backup cannot be repeated,
		// so there is no need to deduplicate targetDBName.
		if _, ok := dbInTarget[dbTask.GetTargetDbName()]; ok {
			t.logger.Debug("skip restore database", zap.String("db_name", dbTask.GetTargetDbName()))
			continue
		}
		t.logger.Info("restore database", zap.String("source", dbTask.GetDbBackup().GetDbName()),
			zap.String("target", dbTask.GetTargetDbName()))
		dt := NewDatabaseTask(t.grpc, dbTask)
		if err := dt.Execute(ctx); err != nil {
			return fmt.Errorf("restore: restore database %w", err)
		}
		t.logger.Debug("finish restore database", zap.String("db_name", dbTask.GetTargetDbName()))
	}

	t.logger.Info("finish restore all database")

	return nil
}

// prepareDB create database if not exist, for restore collection task.
func (t *Task) prepareDB(ctx context.Context, task *backuppb.RestoreBackupTask) error {
	dbInTarget, err := t.grpc.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}

	dbs := make(map[string]struct{})
	for _, collTask := range task.GetCollectionRestoreTasks() {
		dbs[collTask.GetTargetDbName()] = struct{}{}
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

func (t *Task) runCollTask(ctx context.Context, task *backuppb.RestoreBackupTask) error {
	t.logger.Info("start restore collection")

	wp, err := common.NewWorkerPool(ctx, t.params.BackupCfg.RestoreParallelism, 0)
	if err != nil {
		return fmt.Errorf("restore: create collection worker pool %w", err)
	}
	wp.Start()
	t.logger.Info("Start collection level restore pool", zap.Int("parallelism", t.params.BackupCfg.RestoreParallelism))

	collTaskMetas := task.GetCollectionRestoreTasks()
	for _, collTaskMeta := range collTaskMetas {
		collTask := t.newRestoreCollTask(task.GetId(), collTaskMeta)
		job := func(ctx context.Context) error {
			logger := t.logger.With(zap.String("target_ns", collTask.targetNS.String()))
			if err := collTask.Execute(ctx); err != nil {
				logger.Error("restore coll failed", zap.Error(err))
				return fmt.Errorf("restore: restore collection %w", err)
			}

			logger.Info("finish restore collection", zap.Int64("size", collTaskMeta.RestoredSize))
			return nil
		}
		wp.Submit(job)
	}
	wp.Done()
	if err := wp.Wait(); err != nil {
		return fmt.Errorf("restore: wait collection worker pool %w", err)
	}

	duration := time.Now().Sub(time.Unix(task.GetStartTime(), 0))
	t.logger.Info("finish restore all collections",
		zap.Int("collection_num", len(t.backup.GetCollectionBackups())),
		zap.Duration("duration", duration))
	return nil
}

func (t *Task) newRestoreCollTask(taskID string, collTask *backuppb.RestoreCollectionTask) *CollectionTask {
	return newCollectionTask(collTask,
		t.params,
		taskID,
		t.backupBucketName,
		t.backupPath,
		t.backupStorage,
		t.milvusStorage,
		t.grpc,
		t.restful)
}
