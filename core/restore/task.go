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

	params *paramtable.BackupParams
	meta   *meta.MetaManager

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
	meta *meta.MetaManager,
	backupStorage storage.ChunkManager,
	milvusStorage storage.ChunkManager,
	grpcCli client.Grpc,
	restfulCli client.Restful,
) *Task {
	logger := log.L().With(
		zap.String("backup_name", info.GetName()),
		zap.String("backup_path", backupPath),
		zap.String("backup_bucket_name", backupBucketName))

	return &Task{
		logger: logger,

		request: request,
		backup:  info,

		params: params,
		meta:   meta,

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

func (t *Task) filterDBBackup(dbCollections meta.DbCollections) ([]*backuppb.DatabaseBackupInfo, error) {
	bakDB := t.getDBNameDBBackup()

	needRestoreDBs := make([]*backuppb.DatabaseBackupInfo, 0, len(bakDB))
	for db, _ := range dbCollections {
		dbBackup, ok := bakDB[db]
		if !ok {
			return nil, fmt.Errorf("restore: database %s not exist in backup", db)
		}
		needRestoreDBs = append(needRestoreDBs, dbBackup)
	}

	return needRestoreDBs, nil
}

func (t *Task) filterCollBackup(dbCollections meta.DbCollections) ([]*backuppb.CollectionBackupInfo, error) {
	bakDBColl := t.getDBNameCollNameCollBackup()

	needRestoreColls := make([]*backuppb.CollectionBackupInfo, 0, len(bakDBColl))
	for db, colls := range dbCollections {
		collNameColl, ok := bakDBColl[db]
		if !ok {
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

	dbBackups, err := t.filterDBBackup(dbCollections)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: filter db backups by dbColletions %w", err)
	}

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

	dbBackups, err := t.filterDBBackup(dbCollections)
	if err != nil {
		return nil, nil, fmt.Errorf("restore: filter db backups by collection names %w", err)
	}

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

func (t *Task) newCollTaskPB(reNamer nsRenamer, bak *backuppb.CollectionBackupInfo) (*backuppb.RestoreCollectionTask, error) {
	if bak.GetCollectionName() == "" {
		return nil, fmt.Errorf("restore: collection name is empty")
	}

	sourceNS := namespace.New(bak.GetDbName(), bak.GetCollectionName())
	targetNS := reNamer.rename(sourceNS)
	t.logger.Debug("restore: rename collection", zap.String("source", sourceNS.String()), zap.String("target", targetNS.String()))

	size := lo.SumBy(bak.GetPartitionBackups(), func(partition *backuppb.PartitionBackupInfo) int64 { return partition.GetSize() })

	collTaskPB := &backuppb.RestoreCollectionTask{
		Id:                   uuid.NewString(),
		StateCode:            backuppb.RestoreTaskStateCode_INITIAL,
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

	dbTaskes := make([]*backuppb.RestoreDatabaseTask, 0, len(dbBackups))
	for _, dbBackup := range dbBackups {
		dbTask, err := t.newDBTaskPB(dbBackup)
		if err != nil {
			return nil, fmt.Errorf("restore: create database task %w", err)
		}
		dbTaskes = append(dbTaskes, dbTask)
	}

	renamer, err := t.newRenamer()
	if err != nil {
		return nil, fmt.Errorf("restore: create renamer %w", err)
	}
	collTasks := make([]*backuppb.RestoreCollectionTask, 0, len(collBackups))
	for _, collBackup := range collBackups {
		collTask, err := t.newCollTaskPB(renamer, collBackup)
		if err != nil {
			return nil, fmt.Errorf("restore: create collection task %w", err)
		}
		collTasks = append(collTasks, collTask)
	}

	size := lo.SumBy(collTasks, func(coll *backuppb.RestoreCollectionTask) int64 { return coll.ToRestoreSize })
	task := &backuppb.RestoreBackupTask{
		Id:                     t.request.GetId(),
		StateCode:              backuppb.RestoreTaskStateCode_INITIAL,
		StartTime:              time.Now().Unix(),
		ToRestoreSize:          size,
		DatabaseRestoreTasks:   dbTaskes,
		CollectionRestoreTasks: collTasks,
	}
	t.meta.AddRestoreTask(task)

	return task, nil
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
		return fmt.Errorf("restore: database %s collction %s not exist", task.GetTargetDbName(), task.GetTargetCollectionName())
	}

	// collection existed and not drop collection
	if has && !task.GetSkipCreateCollection() && !task.GetDropExistCollection() {
		return fmt.Errorf("restore: database %s collection %s already exist", task.GetTargetDbName(), task.GetTargetCollectionName())
	}

	return nil
}

func (t *Task) Execute(ctx context.Context) error {
	if err := t.privateExecute(ctx); err != nil {
		t.logger.Error("restore task failed", zap.Error(err))
		t.meta.UpdateRestoreTask(t.request.GetId(), meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_FAIL))
		return fmt.Errorf("restore: execute %w", err)
	}

	t.logger.Info("restore task finished")
	t.meta.UpdateRestoreTask(t.request.GetId(), meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_SUCCESS))
	return nil
}

func (t *Task) privateExecute(ctx context.Context) error {
	if err := t.runRBACTask(ctx); err != nil {
		return err
	}

	task, err := t.newRestoreTaskPB()
	if err != nil {
		return fmt.Errorf("restore: create restore task %w", err)
	}

	t.meta.UpdateRestoreTask(task.GetId(), meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_EXECUTING))

	if err := t.runDBTask(ctx, task); err != nil {
		return fmt.Errorf("restore: run database task %w", err)
	}

	if err := t.checkCollsExist(ctx, task); err != nil {
		return fmt.Errorf("restore: check collection exist %w", err)
	}

	if err := t.prepareDB(ctx, task); err != nil {
		return fmt.Errorf("restore: prepare database %w", err)
	}

	if err := t.runCollTask(ctx, task); err != nil {
		return fmt.Errorf("restore: run collection task %w", err)
	}

	return nil
}

func (t *Task) runRBACTask(ctx context.Context) error {
	if !t.request.GetRbac() {
		t.logger.Info("skip restore RBAC")
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
	t.logger.Info("start restore backup")

	wp, err := common.NewWorkerPool(ctx, t.params.BackupCfg.RestoreParallelism, 0)
	if err != nil {
		return fmt.Errorf("restore: create collection worker pool %w", err)
	}
	wp.Start()
	t.logger.Info("Start collection level restore pool", zap.Int("parallelism", t.params.BackupCfg.RestoreParallelism))

	id := task.GetId()
	t.meta.UpdateRestoreTask(id, meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_EXECUTING))

	collTaskMetas := task.GetCollectionRestoreTasks()
	for _, collTaskMeta := range collTaskMetas {
		collTask := t.newRestoreCollTask(task.GetId(), collTaskMeta)
		job := func(ctx context.Context) error {
			err := collTask.Execute(ctx)
			if err != nil {
				t.meta.UpdateRestoreTask(id, meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_FAIL),
					meta.SetRestoreErrorMessage(collTaskMeta.GetErrorMessage()))

				t.meta.UpdateRestoreCollectionTask(id, collTaskMeta.GetId(),
					meta.SetRestoreCollectionStateCode(backuppb.RestoreTaskStateCode_FAIL),
					meta.SetRestoreCollectionErrorMessage(collTaskMeta.GetErrorMessage()))

				t.logger.Error("restore coll failed",
					zap.String("target_db_name", collTaskMeta.GetTargetDbName()),
					zap.String("target_collection_name", collTaskMeta.GetTargetCollectionName()),
					zap.Error(err))
				return fmt.Errorf("restore: restore collection %w", err)
			}

			collTaskMeta.StateCode = backuppb.RestoreTaskStateCode_SUCCESS
			log.Info("finish restore collection",
				zap.String("target_db_name", collTaskMeta.GetTargetDbName()),
				zap.String("target_collection_name", collTaskMeta.GetTargetCollectionName()),
				zap.Int64("size", collTaskMeta.RestoredSize))
			return nil
		}
		wp.Submit(job)
	}
	wp.Done()
	if err := wp.Wait(); err != nil {
		return fmt.Errorf("restore: wait collection worker pool %w", err)
	}

	endTime := time.Now().Unix()
	task.EndTime = endTime

	duration := time.Unix(endTime, 0).Sub(time.Unix(task.GetStartTime(), 0))
	t.logger.Info("finish restore all collections",
		zap.String("backup_name", t.backup.GetName()),
		zap.Int("collection_num", len(t.backup.GetCollectionBackups())),
		zap.String("task_id", task.GetId()),
		zap.Duration("duration", duration))
	return nil
}

func (t *Task) newRestoreCollTask(taskID string, collTask *backuppb.RestoreCollectionTask) *CollectionTask {
	return newCollectionTask(collTask,
		t.meta,
		t.params,
		taskID,
		t.backupBucketName,
		t.backupPath,
		t.backupStorage,
		t.milvusStorage,
		t.grpc,
		t.restful)
}
