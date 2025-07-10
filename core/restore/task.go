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
	"github.com/zilliztech/milvus-backup/core/meta/taskmgr"
	"github.com/zilliztech/milvus-backup/core/namespace"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type Task struct {
	logger *zap.Logger

	request *backuppb.RestoreBackupRequest
	backup  *backuppb.BackupInfo

	planner *planner

	params  *paramtable.BackupParams
	taskMgr *taskmgr.Mgr

	task *backuppb.RestoreBackupTask

	copySem       *semaphore.Weighted
	bulkInsertSem *semaphore.Weighted

	backupStorage storage.Client
	milvusStorage storage.Client

	grpc    milvus.Grpc
	restful milvus.Restful

	backupBucketName string
	backupPath       string
}

func NewTask(
	request *backuppb.RestoreBackupRequest,
	backupPath string,
	backupBucketName string,
	params *paramtable.BackupParams,
	info *backuppb.BackupInfo,
	backupStorage storage.Client,
	milvusStorage storage.Client,
	grpcCli milvus.Grpc,
	restfulCli milvus.Restful,
) (*Task, error) {
	logger := log.L().With(zap.String("backup_name", info.GetName()))
	p, err := newPlanner(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create planner %w", err)
	}

	return &Task{
		logger: logger,

		request: request,
		backup:  info,

		planner: p,

		params:  params,
		taskMgr: taskmgr.DefaultMgr,

		copySem:       semaphore.NewWeighted(params.BackupCfg.BackupCopyDataParallelism),
		bulkInsertSem: semaphore.NewWeighted(params.BackupCfg.ImportJobParallelism),

		backupStorage: backupStorage,
		milvusStorage: milvusStorage,

		grpc:    grpcCli,
		restful: restfulCli,

		backupBucketName: backupBucketName,
		backupPath:       backupPath,
	}, nil
}

func (t *Task) newRestoreTaskPB() (*backuppb.RestoreBackupTask, error) {
	dbTasks, collTasks := t.planner.Plan(t.backup)

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
		return fmt.Errorf("restore: collection not exist, database %s collection %s", task.GetTargetDbName(), task.GetTargetCollectionName())
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

	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(t.params.BackupCfg.RestoreParallelism)
	collTaskMetas := task.GetCollectionRestoreTasks()
	for _, collTaskMeta := range collTaskMetas {
		collTask := t.newRestoreCollTask(collTaskMeta)

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

	duration := time.Now().Sub(time.Unix(task.GetStartTime(), 0))
	t.logger.Info("finish restore all collections",
		zap.Int("collection_num", len(t.backup.GetCollectionBackups())),
		zap.Duration("duration", duration))
	return nil
}

func (t *Task) newRestoreCollTask(collTask *backuppb.RestoreCollectionTask) *CollectionTask {
	opt := collectionTaskOpt{
		task:          collTask,
		params:        t.params,
		parentTaskID:  t.task.GetId(),
		backupPath:    t.backupPath,
		backupStorage: t.backupStorage,
		milvusStorage: t.milvusStorage,
		copySem:       t.copySem,
		bulkInsertSem: t.bulkInsertSem,
		grpcCli:       t.grpc,
		restfulCli:    t.restful,
	}

	return newCollectionTask(opt)
}
