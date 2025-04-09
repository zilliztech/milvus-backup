package restore

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type Task struct {
	logger *zap.Logger

	task *backuppb.RestoreBackupTask
	info *backuppb.BackupInfo

	params *paramtable.BackupParams
	meta   *meta.MetaManager

	backupStorage storage.ChunkManager
	milvusStorage storage.ChunkManager

	grpcCli    client.Grpc
	restfulCli client.RestfulBulkInsert

	backupBucketName string
	backupPath       string
}

func NewTask(task *backuppb.RestoreBackupTask,
	backupPath string,
	backupBucketName string,
	params *paramtable.BackupParams,
	info *backuppb.BackupInfo,
	meta *meta.MetaManager,
	backupStorage storage.ChunkManager,
	milvusStorage storage.ChunkManager,
	grpcCli client.Grpc,
	restfulCli client.RestfulBulkInsert,
) *Task {
	logger := log.L().With(
		zap.String("backup_name", info.GetName()),
		zap.String("backup_path", backupPath),
		zap.String("backup_bucket_name", backupBucketName))

	return &Task{
		logger: logger,

		task: task,
		info: info,

		params: params,
		meta:   meta,

		backupStorage: backupStorage,
		milvusStorage: milvusStorage,

		grpcCli:    grpcCli,
		restfulCli: restfulCli,

		backupBucketName: backupBucketName,
		backupPath:       backupPath,
	}
}

// prepareDB create database if not exist
func (t *Task) prepareDB(ctx context.Context) error {
	dbInTarget, err := t.grpcCli.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("restore: list databases %w", err)
	}

	dbs := make(map[string]struct{})
	for _, collTask := range t.task.GetCollectionRestoreTasks() {
		dbs[collTask.GetTargetDbName()] = struct{}{}
	}
	dbsNeedToRestores := maps.Keys(dbs)

	dbNotInTarget := lo.Without(dbsNeedToRestores, dbInTarget...)
	for _, db := range dbNotInTarget {
		if err := t.grpcCli.CreateDatabase(ctx, db); err != nil {
			return fmt.Errorf("restore: create database %w", err)
		}
		t.logger.Info("create db done", zap.String("database", db))
	}

	return nil
}

// checkCollsExist check if the collection exist in target milvus, if collection exist, return error.
func (t *Task) checkCollsExist(ctx context.Context) error {
	for _, collTask := range t.task.GetCollectionRestoreTasks() {
		if err := t.checkCollExist(ctx, collTask); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) checkCollExist(ctx context.Context, task *backuppb.RestoreCollectionTask) error {
	has, err := t.grpcCli.HasCollection(ctx, task.GetTargetDbName(), task.GetTargetCollectionName())
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
	if err := t.prepareDB(ctx); err != nil {
		return err
	}

	if err := t.checkCollsExist(ctx); err != nil {
		return err
	}

	if err := t.runCollTask(ctx); err != nil {
		return err
	}

	return nil
}

func (t *Task) runCollTask(ctx context.Context) error {
	t.logger.Info("start restore backup")

	wp, err := common.NewWorkerPool(ctx, t.params.BackupCfg.RestoreParallelism, 0)
	if err != nil {
		return fmt.Errorf("restore: create collection worker pool %w", err)
	}
	wp.Start()
	t.logger.Info("Start collection level restore pool", zap.Int("parallelism", t.params.BackupCfg.RestoreParallelism))

	id := t.task.GetId()
	t.meta.UpdateRestoreTask(id, meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_EXECUTING))

	collTaskMetas := t.task.GetCollectionRestoreTasks()
	for _, collTaskMeta := range collTaskMetas {
		collTask := t.newRestoreCollTask(collTaskMeta)
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
	t.task.EndTime = endTime
	t.meta.UpdateRestoreTask(id, meta.SetRestoreStateCode(backuppb.RestoreTaskStateCode_SUCCESS), meta.SetRestoreEndTime(endTime))

	duration := time.Unix(endTime, 0).Sub(time.Unix(t.task.GetStartTime(), 0))
	t.logger.Info("finish restore all collections",
		zap.String("backup_name", t.info.GetName()),
		zap.Int("collection_num", len(t.info.GetCollectionBackups())),
		zap.String("task_id", t.task.GetId()),
		zap.Duration("duration", duration))
	return nil
}

func (t *Task) newRestoreCollTask(collTask *backuppb.RestoreCollectionTask) *CollectionTask {
	return newCollectionTask(collTask,
		t.meta,
		t.params,
		t.task.GetId(),
		t.backupBucketName,
		t.backupPath,
		t.backupStorage,
		t.milvusStorage,
		t.grpcCli,
		t.restfulCli)
}
