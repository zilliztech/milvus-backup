package secondary

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/namespace"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

type TaskArgs struct {
	TaskID string

	SourceClusterID string
	TargetClusterID string

	Backup *backuppb.BackupInfo

	Params *paramtable.BackupParams

	BackupDir     string
	BackupStorage storage.Client

	Grpc milvus.Grpc
}

type Task struct {
	args TaskArgs

	streamCli milvus.Stream

	taskMgr *taskmgr.Mgr

	logger *zap.Logger
}

func NewTask(args TaskArgs) (*Task, error) {
	streamCli, err := args.Grpc.CreateReplicateStream(args.SourceClusterID)
	if err != nil {
		return nil, fmt.Errorf("create replicate stream: %w", err)
	}

	return &Task{
		args: args,

		streamCli: milvus.NewStreamClient(args.SourceClusterID, streamCli),

		taskMgr: taskmgr.DefaultMgr,

		logger: log.With(zap.String("task_id", args.TaskID)),
	}, nil
}

func (t *Task) Execute(ctx context.Context) error {
	t.taskMgr.AddRestoreTask(t.args.TaskID)

	if err := t.runDBTasks(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("secondary: run database tasks: %w", err)
	}

	if err := t.runCollTasks(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("secondary: run collection tasks: %w", err)
	}

	t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreSuccess())
	return nil
}

func (t *Task) runDBTasks(ctx context.Context) error {
	for _, db := range t.args.Backup.GetDatabaseBackups() {
		task, err := newDatabaseTask(t.args.TaskID, t.args.Backup, db, t.streamCli)
		if err != nil {
			return fmt.Errorf("secondary: create database task: %w", err)
		}

		if err := task.Execute(ctx); err != nil {
			return fmt.Errorf("secondary: execute database task: %w", err)
		}
	}

	return nil
}

func (t *Task) runCollTasks(ctx context.Context) error {
	dbIDBackup := make(map[int64]*backuppb.DatabaseBackupInfo)
	for _, db := range t.args.Backup.GetDatabaseBackups() {
		dbIDBackup[db.GetDbId()] = db
	}

	for _, coll := range t.args.Backup.GetCollectionBackups() {
		dbBackup := dbIDBackup[coll.GetDbId()]

		ns := namespace.New(dbBackup.GetDbName(), coll.GetCollectionName())
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.AddRestoreCollTask(ns, coll.GetSize()))
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollExecuting(ns))

		task := newCollectionDDLTask(t.args.TaskID, t.args.Backup, dbBackup, coll, t.streamCli)

		if err := task.Execute(ctx); err != nil {
			t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollFail(ns, err))
			return fmt.Errorf("secondary: execute collection ddl task: %w", err)
		}

		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollSuccess(ns))
	}
	return nil
}
