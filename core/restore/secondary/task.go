package secondary

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand/v2"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/restore/conv"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

type TaskArgs struct {
	TaskID string

	SourceClusterID string
	TargetClusterID string

	Backup *backuppb.BackupInfo

	Params *cfg.Config

	BackupDir     string
	BackupStorage storage.Client

	TaskMgr *taskmgr.Mgr
}

type Task struct {
	args TaskArgs

	grpc    milvus.Grpc
	restful milvus.Restful

	tsAlloc *tsAlloc

	streamCli milvus.Stream

	taskMgr *taskmgr.Mgr

	logger *zap.Logger
}

func NewTask(args TaskArgs) (*Task, error) {
	args.TaskMgr.AddRestoreTask(args.TaskID)

	return &Task{
		args: args,

		tsAlloc: newTTAlloc(),

		taskMgr: args.TaskMgr,

		logger: log.With(zap.String("task_id", args.TaskID)),
	}, nil
}

func (t *Task) initClients() error {
	grpcCli, err := milvus.NewGrpc(&t.args.Params.Milvus)
	if err != nil {
		return fmt.Errorf("secondary: create grpc client: %w", err)
	}
	t.grpc = grpcCli

	restfulCli, err := milvus.NewRestful(&t.args.Params.Milvus)
	if err != nil {
		return fmt.Errorf("secondary: create restful client: %w", err)
	}
	t.restful = restfulCli

	pchs := t.args.Backup.GetPhysicalChannelNames()
	streamCli, err := milvus.NewStreamClient(t.args.SourceClusterID, t.args.TaskID, pchs, t.grpc)
	if err != nil {
		return fmt.Errorf("secondary: create stream client: %w", err)
	}
	t.streamCli = streamCli

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
	if err := t.initClients(); err != nil {
		return err
	}
	defer t.closeClients()

	if err := t.runDBTasks(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("secondary: run database tasks: %w", err)
	}

	if err := t.runCollTasks(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("secondary: run collection tasks: %w", err)
	}

	if err := t.sendRBACMsg(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("secondary: send rbac msg: %w", err)
	}

	if err := t.sendFlushAll(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return fmt.Errorf("secondary: send flush all: %w", err)
	}

	t.logger.Info("wait confirm")
	t.streamCli.WaitConfirm()

	t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreSuccess())
	t.logger.Info("restore done")
	return nil
}

func (t *Task) runDBTasks(ctx context.Context) error {
	args := databaseTaskArgs{
		TaskID:     t.args.TaskID,
		BackupInfo: t.args.Backup,
		TSAlloc:    t.tsAlloc,
		StreamCli:  t.streamCli,
	}

	for _, db := range t.args.Backup.GetDatabaseBackups() {
		task, err := newDatabaseTask(args, db)
		if err != nil {
			return fmt.Errorf("secondary: create database task: %w", err)
		}

		if err := task.Execute(ctx); err != nil {
			return fmt.Errorf("secondary: execute database task: %w", err)
		}
	}

	return nil
}

func (t *Task) dmlTaskArgs() (dmlTaskArgs, error) {
	pchTS := make(map[string]uint64, len(t.args.Backup.GetFlushAllMsgsBase64()))
	for pch, msgBase64 := range t.args.Backup.GetFlushAllMsgsBase64() {
		msyBytes, err := base64.StdEncoding.DecodeString(msgBase64)
		if err != nil {
			return dmlTaskArgs{}, fmt.Errorf("secondary: decode flush all msg: %w", err)
		}

		var msg commonpb.ImmutableMessage
		if err := proto.Unmarshal(msyBytes, &msg); err != nil {
			return dmlTaskArgs{}, fmt.Errorf("secondary: unmarshal flush all msg: %w", err)
		}

		ts, err := milvus.GetTT(&msg)
		if err != nil {
			return dmlTaskArgs{}, fmt.Errorf("secondary: get tt from flush all msg: %w", err)
		}

		pchTS[pch] = ts
	}

	return dmlTaskArgs{
		TaskID: t.args.TaskID,

		TSAlloc: t.tsAlloc,

		PchTS: pchTS,

		BackupStorage: t.args.BackupStorage,
		BackupDir:     t.args.BackupDir,

		StreamCli:  t.streamCli,
		RestfulCli: t.restful,
	}, nil
}

func (t *Task) ddlTaskArgs() ddlTaskArgs {
	return ddlTaskArgs{
		TaskID:     t.args.TaskID,
		BackupInfo: t.args.Backup,
		StreamCli:  t.streamCli,
		TSAlloc:    t.tsAlloc,
	}
}

func (t *Task) runCollTask(ctx context.Context, dbBackup *backuppb.DatabaseBackupInfo, collBackup *backuppb.CollectionBackupInfo, ddlArgs ddlTaskArgs, dmlArgs dmlTaskArgs, loadArgs loadTaskArgs) error {
	ns := namespace.New(dbBackup.GetDbName(), collBackup.GetCollectionName())
	t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.AddRestoreCollTask(ns, collBackup.GetSize()))
	t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollExecuting(ns))

	ddlTask := newCollDDLTask(ddlArgs, dbBackup, collBackup)
	if err := ddlTask.Execute(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollFail(ns, err))
		return fmt.Errorf("secondary: execute collection ddl task: %w", err)
	}

	dmlTask := newCollDMLTask(dmlArgs, collBackup)
	if err := dmlTask.Execute(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollFail(ns, err))
		return fmt.Errorf("secondary: execute collection dml task: %w", err)
	}

	loadTask := newCollLoadTask(loadArgs, dbBackup, collBackup)
	if err := loadTask.Execute(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollFail(ns, err))
		return fmt.Errorf("secondary: execute collection load task: %w", err)
	}

	t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollSuccess(ns))

	return nil
}

func (t *Task) loadTaskArgs() loadTaskArgs {
	return loadTaskArgs{
		TaskID:     t.args.TaskID,
		BackupInfo: t.args.Backup,
		StreamCli:  t.streamCli,
		TSAlloc:    t.tsAlloc,
	}
}

func (t *Task) runCollTasks(ctx context.Context) error {
	dbNameBackup := make(map[string]*backuppb.DatabaseBackupInfo, len(t.args.Backup.GetDatabaseBackups()))
	for _, db := range t.args.Backup.GetDatabaseBackups() {
		dbNameBackup[db.GetDbName()] = db
	}

	dmlArgs, err := t.dmlTaskArgs()
	if err != nil {
		return fmt.Errorf("secondary: get dml task args: %w", err)
	}
	ddlArgs := t.ddlTaskArgs()
	loadArgs := t.loadTaskArgs()
	for _, coll := range t.args.Backup.GetCollectionBackups() {
		if err := t.runCollTask(ctx, dbNameBackup[coll.GetDbName()], coll, ddlArgs, dmlArgs, loadArgs); err != nil {
			return fmt.Errorf("secondary: run collection task: %w", err)
		}
	}

	return nil
}

func (t *Task) sendRBACMsg(ctx context.Context) error {
	t.logger.Info("send rbac msg")
	curRBAC, err := t.grpc.BackupRBAC(context.Background())
	if err != nil {
		return fmt.Errorf("secondary: get current rbac: %w", err)
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

	builder := message.NewRestoreRBACMessageBuilderV2().
		WithHeader(&message.RestoreRBACMessageHeader{}).
		WithBody(&message.RestoreRBACMessageBody{RbacMeta: rbacMeta}).
		WithBroadcast([]string{t.args.Backup.GetControlChannelName()})

	broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
	msgs := broadcast.SplitIntoMutableMessage()
	for _, msg := range msgs {
		ts := t.tsAlloc.Alloc()
		immutableMessage := msg.WithTimeTick(ts).
			WithLastConfirmed(newFakeMessageID(ts)).
			IntoImmutableMessage(newFakeMessageID(ts)).
			IntoImmutableMessageProto()

		if err := t.streamCli.Send(ctx, immutableMessage); err != nil {
			return fmt.Errorf("secondary: send rbac msg: %w", err)
		}
	}

	return nil
}

func (t *Task) sendFlushAll(ctx context.Context) error {
	t.logger.Info("send flush all msg")

	for _, msgBase64 := range t.args.Backup.GetFlushAllMsgsBase64() {
		msyBytes, err := base64.StdEncoding.DecodeString(msgBase64)
		if err != nil {
			return fmt.Errorf("secondary: decode flush all msg: %w", err)
		}

		var msg commonpb.ImmutableMessage
		if err := proto.Unmarshal(msyBytes, &msg); err != nil {
			return fmt.Errorf("secondary: unmarshal flush all msg: %w", err)
		}

		if err := t.streamCli.Send(ctx, &msg); err != nil {
			return fmt.Errorf("secondary: send flush all msg: %w", err)
		}
	}

	t.logger.Info("send flush all msg done")

	return nil
}
