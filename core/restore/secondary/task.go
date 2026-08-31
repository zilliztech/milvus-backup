package secondary

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/restore/conv"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
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

	Params *v2.Config

	BackupDir     string
	BackupStorage storage.Client

	// MilvusStorage is the target cluster's own object storage. When it is not
	// the same bucket/backend as BackupStorage, binlogs are staged into it
	// before the import message is broadcast, because DataCoord lists import
	// paths only from its own storage.
	MilvusStorage storage.Client

	TaskMgr *taskmgr.Mgr
}

type Task struct {
	args TaskArgs

	grpc    milvus.Grpc
	restful milvus.Restful

	streamCli milvus.Stream

	taskMgr *taskmgr.Mgr

	logger *zap.Logger
}

func NewTask(args TaskArgs) (*Task, error) {
	args.TaskMgr.AddRestoreTask(args.TaskID)

	return &Task{
		args: args,

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
	t.streamCli = milvus.NewStreamClient(t.args.SourceClusterID, t.args.TaskID, pchs, t.grpc)

	return nil
}

func (t *Task) closeClients() {
	if t.streamCli != nil {
		t.streamCli.Close()
	}
	if t.grpc != nil {
		if err := t.grpc.Close(); err != nil {
			t.logger.Warn("close grpc client", zap.Error(err))
		}
	}
}

func (t *Task) Execute(ctx context.Context) error {
	// Check before any client is created or any DDL is broadcast, so a backup
	// that cannot produce a matching secondary is rejected before it leaves a
	// half-created collection behind.
	if err := checkIndexExtra(t.args.Backup); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return err
	}

	defer t.closeClients()
	if err := t.initClients(); err != nil {
		return err
	}

	t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreExecuting())

	if err := t.checkBackupHasFullMeta(); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return err
	}

	if err := t.checkTargetNotRestored(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return err
	}

	if err := t.checkTargetIsUnused(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return err
	}

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

	if err := t.verifyRestored(ctx); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreFail(err))
		return err
	}

	t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreSuccess())
	t.logger.Info("restore done")
	return nil
}

// checkBackupHasFullMeta refuses to run against a backup whose cluster-level
// fields are missing.
//
// A secondary restore broadcasts DDL on the source's control channel and
// replays the source's flush-all messages per pchannel. Those fields are
// written only to meta/full_meta.json; the per-level meta files do not carry
// them, and meta.Read silently falls back to the per-level files when
// full_meta.json is absent. Without this check the restore reads the
// collection list fine and then fails at its first broadcast with
// "no pch in message", which points nowhere near the cause.
func (t *Task) checkBackupHasFullMeta() error {
	b := t.args.Backup
	var missing []string
	if b.GetControlChannelName() == "" {
		missing = append(missing, "control channel")
	}
	if len(b.GetPhysicalChannelNames()) == 0 {
		missing = append(missing, "pchannel list")
	}
	if len(b.GetFlushAllMsgsBase64()) == 0 {
		missing = append(missing, "flush-all messages")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("secondary: backup %q carries no %s. These are recorded only by a "+
		"backup that flushed the source, and only in <backup_dir>/meta/full_meta.json. "+
		"Either the backup was taken with --strategy=skip_flush or meta_only (the deprecated "+
		"--force/-f flag means skip_flush), which by design records no flush point, or "+
		"full_meta.json is missing where this restore reads the backup (check the copy if "+
		"the backup was moved between buckets). A plain restore accepts such a backup; a "+
		"secondary restore cannot, because it has no point in the source's stream to start "+
		"replication from. Take the backup again with the default strategy",
		b.GetName(), strings.Join(missing, ", "))
}

// checkTargetNotRestored refuses to restore into a target that has already been
// restored.
//
// The replicate checkpoint is what says so. A newly deployed secondary reports
// zero on every pchannel; a restore ends by forwarding the backup's flush-all
// messages, which carry the source's own time ticks and leave the checkpoint
// there. Nothing an operator does brings it back to zero -- re-applying the
// same replicate configuration does not, and neither does restarting the
// target or dropping its collections.
//
// This matters most for the case that is otherwise invisible. Dropping the
// target's collections hides them but keeps their ids reserved until the
// collections are reclaimed, and a replayed create for a reserved id is
// skipped without an error, so the restore would report success having created
// nothing. That target still carries the checkpoint of the restore that put
// the collections there, which is what this catches.
func (t *Task) checkTargetNotRestored(ctx context.Context) error {
	cfg, err := t.grpc.GetReplicateConfiguration(ctx)
	if err != nil {
		// A cluster with no replication cannot have been restored into as a
		// secondary, and the restore fails on its own if it is not one.
		t.logger.Info("cannot read the target's replicate configuration, skipping the check",
			zap.Error(err))
		return nil
	}

	var pchannels []string
	for _, cluster := range cfg.GetClusters() {
		if cluster.GetClusterId() == t.args.TargetClusterID {
			pchannels = cluster.GetPchannels()
			break
		}
	}
	if len(pchannels) == 0 {
		t.logger.Info("the target reports no pchannels of its own, skipping the check",
			zap.String("targetClusterID", t.args.TargetClusterID))
		return nil
	}

	for _, pchannel := range pchannels {
		resp, err := t.grpc.GetReplicateInfo(ctx, t.args.SourceClusterID, pchannel)
		if err != nil {
			return fmt.Errorf("secondary: read the replicate checkpoint of %s: %w", pchannel, err)
		}
		tick := resp.GetCheckpoint().GetTimeTick()
		if tick == 0 {
			continue
		}
		return fmt.Errorf("secondary: %s already has a replicate checkpoint (%s is at time tick "+
			"%d), so this target has been restored before. A secondary restore runs once, "+
			"against a newly deployed secondary: the messages it injects are time-ticked from "+
			"1, and a secondary only ever moves its checkpoint forward, so everything this "+
			"restore sends would be discarded. Nothing returns a used target to that state -- "+
			"re-applying the same replicate configuration does not clear the checkpoint, "+
			"restarting the target does not, and dropping its collections only hides them "+
			"while their ids stay reserved. To bootstrap again, deploy a new secondary and "+
			"restore into that",
			t.args.TargetClusterID, pchannel, tick)
	}

	t.logger.Info("the target has no replicate checkpoint, as a newly deployed secondary should",
		zap.Int("pchannels", len(pchannels)))
	return nil
}

// checkTargetIsUnused refuses to restore into a target that already holds one
// of the collections in the backup.
//
// A secondary restore replays the source's DDL verbatim, collection ids
// included, and it is meant to run once against a newly deployed secondary.
// Restoring on top of a collection that is already there does not replace it:
// the create is skipped and the data is imported into the existing collection,
// which silently doubles its rows.
func (t *Task) checkTargetIsUnused(ctx context.Context) error {
	var present []string
	// Databases the backup will create do not exist on the target yet; that is
	// the expected state for a newly deployed secondary, so it is reported once
	// per database and without the error object, which otherwise reads as a
	// failure. Any other error is a real inability to check and is warned.
	skipped := make(map[string]int)
	for _, coll := range t.args.Backup.GetCollectionBackups() {
		ns := fmt.Sprintf("%s.%s", coll.GetDbName(), coll.GetCollectionName())
		has, err := t.grpc.HasCollection(ctx, coll.GetDbName(), coll.GetCollectionName())
		if err != nil {
			if isDatabaseNotFound(err) {
				skipped[coll.GetDbName()]++
				continue
			}
			t.logger.Warn("cannot check whether the target already holds a collection, continuing",
				zap.String("ns", ns), zap.Error(err))
			continue
		}
		if has {
			present = append(present, ns)
		}
	}
	dbs := make([]string, 0, len(skipped))
	for db := range skipped {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	for _, db := range dbs {
		t.logger.Info("database does not exist on the target yet, as expected for a newly deployed secondary; skipping the duplicate-collection check for it",
			zap.String("db", db), zap.Int("collections", skipped[db]))
	}
	if len(present) == 0 {
		return nil
	}
	return fmt.Errorf("secondary: the target already holds %v, so it is not a newly "+
		"deployed secondary. A secondary restore replays the source's DDL with the "+
		"source's collection ids and is meant to run once: restoring on top of an "+
		"existing collection does not replace it, it imports into it and doubles its "+
		"rows. Dropping those collections does not make the target usable either, "+
		"because their ids stay reserved while they are reclaimed and a restore of the "+
		"same ids is then discarded without an error. To bootstrap again, deploy a new "+
		"secondary and restore into that", present)
}

// isDatabaseNotFound reports whether err is Milvus saying the database does not
// exist. The client surfaces it as a wrapped status error, so match the text.
func isDatabaseNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "database not found")
}

// verifyRestored confirms the collections actually exist on the target.
//
// Every step of this restore reports on whether the messages it sent were
// accepted, not on what the target did with them. The target can accept them
// and still create nothing -- a collection id left reserved by an earlier,
// reclaimed collection makes the replayed create a silent no-op -- so without
// this check the restore reports success against an empty target.
func (t *Task) verifyRestored(ctx context.Context) error {
	deadline := time.Now().Add(_restoreVerifyTimeout)
	var missing []string
	for {
		missing = missing[:0]
		for _, coll := range t.args.Backup.GetCollectionBackups() {
			has, err := t.grpc.HasCollection(ctx, coll.GetDbName(), coll.GetCollectionName())
			if err != nil {
				return fmt.Errorf("secondary: verify restored collection %s.%s: %w",
					coll.GetDbName(), coll.GetCollectionName(), err)
			}
			if !has {
				missing = append(missing, fmt.Sprintf("%s.%s",
					coll.GetDbName(), coll.GetCollectionName()))
			}
		}
		if len(missing) == 0 {
			t.logger.Info("all restored collections are present on the target")
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
		// The DDL is applied asynchronously on the target, so give it a moment
		// before concluding that it never arrived.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(_restoreVerifyInterval):
		}
	}
	return fmt.Errorf("secondary: the restore reported no errors, but %v %s not present on "+
		"the target after %s. The messages were accepted and nothing was created, which "+
		"happens when the target is not a newly deployed secondary: a collection id that "+
		"an earlier collection still holds while it is being reclaimed makes the replayed "+
		"create a silent no-op. Deploy a new secondary and restore into that",
		missing, plural(len(missing)), _restoreVerifyTimeout)
}

func plural(n int) string {
	if n == 1 {
		return "is"
	}
	return "are"
}

func (t *Task) runDBTasks(ctx context.Context) error {
	args := databaseTaskArgs{
		TaskID:     t.args.TaskID,
		BackupInfo: t.args.Backup,
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

	streaming := false
	if t.args.MilvusStorage != nil {
		streaming = storage.UseStreaming(t.args.Params.Transfer.Mode.Val,
			t.args.BackupStorage.Config(), t.args.MilvusStorage.Config())
	}

	return dmlTaskArgs{
		TaskID: t.args.TaskID,

		PchTS: pchTS,

		BackupStorage: t.args.BackupStorage,
		BackupDir:     t.args.BackupDir,

		MilvusStorage:  t.args.MilvusStorage,
		MilvusRootPath: t.args.Params.Milvus.Storage.RootPath.Val,
		Streaming:      streaming,
		CopySem:        semaphore.NewWeighted(int64(t.args.Params.Transfer.Concurrency.Val)),

		StreamCli:  t.streamCli,
		RestfulCli: t.restful,
	}, nil
}

func (t *Task) ddlTaskArgs() ddlTaskArgs {
	return ddlTaskArgs{
		TaskID:     t.args.TaskID,
		BackupInfo: t.args.Backup,
		StreamCli:  t.streamCli,
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

	if err := t.waitCollCreated(ctx, ns, collBackup.GetCollectionId()); err != nil {
		t.taskMgr.UpdateRestoreTask(t.args.TaskID, taskmgr.SetRestoreCollFail(ns, err))
		return err
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

// waitCollCreated blocks until the collection this restore just created is
// usable on the target, and fails the collection if it never becomes so.
//
// The DDL is not applied where it is sent. createColl returns once the create
// message is queued for the target's replicate stream, which says nothing about
// whether the target has accepted it, let alone applied it. Everything after it
// -- the import above all -- depends on the collection actually being there, and
// an import submitted against a collection the target does not have is accepted
// and then killed partway through, reported as the collection having been
// dropped. Waiting here turns that into a failure at the point of the cause.
func (t *Task) waitCollCreated(ctx context.Context, ns namespace.NS, collectionID int64) error {
	deadline := time.Now().Add(_collCreateTimeout)
	for {
		has, err := t.grpc.HasCollectionByID(ctx, collectionID)
		if err != nil {
			return fmt.Errorf("secondary: wait for collection %s (id %d): %w", ns, collectionID, err)
		}
		if has {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(_collCreateInterval):
		}
	}

	return fmt.Errorf("secondary: collection %s (id %d) is still not on the target %s after "+
		"its create was sent, so the create had no effect. A secondary restore replays the "+
		"source's create with the source's collection id, and the target keeps that id "+
		"reserved for as long as a collection that held it is being reclaimed -- dropping "+
		"the collections on the target does not release their ids, and a create of a "+
		"reserved id is discarded without an error. Restore into a newly deployed "+
		"secondary, and check with birdwatcher that none of the backup's collection ids "+
		"are present there in any state, dropped included",
		ns, collectionID, _collCreateTimeout)
}

func (t *Task) loadTaskArgs() loadTaskArgs {
	return loadTaskArgs{
		TaskID:     t.args.TaskID,
		BackupInfo: t.args.Backup,
		StreamCli:  t.streamCli,
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

	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(t.args.Params.Restore.Concurrency.Collections.Val)
	for _, coll := range t.args.Backup.GetCollectionBackups() {
		g.Go(func() error {
			return t.runCollTask(subCtx, dbNameBackup[coll.GetDbName()], coll, ddlArgs, dmlArgs, loadArgs)
		})
	}

	return g.Wait()
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

	err = t.streamCli.Send(ctx, func(uint64) []message.MutableMessage {
		broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
		return broadcast.SplitIntoMutableMessage()
	})
	if err != nil {
		return fmt.Errorf("secondary: send rbac msg: %w", err)
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

		if err := t.streamCli.Forward(ctx, &msg); err != nil {
			return fmt.Errorf("secondary: send flush all msg: %w", err)
		}
	}

	t.logger.Info("send flush all msg done")

	return nil
}
