package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const (
	_rpcChWarnMessage = "Failed to back up RPC channel position. This won't cause the backup to fail, " +
		"but may lead to inconsistency when reconnecting to CDC for incremental data replication."
)

type TaskArgs struct {
	TaskID string

	Option Option

	MilvusStorage storage.Client

	BackupStorage storage.Client
	BackupDir     string

	Params *cfg.Config

	TaskMgr *taskmgr.Mgr
}

type Option struct {
	BackupName string

	PauseGC    bool
	ManageAddr string

	Strategy Strategy

	BackupRBAC bool

	BackupIndexExtra bool

	// dbName -> CollFilter
	Filter filter.Filter
}

type Task struct {
	taskID string

	logger *zap.Logger

	option Option
	params *cfg.Config

	milvusStorage  storage.Client
	milvusRootPath string

	crossStorage bool

	backupStorage storage.Client
	backupDir     string

	throttling concurrencyThrottling

	grpc    milvus.Grpc
	restful milvus.Restful
	manage  milvus.Manage

	etcdCli      *clientv3.Client
	etcdRootPath string

	metaBuilder *metaBuilder

	gcCtrl gcCtrl

	taskMgr *taskmgr.Mgr

	rpcChannelName string
}

func newGCCtrl(taskID string, pauseGC bool, grpc milvus.Grpc, manage milvus.Manage) gcCtrl {
	if !pauseGC {
		return &emptyGCCtrl{}
	}

	if grpc.HasFeature(milvus.CollectionLevelGCControl) {
		return newCollGCCtrl(taskID, manage)
	}

	return newClusterGCCtrl(taskID, manage)
}

func NewTask(args TaskArgs) (*Task, error) {
	logger := log.L().With(zap.String("task_id", args.TaskID))

	crossStorage := args.Params.Minio.CrossStorage.Val
	if args.BackupStorage.Config().Provider != args.MilvusStorage.Config().Provider {
		crossStorage = true
	}

	mb := newMetaBuilder(args.TaskID, args.Option.BackupName)
	err := args.TaskMgr.AddBackupTask(args.TaskID, args.Option.BackupName)
	if err != nil {
		return nil, fmt.Errorf("backup: add backup task to manager: %w", err)
	}

	throttling := concurrencyThrottling{
		CollSem: semaphore.NewWeighted(int64(args.Params.Backup.Parallelism.BackupCollection.Val)),
		SegSem:  semaphore.NewWeighted(int64(args.Params.Backup.Parallelism.BackupSegment.Val)),
		CopySem: semaphore.NewWeighted(int64(args.Params.Backup.Parallelism.CopyData.Val)),
	}

	return &Task{
		taskID: args.TaskID,

		logger: logger,

		option: args.Option,

		params: args.Params,

		milvusStorage:  args.MilvusStorage,
		milvusRootPath: args.Params.Minio.RootPath.Val,

		crossStorage: crossStorage,

		backupStorage: args.BackupStorage,
		backupDir:     args.BackupDir,

		throttling: throttling,

		etcdRootPath: args.Params.Milvus.Etcd.RootPath.Val,

		metaBuilder: mb,

		taskMgr: args.TaskMgr,

		rpcChannelName: args.Params.Milvus.RPCChannelName.Val,
	}, nil
}

func (t *Task) initClients() error {
	grpcCli, err := milvus.NewGrpc(&t.params.Milvus)
	if err != nil {
		return fmt.Errorf("backup: create grpc client: %w", err)
	}
	t.grpc = grpcCli

	restfulCli, err := milvus.NewRestful(&t.params.Milvus)
	if err != nil {
		return fmt.Errorf("backup: create restful client: %w", err)
	}
	t.restful = restfulCli

	manageAddr := t.params.Backup.GCPause.Address.Val
	if t.option.ManageAddr != "" {
		manageAddr = t.option.ManageAddr
	}
	t.manage = milvus.NewManage(manageAddr)

	if t.option.BackupIndexExtra {
		endpoints := strings.Split(t.params.Milvus.Etcd.Endpoints.Val, ",")
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return fmt.Errorf("backup: create etcd client: %w", err)
		}
		t.etcdCli = etcdCli
	}

	t.gcCtrl = newGCCtrl(t.taskID, t.option.PauseGC, t.grpc, t.manage)

	return nil
}

func (t *Task) closeClients() {
	if t.grpc != nil {
		if err := t.grpc.Close(); err != nil {
			t.logger.Warn("close grpc client", zap.Error(err))
		}
	}
	if t.etcdCli != nil {
		if err := t.etcdCli.Close(); err != nil {
			t.logger.Warn("close etcd client", zap.Error(err))
		}
	}
}

func (t *Task) Execute(ctx context.Context) error {
	if err := t.initClients(); err != nil {
		return err
	}
	defer t.closeClients()

	if err := t.prepare(ctx); err != nil {
		return err
	}

	if err := t.privateExecute(ctx); err != nil {
		t.taskMgr.UpdateBackupTask(t.taskID, taskmgr.SetBackupFail(err))
		return err
	}

	t.taskMgr.UpdateBackupTask(t.taskID, taskmgr.SetBackupSuccess())
	return nil
}

func (t *Task) privateExecute(ctx context.Context) error {
	version, err := t.grpc.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("backup: get milvus version: %w", err)
	}
	t.metaBuilder.setVersion(version)

	t.gcCtrl.PauseGC(ctx)
	defer t.gcCtrl.ResumeGC(ctx)

	dbNames, collections, err := t.listDBAndNSS(ctx)
	if err != nil {
		return fmt.Errorf("backup: list db and collection: %w", err)
	}

	if err := t.backupDatabase(ctx, dbNames); err != nil {
		return fmt.Errorf("backup: run db task: %w", err)
	}

	if err := t.backupCollection(ctx, collections); err != nil {
		return fmt.Errorf("backup: run collection task: %w", err)
	}

	if err := t.backupRBAC(ctx); err != nil {
		return fmt.Errorf("backup: run rbac task: %w", err)
	}

	t.backupRPCChannelPOS(ctx)

	if err := t.backupIndexExtraInfo(ctx); err != nil {
		return fmt.Errorf("backup: run index extra info task: %w", err)
	}

	if err := t.writeMeta(ctx); err != nil {
		return fmt.Errorf("backup: write meta: %w", err)
	}

	t.logger.Info("backup successfully")
	return nil
}

func (t *Task) prepare(ctx context.Context) error {
	exist, err := storage.Exist(ctx, t.backupStorage, t.backupDir)
	if err != nil {
		return fmt.Errorf("backup: check whether exist backup with name: %s", t.option.BackupName)
	}
	if exist {
		return fmt.Errorf("backup: backup with name %s already exist", t.option.BackupName)
	}

	return nil
}

func (t *Task) listDBAndNSS(ctx context.Context) ([]string, []namespace.NS, error) {
	f := t.option.Filter

	if f.DBCollFilter == nil {
		return t.listAllDBAndNSS(ctx)
	}

	return t.listFilteredDBAndNSS(ctx, f)
}

func (t *Task) listAllDBAndNSS(ctx context.Context) ([]string, []namespace.NS, error) {
	var dbNames []string
	var err error

	if t.grpc.HasFeature(milvus.MultiDatabase) {
		dbNames, err = t.grpc.ListDatabases(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("backup: list databases: %w", err)
		}
	} else {
		dbNames = []string{namespace.DefaultDBName}
	}

	var nss []namespace.NS
	for _, dbName := range dbNames {
		resp, err := t.grpc.ListCollections(ctx, dbName)
		if err != nil {
			return nil, nil, fmt.Errorf("backup: list collections for db %s: %w", dbName, err)
		}
		for _, coll := range resp.CollectionNames {
			nss = append(nss, namespace.New(dbName, coll))
		}
	}

	t.logger.Info("listed all collections",
		zap.Strings("db", dbNames),
		zap.Strings("ns", nsStrings(nss)))

	return dbNames, nss, nil
}

func (t *Task) listFilteredDBAndNSS(ctx context.Context, f filter.Filter) ([]string, []namespace.NS, error) {
	// With filter: process each filter entry:
	// - db.*: list all collections in the db
	// - db.coll1,db.coll2: list collections with existence check
	// - db.: database only (no collections)
	dbNameSet := make(map[string]struct{}, len(f.DBCollFilter))
	var nss []namespace.NS

	for dbName, collFilter := range f.DBCollFilter {
		dbNameSet[dbName] = struct{}{}

		if collFilter.AllowAll {
			resp, err := t.grpc.ListCollections(ctx, dbName)
			if err != nil {
				return nil, nil, fmt.Errorf("backup: list collections for db %s: %w", dbName, err)
			}
			for _, coll := range resp.CollectionNames {
				nss = append(nss, namespace.New(dbName, coll))
			}
			continue
		}

		for collName := range collFilter.CollName {
			exists, err := t.grpc.HasCollection(ctx, dbName, collName)
			if err != nil {
				return nil, nil, fmt.Errorf("backup: check collection %s.%s: %w", dbName, collName, err)
			}
			if !exists {
				return nil, nil, fmt.Errorf("backup: filter collection %s.%s not found in milvus", dbName, collName)
			}
			nss = append(nss, namespace.New(dbName, collName))
		}
	}

	dbNames := make([]string, 0, len(dbNameSet))
	for dbName := range dbNameSet {
		dbNames = append(dbNames, dbName)
	}

	t.logger.Info("listed collections (filtered)",
		zap.Strings("db", dbNames),
		zap.Strings("ns", nsStrings(nss)))

	return dbNames, nss, nil
}

func nsStrings(nss []namespace.NS) []string {
	out := make([]string, 0, len(nss))
	for _, ns := range nss {
		out = append(out, ns.String())
	}
	return out
}

func (t *Task) backupDatabase(ctx context.Context, dbNames []string) error {
	t.logger.Info("start backup databases", zap.Int("count", len(dbNames)))
	t.taskMgr.UpdateBackupTask(t.taskID, taskmgr.SetBackupDatabaseExecuting())

	for _, dbName := range dbNames {
		dbTask := newDatabaseTask(t.taskID, dbName, t.grpc, t.manage, t.metaBuilder)
		if err := dbTask.Execute(ctx); err != nil {
			return fmt.Errorf("backup: execute db task %s: %w", dbName, err)
		}
	}

	t.logger.Info("backup db done")

	return nil
}

func (t *Task) newCollTaskArgs() collTaskArgs {
	return collTaskArgs{
		TaskID:         t.taskID,
		MilvusStorage:  t.milvusStorage,
		MilvusRootPath: t.milvusRootPath,
		CrossStorage:   t.crossStorage,
		BackupStorage:  t.backupStorage,
		BackupDir:      t.backupDir,
		Throttling:     t.throttling,
		MetaBuilder:    t.metaBuilder,
		TaskMgr:        t.taskMgr,
		Grpc:           t.grpc,
		Restful:        t.restful,
		gcCtrl:         t.gcCtrl,
	}
}

func (t *Task) backupCollection(ctx context.Context, nss []namespace.NS) error {
	t.logger.Info("start backup collections", zap.Int("count", len(nss)))

	t.taskMgr.UpdateBackupTask(t.taskID, taskmgr.AddBackupCollTasks(nss))
	t.taskMgr.UpdateBackupTask(t.taskID, taskmgr.SetBackupCollectionExecuting())

	strategy, err := t.selectStrategy(nss)
	if err != nil {
		return fmt.Errorf("backup: select strategy: %w", err)
	}
	if err := strategy.Execute(ctx); err != nil {
		return fmt.Errorf("backup: execute collection strategy: %w", err)
	}

	t.logger.Info("backup all collections successfully")

	return nil
}

func (t *Task) selectStrategy(nss []namespace.NS) (tasklet.Tasklet, error) {
	args := t.newCollTaskArgs()

	switch t.option.Strategy {
	case StrategyAuto:
		if t.grpc.HasFeature(milvus.FlushAll) {
			t.logger.Info("use bulk flush strategy")
			return newBulkFlushStrategy(nss, args), nil
		}
		t.logger.Info("use serial flush strategy")
		return newSerialFlushStrategy(nss, args), nil
	case StrategyMetaOnly:
		t.logger.Info("use meta only strategy")
		return newMetaOnlyStrategy(nss, t.newCollTaskArgs()), nil
	case StrategySkipFlush:
		t.logger.Info("use skip flush strategy")
		return newSkipFlushStrategy(nss, t.newCollTaskArgs()), nil
	case StrategyBulkFlush:
		t.logger.Info("use bulk flush strategy")
		return newBulkFlushStrategy(nss, t.newCollTaskArgs()), nil
	case StrategySerialFlush:
		t.logger.Info("use serial flush strategy")
		return newSerialFlushStrategy(nss, t.newCollTaskArgs()), nil
	default:
		return nil, fmt.Errorf("backup: unsupported strategy: %s", t.option.Strategy)
	}
}

func (t *Task) backupRBAC(ctx context.Context) error {
	if !t.option.BackupRBAC {
		t.logger.Info("skip backup rbac")
		return nil
	}

	rt := NewRBACTask(t.taskID, t.metaBuilder, t.grpc)
	if err := rt.Execute(ctx); err != nil {
		return fmt.Errorf("backup: execute rbac task: %w", err)
	}

	return nil
}

// backupRPCChannelPOS backs up the RPC channel position for CDC incremental replication.
// This is only needed for Milvus 2.5.x CDC, which is no longer maintained.
// Consider removing this entirely in the future.
func (t *Task) backupRPCChannelPOS(ctx context.Context) {
	if !t.grpc.HasFeature(milvus.ReplicateMessage) {
		t.logger.Info("skip backup rpc channel pos, replicate message not supported in this version")
		return
	}
	t.logger.Info("start backup rpc channel pos")
	rpcPosTask := newRPCChannelPOSTask(t.taskID, t.rpcChannelName, t.grpc, t.metaBuilder)
	if err := rpcPosTask.Execute(ctx); err != nil {
		t.logger.Warn(_rpcChWarnMessage, zap.Error(err))
		return
	}
	t.logger.Info("backup rpc channel pos done")
}

func (t *Task) backupIndexExtraInfo(ctx context.Context) error {
	if !t.option.BackupIndexExtra {
		t.logger.Info("skip backup index extra info")
		return nil
	}

	if t.etcdCli == nil {
		return errors.New("backup: need backup etcd info but etcd client is nil")
	}

	t.logger.Info("start backup index extra info")

	indexExtraTask := newCollIndexExtraTask(t.taskID, t.etcdCli, t.etcdRootPath, t.metaBuilder)
	if err := indexExtraTask.Execute(ctx); err != nil {
		return fmt.Errorf("backup: execute index extra task: %w", err)
	}

	return nil
}

func (t *Task) writeMeta(ctx context.Context) error {
	t.logger.Info("start write meta")

	type metaEntry struct {
		Type mpath.MetaType
		Fn   func() ([]byte, error)
	}

	entries := []metaEntry{
		{Type: mpath.BackupMeta, Fn: t.metaBuilder.buildBackupMeta},
		{Type: mpath.CollectionMeta, Fn: t.metaBuilder.buildCollectionMeta},
		{Type: mpath.PartitionMeta, Fn: t.metaBuilder.buildPartitionMeta},
		{Type: mpath.SegmentMeta, Fn: t.metaBuilder.buildSegmentMeta},
		{Type: mpath.FullMeta, Fn: t.metaBuilder.buildFullMeta},
	}

	for _, entry := range entries {
		data, err := entry.Fn()
		if err != nil {
			return fmt.Errorf("backup: build %s meta: %w", entry.Type, err)
		}
		err = storage.Write(ctx, t.backupStorage, mpath.MetaKey(t.backupDir, entry.Type), data)
		if err != nil {
			return fmt.Errorf("backup: write %s meta: %w", entry.Type, err)
		}
	}

	t.logger.Info("finish write meta")
	return nil
}
