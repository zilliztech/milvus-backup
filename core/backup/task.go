package backup

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/tasklet"
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

	Params *paramtable.BackupParams

	Grpc    milvus.Grpc
	Restful milvus.Restful
	Manage  milvus.Manage

	EtcdCli *clientv3.Client

	TaskMgr *taskmgr.Mgr
}

type Option struct {
	BackupName string

	PauseGC bool

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

func newGCCtrl(args TaskArgs) gcCtrl {
	if !args.Option.PauseGC {
		return &emptyGCCtrl{}
	}

	if args.Grpc.HasFeature(milvus.CollectionLevelGCControl) {
		return newCollectionGCCtrl(args.TaskID, args.Manage)
	}

	return newClusterGCCtrl(args.TaskID, args.Manage)
}

func NewTask(args TaskArgs) *Task {
	logger := log.L().With(zap.String("task_id", args.TaskID))

	crossStorage := args.Params.MinioCfg.CrossStorage
	if args.BackupStorage.Config().Provider != args.MilvusStorage.Config().Provider {
		crossStorage = true
	}

	mb := newMetaBuilder(args.TaskID, args.Option.BackupName)
	args.TaskMgr.AddBackupTask(args.TaskID, args.Option.BackupName)

	throttling := concurrencyThrottling{
		CollSem: semaphore.NewWeighted(args.Params.BackupCfg.BackupCollectionParallelism),
		SegSem:  semaphore.NewWeighted(args.Params.BackupCfg.BackupSegmentParallelism),
		CopySem: semaphore.NewWeighted(args.Params.BackupCfg.BackupCopyDataParallelism),
	}

	return &Task{
		taskID: args.TaskID,

		logger: logger,

		option: args.Option,

		milvusStorage:  args.MilvusStorage,
		milvusRootPath: args.Params.MinioCfg.RootPath,

		crossStorage: crossStorage,

		backupStorage: args.BackupStorage,
		backupDir:     args.BackupDir,

		throttling: throttling,

		grpc:    args.Grpc,
		restful: args.Restful,
		manage:  args.Manage,

		gcCtrl: newGCCtrl(args),

		etcdCli:      args.EtcdCli,
		etcdRootPath: args.Params.MilvusCfg.EtcdConfig.RootPath,

		metaBuilder: mb,

		taskMgr: args.TaskMgr,

		rpcChannelName: args.Params.MilvusCfg.RPCChanelName,
	}
}

func (t *Task) Execute(ctx context.Context) error {
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

func (t *Task) listNS(ctx context.Context, db string) ([]namespace.NS, error) {
	var nss []namespace.NS
	resp, err := t.grpc.ListCollections(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("backup: list collections for db %s: %w", db, err)
	}
	for _, coll := range resp.CollectionNames {
		nss = append(nss, namespace.New(db, coll))
	}

	return nss, nil
}

func (t *Task) listAllDBAndNSS(ctx context.Context) ([]string, []namespace.NS, error) {
	// if milvus support multi database, list all databases and collections
	if t.grpc.HasFeature(milvus.MultiDatabase) {
		t.logger.Info("the milvus server support multi database")
		dbNames, err := t.grpc.ListDatabases(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("backup: list databases: %w", err)
		}

		var nss []namespace.NS
		for _, dbName := range dbNames {
			dbNss, err := t.listNS(ctx, dbName)
			if err != nil {
				return nil, nil, fmt.Errorf("backup: list collections for db %s: %w", dbName, err)
			}
			nss = append(nss, dbNss...)
		}

		return dbNames, nss, nil
	} else {
		dbNames := []string{namespace.DefaultDBName}
		var nss []namespace.NS

		defaultDBNss, err := t.listNS(ctx, namespace.DefaultDBName)
		if err != nil {
			return nil, nil, err
		}
		nss = append(nss, defaultDBNss...)

		return dbNames, nss, nil
	}
}

func (t *Task) filterDBAndNSS(dbNames []string, nss []namespace.NS) ([]string, []namespace.NS, error) {
	filteredDBNames := t.option.Filter.AllowDBs(dbNames)
	filteredNSS := t.option.Filter.AllowNSS(nss)

	// if the filter have some db not in milvus, return error
	dbNameSet := lo.SliceToMap(dbNames, func(item string) (string, struct{}) { return item, struct{}{} })
	for dbName := range t.option.Filter.DBCollFilter {
		if _, ok := dbNameSet[dbName]; !ok {
			return nil, nil, fmt.Errorf("backup: filter db %s not found in milvus", dbName)
		}
	}

	// if the filter have some collection not in milvus, return error
	nsSet := lo.SliceToMap(nss, func(item namespace.NS) (namespace.NS, struct{}) { return item, struct{}{} })
	for dbName, collFilter := range t.option.Filter.DBCollFilter {
		if collFilter.AllowAll {
			continue
		}

		for collName := range collFilter.CollName {
			ns := namespace.New(dbName, collName)
			if _, ok := nsSet[ns]; !ok {
				return nil, nil, fmt.Errorf("backup: filter collection %s not found in milvus", collName)
			}
		}
	}

	return filteredDBNames, filteredNSS, nil
}

func (t *Task) listDBAndNSS(ctx context.Context) ([]string, []namespace.NS, error) {
	dbNames, nss, err := t.listAllDBAndNSS(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("backup: list all db and ns: %w", err)
	}
	t.logger.Info("all db and collections in milvus",
		zap.Strings("db", dbNames),
		zap.Strings("ns", lo.Map(nss, func(ns namespace.NS, _ int) string { return ns.String() })))

	dbNames, nss, err = t.filterDBAndNSS(dbNames, nss)
	if err != nil {
		return nil, nil, fmt.Errorf("backup: filter db and ns: %w", err)
	}
	t.logger.Info("db and collections need to backup",
		zap.Strings("db", dbNames),
		zap.Strings("ns", lo.Map(nss, func(ns namespace.NS, _ int) string { return ns.String() })))

	return dbNames, nss, nil
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

func (t *Task) newCollTaskArgs() collectionTaskArgs {
	return collectionTaskArgs{
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

func (t *Task) backupRPCChannelPOS(ctx context.Context) {
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

	indexExtraTask := newCollectionIndexExtraTask(t.taskID, t.etcdCli, t.etcdRootPath, t.metaBuilder)
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
