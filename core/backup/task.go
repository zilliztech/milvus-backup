package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

const (
	_rpcChWarnMessage = "Failed to back up RPC channel position. This won't cause the backup to fail, " +
		"but may lead to inconsistency when reconnecting to CDC for incremental data replication."
)

type TaskArgs struct {
	TaskID string

	MilvusStorage storage.Client

	BackupStorage storage.Client
	BackupDir     string

	Request *backuppb.CreateBackupRequest
	Params  *paramtable.BackupParams

	Grpc    milvus.Grpc
	Restful milvus.Restful
	Manage  milvus.Manage

	Meta *meta.MetaManager
}

type Task struct {
	taskID string

	logger *zap.Logger

	milvusStorage  storage.Client
	milvusRootPath string

	backupStorage storage.Client
	backupDir     string

	collSem *semaphore.Weighted

	crossStorage bool
	copySem      *semaphore.Weighted

	grpc    milvus.Grpc
	restful milvus.Restful
	manage  milvus.Manage

	gcCtrl  *gcController
	pauseGC bool

	meta *meta.MetaManager

	rpcChannelName string

	request *backuppb.CreateBackupRequest
}

func NewTask(args TaskArgs) (*Task, error) {
	logger := log.L().With(zap.String("task_id", args.TaskID))

	crossStorage := args.Params.MinioCfg.CrossStorage
	if args.BackupStorage.Config().Provider != args.MilvusStorage.Config().Provider {
		crossStorage = true
	}

	pauseGC := args.Request.GetGcPauseEnable() || args.Params.BackupCfg.GcPauseEnable

	return &Task{
		taskID: args.TaskID,

		logger: logger,

		milvusStorage:  args.MilvusStorage,
		milvusRootPath: args.Params.MinioCfg.RootPath,

		backupStorage: args.BackupStorage,
		backupDir:     args.BackupDir,

		collSem: semaphore.NewWeighted(int64(args.Params.BackupCfg.BackupCollectionParallelism)),

		crossStorage: crossStorage,
		copySem:      semaphore.NewWeighted(args.Params.BackupCfg.BackupCopyDataParallelism),

		grpc:    args.Grpc,
		restful: args.Restful,

		gcCtrl:  newGCController(args.Manage),
		pauseGC: pauseGC,

		meta: args.Meta,

		rpcChannelName: args.Params.MilvusCfg.RPCChanelName,

		request: args.Request,
	}, nil
}

func (t *Task) Execute(ctx context.Context) error {
	if err := t.privateExecute(ctx); err != nil {
		return err
	}

	return nil
}

func (t *Task) privateExecute(ctx context.Context) error {
	if t.pauseGC {
		t.gcCtrl.Pause(ctx)
		defer t.gcCtrl.Resume(ctx)
	}

	dbNames, collections, err := t.listDBAndCollection(ctx)
	if err != nil {
		return fmt.Errorf("backup: list db and collection: %w", err)
	}

	if err := t.runDBTask(ctx, dbNames); err != nil {
		return fmt.Errorf("backup: run db task: %w", err)
	}

	if err := t.runCollTask(ctx, collections); err != nil {
		return fmt.Errorf("backup: run collection task: %w", err)
	}

	if err := t.runRBACTask(ctx); err != nil {
		return fmt.Errorf("backup: run rbac task: %w", err)
	}

	t.runRPCChannelPOSTask(ctx)

	if err := t.writeMeta(ctx); err != nil {
		return fmt.Errorf("backup: write meta: %w", err)
	}

	t.logger.Info("backup successfully")
	return nil
}

func (t *Task) listDBAndCollectionFromDBCollections(ctx context.Context, dbCollectionsStr string) ([]string, []namespace.NS, error) {
	var dbCollections meta.DbCollections
	if err := json.Unmarshal([]byte(dbCollectionsStr), &dbCollections); err != nil {
		return nil, nil, fmt.Errorf("backup: unmarshal dbCollections: %w", err)
	}

	var dbNames []string
	var nss []namespace.NS
	for db, colls := range dbCollections {
		if db == "" {
			db = namespace.DefaultDBName
		}
		dbNames = append(dbNames, db)

		// if collections is empty, list all collections in the database
		if len(colls) == 0 {
			resp, err := t.grpc.ListCollections(ctx, db)
			if err != nil {
				return nil, nil, fmt.Errorf("backup: list collection for db %s: %w", db, err)
			}
			colls = resp.CollectionNames
		}

		for _, coll := range colls {
			ns := namespace.New(db, coll)
			exist, err := t.grpc.HasCollection(ctx, ns.DBName(), ns.CollName())
			if err != nil {
				return nil, nil, fmt.Errorf("backup: check collection %s: %w", ns, err)
			}
			if !exist {
				return nil, nil, fmt.Errorf("backup: collection %s does not exist", ns)
			}
			t.logger.Debug("need backup collection", zap.String("db", db), zap.String("collection", coll))
			nss = append(nss, ns)
		}
	}

	return dbNames, nss, nil
}

func (t *Task) listDBAndCollectionFromCollectionNames(ctx context.Context, collectionNames []string) ([]string, []namespace.NS, error) {
	dbNames := make(map[string]struct{})
	nss := make([]namespace.NS, 0, len(collectionNames))
	for _, collectionName := range collectionNames {
		dbName := namespace.DefaultDBName
		if strings.Contains(collectionName, ".") {
			splits := strings.Split(collectionName, ".")
			dbName = splits[0]
			collectionName = splits[1]
		}

		dbNames[dbName] = struct{}{}
		exist, err := t.grpc.HasCollection(ctx, dbName, collectionName)
		if err != nil {
			return nil, nil, fmt.Errorf("backup: check collection %s.%s: %w", dbName, collectionName, err)
		}
		if !exist {
			return nil, nil, fmt.Errorf("backup: collection %s.%s does not exist", dbName, collectionName)
		}
		nss = append(nss, namespace.New(dbName, collectionName))
	}

	return maps.Keys(dbNames), nss, nil
}

func (t *Task) listDBAndCollectionFromAPI(ctx context.Context) ([]string, []namespace.NS, error) {
	var dbNames []string
	var nss []namespace.NS
	// compatible to milvus under v2.2.8 without database support
	if t.grpc.HasFeature(milvus.MultiDatabase) {
		t.logger.Info("the milvus server support multi database")
		dbs, err := t.grpc.ListDatabases(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("backup: list databases: %w", err)
		}
		for _, db := range dbs {
			t.logger.Debug("need backup db", zap.String("db", db))
			dbNames = append(dbNames, db)
			resp, err := t.grpc.ListCollections(ctx, db)
			if err != nil {
				return nil, nil, fmt.Errorf("backup: list collections for db %s: %w", db, err)
			}
			for _, coll := range resp.CollectionNames {
				t.logger.Debug("need backup collection", zap.String("db", db), zap.String("collection", coll))
				nss = append(nss, namespace.New(db, coll))
			}
		}
	} else {
		t.logger.Info("the milvus server does not support multi database")
		dbNames = append(dbNames, namespace.DefaultDBName)
		resp, err := t.grpc.ListCollections(ctx, namespace.DefaultDBName)
		if err != nil {
			return nil, nil, fmt.Errorf("backup: list collections for db %s: %w", namespace.DefaultDBName, err)
		}
		for _, coll := range resp.CollectionNames {
			t.logger.Debug("need backup collection", zap.String("db", namespace.DefaultDBName), zap.String("collection", coll))
			nss = append(nss, namespace.New(namespace.DefaultDBName, coll))
		}
	}

	return dbNames, nss, nil
}

// listDBAndCollection lists the database and collection names to be backed up.
// 1. Use dbCollection in the request if it is not empty.
// 2. If dbCollection is empty, use collectionNames in the request.
// 3. If collectionNames is empty, list all databases and collections.
func (t *Task) listDBAndCollection(ctx context.Context) ([]string, []namespace.NS, error) {
	dbCollectionsStr := utils.GetDBCollections(t.request.GetDbCollections())
	t.logger.Debug("get dbCollections from request", zap.String("dbCollections", dbCollectionsStr))

	// 1. dbCollections
	if dbCollectionsStr != "" {
		t.logger.Info("read need backup db and collection from dbCollection", zap.String("dbCollections", dbCollectionsStr))
		dbNames, nss, err := t.listDBAndCollectionFromDBCollections(ctx, dbCollectionsStr)
		if err != nil {
			return nil, nil, fmt.Errorf("backup: list db and collection from dbCollections: %w", err)
		}
		t.logger.Info("list db and collection from dbCollections done", zap.Strings("dbNames", dbNames), zap.Any("nss", nss))
		return dbNames, nss, nil
	}

	// 2. collectionNames
	if len(t.request.GetCollectionNames()) > 0 {
		t.logger.Info("read need backup db and collection from collectionNames", zap.Strings("collectionNames", t.request.GetCollectionNames()))
		dbNames, nss, err := t.listDBAndCollectionFromCollectionNames(ctx, t.request.GetCollectionNames())
		if err != nil {
			return nil, nil, fmt.Errorf("backup: list db and collection from collectionNames: %w", err)
		}
		t.logger.Info("list db and collection from collectionNames done", zap.Strings("dbNames", dbNames), zap.Any("nss", nss))
		return dbNames, nss, nil
	}

	// 3. list all databases and collections
	t.logger.Info("read need backup db and collection from API")
	dbNames, nss, err := t.listDBAndCollectionFromAPI(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("backup: list db and collection from API: %w", err)
	}
	t.logger.Info("list db and collection from API done", zap.Strings("dbNames", dbNames), zap.Any("nss", nss))

	return dbNames, nss, nil
}

func (t *Task) runDBTask(ctx context.Context, dbNames []string) error {
	for _, dbName := range dbNames {
		dbTask := NewDatabaseTask(t.taskID, dbName, t.grpc, t.meta)
		if err := dbTask.Execute(ctx); err != nil {
			return fmt.Errorf("backup: execute db task %s: %w", dbName, err)
		}
	}

	t.logger.Info("backup db done")

	return nil
}

func (t *Task) newCollectionTaskArgs() collectionTaskArgs {
	return collectionTaskArgs{
		TaskID:         t.request.GetRequestId(),
		MetaOnly:       t.request.GetMetaOnly(),
		SkipFlush:      t.request.GetForce(),
		MilvusStorage:  t.milvusStorage,
		MilvusRootPath: t.milvusRootPath,
		CrossStorage:   t.crossStorage,
		BackupStorage:  t.backupStorage,
		CopySem:        t.copySem,
		BackupDir:      t.backupDir,
		Meta:           t.meta,
		Grpc:           t.grpc,
		Restful:        t.restful,
	}
}

func (t *Task) runCollTask(ctx context.Context, nss []namespace.NS) error {
	nsStrs := lo.Map(nss, func(coll namespace.NS, _ int) string { return coll.String() })
	t.logger.Info("start backup all collections", zap.Int("count", len(nss)), zap.Strings("ns", nsStrs))
	args := t.newCollectionTaskArgs()

	g, subCtx := errgroup.WithContext(ctx)
	for _, ns := range nss {
		if err := t.collSem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("backup: acquire collection semaphore: %w", err)
		}

		g.Go(func() error {
			defer t.collSem.Release(1)

			task := NewCollectionTask(ns, args)
			if err := task.Execute(subCtx); err != nil {
				return fmt.Errorf("create: backup collection %s, err: %w", ns, err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("backup: wait worker pool: %w", err)
	}

	t.logger.Info("backup all collections successfully")

	return nil
}

func (t *Task) runRBACTask(ctx context.Context) error {
	if !t.request.GetRbac() {
		t.logger.Info("skip backup rbac")
		return nil
	}

	rt := NewRBACTask(t.taskID, t.meta, t.grpc)
	if err := rt.Execute(ctx); err != nil {
		return fmt.Errorf("backup: execute rbac task: %w", err)
	}

	return nil
}

func (t *Task) runRPCChannelPOSTask(ctx context.Context) {
	t.logger.Info("start backup rpc channel pos")
	rpcPosTask := newRPCChannelPOSTask(t.taskID, t.rpcChannelName, t.grpc, t.meta)
	if err := rpcPosTask.Execute(ctx); err != nil {
		t.logger.Warn(_rpcChWarnMessage, zap.Error(err))
		return
	}
	t.logger.Info("backup rpc channel pos done")
}

func (t *Task) writeMeta(ctx context.Context) error {
	backupInfo := t.meta.GetFullMeta(t.taskID)
	output, err := meta.Serialize(backupInfo)
	if err != nil {
		return err
	}

	collectionBackups := backupInfo.GetCollectionBackups()
	collectionPositions := make(map[string][]*backuppb.ChannelPosition)
	for _, collectionBackup := range collectionBackups {
		collectionCPs := make([]*backuppb.ChannelPosition, 0, len(collectionBackup.GetChannelCheckpoints()))
		for vCh, position := range collectionBackup.GetChannelCheckpoints() {
			collectionCPs = append(collectionCPs, &backuppb.ChannelPosition{
				Name:     vCh,
				Position: position,
			})
		}
		collectionPositions[collectionBackup.GetCollectionName()] = collectionCPs
	}
	channelCPsBytes, err := json.Marshal(collectionPositions)
	if err != nil {
		return err
	}

	err = storage.Write(ctx, t.backupStorage, mpath.MetaKey(t.backupDir, mpath.BackupMeta), output.BackupMetaBytes)
	if err != nil {
		return fmt.Errorf("backup: write backup meta: %w", err)
	}
	err = storage.Write(ctx, t.backupStorage, mpath.MetaKey(t.backupDir, mpath.CollectionMeta), output.CollectionMetaBytes)
	if err != nil {
		return fmt.Errorf("backup: write collection meta: %w", err)
	}
	err = storage.Write(ctx, t.backupStorage, mpath.MetaKey(t.backupDir, mpath.PartitionMeta), output.PartitionMetaBytes)
	if err != nil {
		return fmt.Errorf("backup: write partition meta: %w", err)
	}
	err = storage.Write(ctx, t.backupStorage, mpath.MetaKey(t.backupDir, mpath.SegmentMeta), output.SegmentMetaBytes)
	if err != nil {
		return fmt.Errorf("backup: write segment meta: %w", err)
	}
	err = storage.Write(ctx, t.backupStorage, mpath.MetaKey(t.backupDir, mpath.ChannelCPMeta), channelCPsBytes)
	if err != nil {
		return err
	}

	err = storage.Write(ctx, t.backupStorage, mpath.MetaKey(t.backupDir, mpath.FullMeta), output.FullMetaBytes)
	if err != nil {
		return fmt.Errorf("backup: write full meta: %w", err)
	}

	log.Info("finish writeBackupInfoMeta")
	return nil
}
