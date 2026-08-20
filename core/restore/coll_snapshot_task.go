package restore

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const _snapshotPollInterval = 3 * time.Second

type collSnapshotTask struct {
	taskID string

	collBackup *backuppb.CollectionBackupInfo
	targetNS   namespace.NS

	source       snapshotSource
	dropExist    bool
	maxShardNum  int32
	descOverride string
	skipParams   SkipParams

	pollInterval time.Duration

	grpcCli milvus.Grpc
	taskMgr *taskmgr.Mgr

	logger *zap.Logger
}

type collSnapshotTaskArgs struct {
	taskID string

	collBackup *backuppb.CollectionBackupInfo
	targetNS   namespace.NS

	source       snapshotSource
	dropExist    bool
	maxShardNum  int32
	descOverride string
	skipParams   SkipParams

	grpcCli milvus.Grpc
	taskMgr *taskmgr.Mgr
}

func newCollSnapshotTask(args collSnapshotTaskArgs) *collSnapshotTask {
	srcNS := namespace.New(args.collBackup.GetDbName(), args.collBackup.GetCollectionName())

	logger := log.With(
		zap.String("restore_task_id", args.taskID),
		zap.String("backup_ns", srcNS.String()),
		zap.String("target_ns", args.targetNS.String()))

	// The export job reported this as the size of the whole bundle, index files included,
	// so it is what the restore job's progress is a fraction of.
	args.taskMgr.UpdateRestoreTask(args.taskID, taskmgr.AddRestoreCollTask(args.targetNS, args.collBackup.GetSize()))

	return &collSnapshotTask{
		taskID: args.taskID,

		collBackup: args.collBackup,
		targetNS:   args.targetNS,

		source:       args.source,
		dropExist:    args.dropExist,
		maxShardNum:  args.maxShardNum,
		descOverride: args.descOverride,
		skipParams:   args.skipParams,

		pollInterval: _snapshotPollInterval,

		grpcCli: args.grpcCli,
		taskMgr: args.taskMgr,

		logger: logger,
	}
}

func (ct *collSnapshotTask) TargetNS() namespace.NS { return ct.targetNS }

// Execute hands the collection to Milvus, which creates it from the schema in the bundle,
// restores its indexes and partitions, and copies the data. No bytes move through this
// process, and nothing here creates anything in the target cluster.
func (ct *collSnapshotTask) Execute(ctx context.Context) error {
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollExecuting(ct.targetNS))

	if err := ct.privateExecute(ctx); err != nil {
		ct.logger.Error("restore collection from snapshot failed", zap.Error(err))
		ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollFail(ct.targetNS, err))
		return err
	}

	ct.logger.Info("restore collection from snapshot success")
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.SetRestoreCollSuccess(ct.targetNS))

	return nil
}

func (ct *collSnapshotTask) privateExecute(ctx context.Context) error {
	ct.logger.Info("start restore collection from snapshot")

	// Milvus creates the collection from the schema in the bundle, so the shard count
	// cannot be capped here. max_shard_num is only a bound: it changes nothing while it
	// is not smaller than the bundle's shard count, so only a request that would actually
	// cap shards is refused.
	if ct.maxShardNum > 0 {
		if shardNum := ct.collBackup.GetShardsNum(); shardNum > ct.maxShardNum {
			return fmt.Errorf("restore: collection has %d shards, exceeding max_shard_num=%d; the snapshot path cannot cap the shard count",
				shardNum, ct.maxShardNum)
		}
		ct.logger.Info("max_shard_num does not bind, keeping the bundle's shard count",
			zap.Int32("shard_num", ct.collBackup.GetShardsNum()),
			zap.Int32("max_shard_num", ct.maxShardNum))
	}

	metadataURI, err := ct.source.metadataURI(ct.collBackup.GetSnapshotBackup().GetMetadataPath())
	if err != nil {
		return err
	}

	if err := ct.dropExistedColl(ctx); err != nil {
		return err
	}

	jobID, err := ct.grpcCli.RestoreExternalSnapshot(ctx, milvus.RestoreExternalSnapshotInput{
		DB:                   ct.targetNS.DBName(),
		TargetCollectionName: ct.targetNS.CollName(),
		SnapshotMetadataURI:  metadataURI,
		ExternalSpec:         ct.source.externalSpec,
	})
	if err != nil {
		return fmt.Errorf("restore: restore external snapshot: %w", err)
	}
	ct.logger.Info("snapshot restore job accepted",
		zap.Int64("job_id", jobID),
		zap.String("metadata_uri", metadataURI))

	if err := ct.waitRestore(ctx, jobID); err != nil {
		return err
	}

	if err := ct.applySkipParams(ctx); err != nil {
		return err
	}

	return ct.applyDescOverride(ctx)
}

// applySkipParams drops the params the caller asked to skip from the restored collection.
// Milvus creates the collection from the bundle's schema, so the source cluster's overrides
// (mmap.enabled, for example) come along with it; the binlog path would simply not write
// them. Here they are removed after the restore with delete_keys, which drops the override
// and lets each key fall back to the target cluster's own default.
//
// What gets deleted is filtered against the schema saved in the backup meta: a key the
// backed-up object did not carry is not sent to the server at all. That also makes a skip
// list naming absent keys — the common case — a quiet no-op rather than an error.
func (ct *collSnapshotTask) applySkipParams(ctx context.Context) error {
	props := append(ct.collBackup.GetSchema().GetProperties(), ct.collBackup.GetProperties()...)
	if present := presentKeys(props, ct.skipParams.CollectionProperties); len(present) != 0 {
		if err := ct.grpcCli.DropCollectionProperties(ctx, ct.targetNS.DBName(), ct.targetNS.CollName(), present); err != nil {
			return fmt.Errorf("restore: drop skipped collection properties: %w", err)
		}
		ct.logger.Info("skipped collection properties dropped", zap.Strings("keys", present))
	}

	for _, field := range ct.collBackup.GetSchema().GetFields() {
		present := presentKeys(field.GetTypeParams(), ct.skipParams.FieldTypeParams)
		if len(present) == 0 {
			continue
		}
		if err := ct.grpcCli.DropCollectionFieldProperties(ctx, ct.targetNS.DBName(), ct.targetNS.CollName(), field.GetName(), present); err != nil {
			return fmt.Errorf("restore: drop skipped type params of field %s: %w", field.GetName(), err)
		}
		ct.logger.Info("skipped field type params dropped",
			zap.String("field", field.GetName()), zap.Strings("keys", present))
	}

	// A param the caller wants off an index can be recorded either as an index param or as
	// a field index param; both live on the index once Milvus restores the bundle, so the
	// two skip lists are dropped together.
	indexKeys := lo.Union(ct.skipParams.IndexParams, ct.skipParams.FieldIndexParams)
	for _, index := range ct.collBackup.GetIndexInfos() {
		present := lo.Intersect(indexKeys, lo.Keys(index.GetParams()))
		if len(present) == 0 {
			continue
		}
		sort.Strings(present)
		if err := ct.grpcCli.DropIndexProperties(ctx, ct.targetNS.DBName(), ct.targetNS.CollName(), index.GetIndexName(), present); err != nil {
			return fmt.Errorf("restore: drop skipped params of index %s: %w", index.GetIndexName(), err)
		}
		ct.logger.Info("skipped index params dropped",
			zap.String("index", index.GetIndexName()), zap.Strings("keys", present))
	}

	return nil
}

// presentKeys returns the subset of keys that actually appear in kvs, so a delete_keys call
// only names overrides the restored object really carries.
func presentKeys[K interface{ GetKey() string }](kvs []K, keys []string) []string {
	present := make([]string, 0, len(keys))
	for _, kv := range kvs {
		if lo.Contains(keys, kv.GetKey()) {
			present = append(present, kv.GetKey())
		}
	}
	return present
}

// applyDescOverride rewrites the collection description after the restore completes. Milvus
// creates the collection from the bundle's schema, so the only way to change the description
// is an AlterCollection once the collection exists.
func (ct *collSnapshotTask) applyDescOverride(ctx context.Context) error {
	if ct.descOverride == "" {
		return nil
	}

	props := []*commonpb.KeyValuePair{{Key: common.CollectionDescription, Value: ct.descOverride}}
	if err := ct.grpcCli.AlterCollection(ctx, ct.targetNS.DBName(), ct.targetNS.CollName(), props); err != nil {
		return fmt.Errorf("restore: alter collection description: %w", err)
	}

	ct.logger.Info("collection description overridden",
		zap.String("target_ns", ct.targetNS.String()),
		zap.String("description", ct.descOverride))
	return nil
}

// dropExistedColl removes the target ahead of the restore. Milvus refuses to restore into a
// collection that already exists, because it creates the collection itself.
func (ct *collSnapshotTask) dropExistedColl(ctx context.Context) error {
	if !ct.dropExist {
		return nil
	}

	exist, err := ct.grpcCli.HasCollection(ctx, ct.targetNS.DBName(), ct.targetNS.CollName())
	if err != nil {
		return fmt.Errorf("restore: check collection exist: %w", err)
	}
	if !exist {
		return nil
	}

	ct.logger.Info("drop existed collection")
	if err := ct.grpcCli.DropCollection(ctx, ct.targetNS.DBName(), ct.targetNS.CollName()); err != nil {
		return fmt.Errorf("restore: drop existed collection: %w", err)
	}

	return nil
}

// waitRestore polls the restore job until it is done. A job that failed comes back through
// the info rather than as an error from the state query, so both are checked.
func (ct *collSnapshotTask) waitRestore(ctx context.Context, jobID int64) error {
	ticker := time.NewTicker(ct.pollInterval)
	defer ticker.Stop()

	job := strconv.FormatInt(jobID, 10)
	ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.AddRestoreImportJob(ct.targetNS, job, ct.collBackup.GetSize()))

	for {
		info, err := ct.grpcCli.GetRestoreSnapshotState(ctx, jobID)
		if err != nil {
			return fmt.Errorf("restore: get restore snapshot state: %w", err)
		}

		ct.taskMgr.UpdateRestoreTask(ct.taskID, taskmgr.UpdateRestoreImportJob(ct.targetNS, job, int(info.GetProgress())))

		switch info.GetState() {
		case milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted:
			return nil
		case milvuspb.RestoreSnapshotState_RestoreSnapshotFailed:
			// Milvus drops the collection it created if the restore fails before the copy
			// starts, but a copy that fails partway leaves it in place, and the next
			// attempt will refuse to restore over it.
			ct.logger.Error("snapshot restore job failed, the target collection may need to be dropped by hand",
				zap.Int64("job_id", jobID),
				zap.String("reason", info.GetReason()))
			return fmt.Errorf("restore: snapshot restore job %d failed: %s", jobID, info.GetReason())
		}

		ct.logger.Debug("waiting for snapshot restore",
			zap.Int64("job_id", jobID),
			zap.Int32("progress", info.GetProgress()))

		select {
		case <-ctx.Done():
			return fmt.Errorf("restore: wait snapshot restore job %d: %w", jobID, ctx.Err())
		case <-ticker.C:
		}
	}
}
