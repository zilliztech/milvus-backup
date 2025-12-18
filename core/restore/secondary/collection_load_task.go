package secondary

import (
	"context"
	"math/rand/v2"
	"sort"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/namespace"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
)

type loadTaskArgs struct {
	TaskID string

	BackupInfo *backuppb.BackupInfo

	StreamCli milvus.Stream
	TSAlloc   *tsAlloc
}

type collectionLoadTask struct {
	taskID string

	backupInfo *backuppb.BackupInfo
	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	tsAlloc *tsAlloc

	streamCli milvus.Stream
	logger    *zap.Logger
}

func newCollectionLoadTask(args loadTaskArgs, dbBackup *backuppb.DatabaseBackupInfo, collBackup *backuppb.CollectionBackupInfo) *collectionLoadTask {
	ns := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())

	return &collectionLoadTask{
		taskID: args.TaskID,

		backupInfo: args.BackupInfo,
		dbBackup:   dbBackup,
		collBackup: collBackup,

		tsAlloc: args.TSAlloc,

		streamCli: args.StreamCli,
		logger:    log.With(zap.String("task_id", args.TaskID), zap.String("ns", ns.String())),
	}
}

func (clt *collectionLoadTask) Execute(_ context.Context) error {
	if clt.collBackup.GetLoadState() == meta.LoadStateNotload {
		clt.logger.Info("collection not load, skip load")
		return nil
	}

	clt.logger.Info("load collection")

	header := clt.buildHeader()

	builder := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{clt.backupInfo.GetControlChannelName()})

	broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
	msgs := broadcast.SplitIntoMutableMessage()
	for _, msg := range msgs {
		ts := clt.tsAlloc.Alloc()
		immutableMessage := msg.WithTimeTick(ts).
			WithLastConfirmed(newFakeMessageID(ts)).
			IntoImmutableMessage(newFakeMessageID(ts)).
			IntoImmutableMessageProto()

		if err := clt.streamCli.Send(immutableMessage); err != nil {
			return err
		}
	}

	return nil

}

func (clt *collectionLoadTask) buildLoadFields() []*messagespb.LoadFieldConfig {
	indexField := lo.SliceToMap(clt.collBackup.GetIndexInfos(), func(index *backuppb.IndexInfo) (int64, *backuppb.IndexInfo) {
		return index.GetIndexId(), index
	})

	fieldConfigs := make([]*messagespb.LoadFieldConfig, 0, len(clt.collBackup.GetSchema().GetFields()))
	for _, field := range clt.collBackup.GetSchema().GetFields() {
		indexInfo := indexField[field.GetFieldID()]
		fieldConfig := &messagespb.LoadFieldConfig{
			FieldId: field.GetFieldID(),
			IndexId: indexInfo.GetIndexId(),
		}
		fieldConfigs = append(fieldConfigs, fieldConfig)
	}

	return fieldConfigs
}

func (clt *collectionLoadTask) buildLoadConfig() []*messagespb.LoadReplicaConfig {
	replicaConfigs := make([]*messagespb.LoadReplicaConfig, 0, len(clt.collBackup.GetReplicas()))

	for _, replica := range clt.collBackup.GetReplicas() {
		rgName := replica.GetResourceGroupName()
		if len(rgName) == 0 {
			rgName = "__default_resource_group"
		}

		replicaConfig := &messagespb.LoadReplicaConfig{
			ReplicaId:         replica.GetReplicaID(),
			ResourceGroupName: rgName,
		}
		replicaConfigs = append(replicaConfigs, replicaConfig)
	}

	sort.Slice(replicaConfigs, func(i, j int) bool {
		return replicaConfigs[i].GetReplicaId() < replicaConfigs[j].GetReplicaId()
	})

	return replicaConfigs
}

func (clt *collectionLoadTask) buildHeader() *message.AlterLoadConfigMessageHeader {
	partitionIDs := lo.Map(clt.collBackup.GetPartitionBackups(), func(partition *backuppb.PartitionBackupInfo, _ int) int64 {
		return partition.GetPartitionId()
	})

	sort.Slice(partitionIDs, func(i, j int) bool {
		return partitionIDs[i] < partitionIDs[j]
	})

	return &message.AlterLoadConfigMessageHeader{
		DbId:         clt.dbBackup.GetDbId(),
		CollectionId: clt.collBackup.GetCollectionId(),
		PartitionIds: partitionIDs,
		LoadFields:   clt.buildLoadFields(),
		Replicas:     clt.buildLoadConfig(),
	}
}
