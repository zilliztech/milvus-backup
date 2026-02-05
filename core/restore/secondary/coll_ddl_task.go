package secondary

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

type collDDLTask struct {
	taskID string

	backupInfo *backuppb.BackupInfo
	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	tsAlloc *tsAlloc

	streamCli milvus.Stream
	logger    *zap.Logger
}

type ddlTaskArgs struct {
	TaskID string

	BackupInfo *backuppb.BackupInfo

	StreamCli milvus.Stream
	TSAlloc   *tsAlloc
}

func newCollDDLTask(args ddlTaskArgs, dbBackup *backuppb.DatabaseBackupInfo, collBackup *backuppb.CollectionBackupInfo) *collDDLTask {
	ns := namespace.New(collBackup.GetDbName(), collBackup.GetCollectionName())

	return &collDDLTask{
		taskID: args.TaskID,

		backupInfo: args.BackupInfo,
		dbBackup:   dbBackup,
		collBackup: collBackup,

		tsAlloc: args.TSAlloc,

		streamCli: args.StreamCli,
		logger:    log.With(zap.String("task_id", args.TaskID), zap.String("ns", ns.String())),
	}
}

func (ddlt *collDDLTask) Execute(ctx context.Context) error {
	if err := ddlt.createColl(ctx); err != nil {
		return fmt.Errorf("collection: create collection: %w", err)
	}

	if err := ddlt.createIndexes(ctx); err != nil {
		return fmt.Errorf("collection: create indexes: %w", err)
	}

	return nil
}

func (ddlt *collDDLTask) createIndexes(ctx context.Context) error {
	for _, index := range ddlt.collBackup.GetIndexInfos() {
		if err := ddlt.createIndex(ctx, index); err != nil {
			return fmt.Errorf("collection: create index: %w", err)
		}
	}

	return nil
}

func (ddlt *collDDLTask) createIndex(ctx context.Context, index *backuppb.IndexInfo) error {
	indexInfo := &indexpb.IndexInfo{
		CollectionID:    ddlt.collBackup.GetCollectionId(),
		FieldID:         index.GetFieldId(),
		IndexName:       index.GetIndexName(),
		IndexID:         index.GetIndexId(),
		TypeParams:      pbconv.BakKVToMilvusKV(index.GetTypeParams()),
		IndexParams:     pbconv.BakKVToMilvusKV(index.GetIndexParams()),
		IsAutoIndex:     index.GetIsAutoIndex(),
		UserIndexParams: pbconv.BakKVToMilvusKV(index.GetUserIndexParams()),
		MinIndexVersion: index.GetMinIndexVersion(),
		MaxIndexVersion: index.GetMaxIndexVersion(),
	}
	fieldIndex := &indexpb.FieldIndex{IndexInfo: indexInfo, CreateTime: index.GetCreateTime()}
	body := &message.CreateIndexMessageBody{FieldIndex: fieldIndex}
	header := &message.CreateIndexMessageHeader{
		DbId:         ddlt.dbBackup.GetDbId(),
		CollectionId: ddlt.collBackup.GetCollectionId(),
		FieldId:      index.GetFieldId(),
		IndexId:      index.GetIndexId(),
		IndexName:    index.GetIndexName(),
	}

	ddlt.logger.Info("create index", zap.Any("header", header), zap.Any("body", body))

	builder := message.NewCreateIndexMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast([]string{ddlt.backupInfo.GetControlChannelName()})

	broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
	msgs := broadcast.SplitIntoMutableMessage()

	for _, msg := range msgs {
		ts := ddlt.tsAlloc.Alloc()
		immutableMessage := msg.WithTimeTick(ts).
			WithLastConfirmed(newFakeMessageID(ts)).
			IntoImmutableMessage(newFakeMessageID(ts)).
			IntoImmutableMessageProto()

		if err := ddlt.streamCli.Send(ctx, immutableMessage); err != nil {
			return fmt.Errorf("collection: broadcast create index: %w", err)
		}
	}

	return nil
}

func (ddlt *collDDLTask) partitionNames() []string {
	return lo.Map(ddlt.collBackup.GetPartitionBackups(), func(part *backuppb.PartitionBackupInfo, _ int) string {
		return part.GetPartitionName()
	})
}

func (ddlt *collDDLTask) partitionIDs() []int64 {
	return lo.Map(ddlt.collBackup.GetPartitionBackups(), func(part *backuppb.PartitionBackupInfo, _ int) int64 {
		return part.GetPartitionId()
	})
}

func (ddlt *collDDLTask) createColl(ctx context.Context) error {
	header := &message.CreateCollectionMessageHeader{
		CollectionId: ddlt.collBackup.GetCollectionId(),
		DbId:         ddlt.dbBackup.GetDbId(),
		PartitionIds: ddlt.partitionIDs(),
	}

	ddlt.logger.Info("create collection", zap.Any("header", header))

	schema, err := conv.Schema(ddlt.collBackup.GetSchema())
	if err != nil {
		return fmt.Errorf("secondary: convert schema: %w", err)
	}
	schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{
		Key:   common.ConsistencyLevel,
		Value: ddlt.collBackup.GetConsistencyLevel().String(),
	})
	appendSysFields(schema)
	appendDynamicField(schema)

	ddlt.logger.Info("collection schema", zap.Any("schema", schema))

	req := &message.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			Timestamp: ddlt.tsAlloc.Alloc(),
		},
		DbName:               ddlt.collBackup.GetDbName(),
		CollectionName:       ddlt.collBackup.GetCollectionName(),
		DbID:                 ddlt.dbBackup.GetDbId(),
		CollectionID:         ddlt.collBackup.GetCollectionId(),
		VirtualChannelNames:  ddlt.collBackup.GetVirtualChannelNames(),
		PhysicalChannelNames: ddlt.collBackup.GetPhysicalChannelNames(),
		CollectionSchema:     schema,
		PartitionNames:       ddlt.partitionNames(),
		PartitionIDs:         ddlt.partitionIDs(),
	}
	ddlt.logger.Info("create collection", zap.Any("request", req))

	builder := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(header).
		WithBody(req).
		WithBroadcast(append(ddlt.collBackup.GetVirtualChannelNames(), ddlt.backupInfo.GetControlChannelName()))

	broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
	msgs := broadcast.SplitIntoMutableMessage()

	for _, msg := range msgs {
		ts := ddlt.tsAlloc.Alloc()
		immutableMessage := msg.WithTimeTick(ts).
			WithLastConfirmed(newFakeMessageID(ts)).
			IntoImmutableMessage(newFakeMessageID(ts)).
			IntoImmutableMessageProto()

		if err := ddlt.streamCli.Send(ctx, immutableMessage); err != nil {
			return fmt.Errorf("secondary: broadcast create collection: %w", err)
		}
	}

	return nil
}
