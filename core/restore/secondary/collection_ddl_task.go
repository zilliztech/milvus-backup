package secondary

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/taskmgr"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

type collectionDDLTask struct {
	taskID string

	backupInfo *backuppb.BackupInfo
	dbBackup   *backuppb.DatabaseBackupInfo
	collBackup *backuppb.CollectionBackupInfo

	taskMgr *taskmgr.Mgr

	streamCli milvus.Stream
	logger    *zap.Logger
}

func newCollectionDDLTask(taskID string, backupInfo *backuppb.BackupInfo, dbBackup *backuppb.DatabaseBackupInfo, collBackup *backuppb.CollectionBackupInfo, streamCli milvus.Stream) *collectionDDLTask {
	return &collectionDDLTask{
		taskID: taskID,

		backupInfo: backupInfo,
		dbBackup:   dbBackup,
		collBackup: collBackup,

		streamCli: streamCli,
		logger:    log.With(zap.String("task_id", taskID)),
	}
}

func (ddlt *collectionDDLTask) Execute(ctx context.Context) error {
	if err := ddlt.createColl(); err != nil {
		return fmt.Errorf("collection: create collection: %w", err)
	}

	if err := ddlt.createIndexes(); err != nil {
		return fmt.Errorf("collection: create indexes: %w", err)
	}

	time.Sleep(10)

	return nil
}

func (ddlt *collectionDDLTask) createIndexes() error {
	for _, index := range ddlt.collBackup.GetIndexInfos() {
		if err := ddlt.createIndex(index); err != nil {
			return fmt.Errorf("collection: create index: %w", err)
		}
	}

	return nil
}

func (ddlt *collectionDDLTask) createIndex(index *backuppb.IndexInfo) error {
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

	builder := message.NewCreateIndexMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast([]string{ddlt.backupInfo.GetControlChannelName()})

	broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
	msgs := broadcast.SplitIntoMutableMessage()

	for _, msg := range msgs {
		if err := ddlt.streamCli.Send(msg); err != nil {
			return fmt.Errorf("collection: broadcast create index: %w", err)
		}
	}

	return nil
}

func (ddlt *collectionDDLTask) convFields(bakFields []*backuppb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	fields := make([]*schemapb.FieldSchema, 0, len(bakFields))

	for _, bakField := range bakFields {
		defaultValue, err := conv.DefaultValue(bakField)
		if err != nil {
			return nil, fmt.Errorf("restore: failed to get default value: %w", err)
		}

		fieldRestore := &schemapb.FieldSchema{
			FieldID:          bakField.GetFieldID(),
			Name:             bakField.GetName(),
			IsPrimaryKey:     bakField.GetIsPrimaryKey(),
			AutoID:           bakField.GetAutoID(),
			Description:      bakField.GetDescription(),
			DataType:         schemapb.DataType(bakField.GetDataType()),
			TypeParams:       pbconv.BakKVToMilvusKV(bakField.GetTypeParams()),
			IndexParams:      pbconv.BakKVToMilvusKV(bakField.GetIndexParams()),
			IsDynamic:        bakField.GetIsDynamic(),
			IsPartitionKey:   bakField.GetIsPartitionKey(),
			Nullable:         bakField.GetNullable(),
			ElementType:      schemapb.DataType(bakField.GetElementType()),
			IsFunctionOutput: bakField.GetIsFunctionOutput(),
			DefaultValue:     defaultValue,
		}

		fields = append(fields, fieldRestore)
	}

	return fields, nil
}

func (ddlt *collectionDDLTask) structArrayFields() ([]*schemapb.StructArrayFieldSchema, error) {
	bakFields := ddlt.collBackup.GetSchema().GetStructArrayFields()
	structArrayFields := make([]*schemapb.StructArrayFieldSchema, 0, len(bakFields))
	for _, bakField := range bakFields {
		fields, err := ddlt.convFields(bakField.GetFields())
		if err != nil {
			return nil, fmt.Errorf("restore: failed to convert struct array fields: %w", err)
		}

		structArrayField := &schemapb.StructArrayFieldSchema{
			FieldID:     bakField.GetFieldID(),
			Name:        bakField.GetName(),
			Description: bakField.GetDescription(),
			Fields:      fields,
		}

		structArrayFields = append(structArrayFields, structArrayField)
	}

	return structArrayFields, nil
}

func (ddlt *collectionDDLTask) schema() (*schemapb.CollectionSchema, error) {
	fields, err := ddlt.convFields(ddlt.collBackup.GetSchema().GetFields())
	if err != nil {
		return nil, fmt.Errorf("collection: get fields: %w", err)
	}
	ddlt.logger.Info("restore collection fields", zap.Any("fields", fields))

	functions := conv.Functions(ddlt.collBackup.GetSchema().GetFunctions())
	ddlt.logger.Info("restore collection functions", zap.Any("functions", functions))

	structArrayFields, err := ddlt.structArrayFields()
	if err != nil {
		return nil, fmt.Errorf("restore: conv struct array fields: %w", err)
	}

	properties := pbconv.BakKVToMilvusKV(ddlt.collBackup.GetSchema().GetProperties())
	properties = append(properties, &commonpb.KeyValuePair{
		Key:   common.ConsistencyLevel,
		Value: ddlt.collBackup.GetConsistencyLevel().String(),
	})

	schema := &schemapb.CollectionSchema{
		Name:               ddlt.collBackup.GetCollectionName(),
		Description:        ddlt.collBackup.GetSchema().GetDescription(),
		Functions:          functions,
		Fields:             fields,
		EnableDynamicField: ddlt.collBackup.GetSchema().GetEnableDynamicField(),
		Properties:         properties,
		StructArrayFields:  structArrayFields,
	}

	return schema, nil
}

func (ddlt *collectionDDLTask) partitionNames() []string {
	return lo.Map(ddlt.collBackup.GetPartitionBackups(), func(part *backuppb.PartitionBackupInfo, _ int) string {
		return part.GetPartitionName()
	})
}

func (ddlt *collectionDDLTask) partitionIDs() []int64 {
	return lo.Map(ddlt.collBackup.GetPartitionBackups(), func(part *backuppb.PartitionBackupInfo, _ int) int64 {
		return part.GetPartitionId()
	})
}

func (ddlt *collectionDDLTask) createColl() error {
	header := &message.CreateCollectionMessageHeader{
		CollectionId: ddlt.collBackup.GetCollectionId(),
		DbId:         ddlt.dbBackup.GetDbId(),
	}
	ddlt.logger.Info("create collection", zap.Any("header", header))

	schema, err := ddlt.schema()
	if err != nil {
		return fmt.Errorf("collection: get schema: %w", err)
	}
	req := &message.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			Timestamp: 1,
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
		if err := ddlt.streamCli.Send(msg); err != nil {
			return fmt.Errorf("collection: broadcast create collection: %w", err)
		}
	}

	return nil
}
