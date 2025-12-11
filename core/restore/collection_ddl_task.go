package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/validate"
)

type collectionDDLTask struct {
	option *Option

	collBackup *backuppb.CollectionBackupInfo

	targetNS namespace.NS
	grpcCli  milvus.Grpc

	logger *zap.Logger
}

func newCollectionDDLTask(taskID string, opt *Option, collBackup *backuppb.CollectionBackupInfo, targetNS namespace.NS, grpcCli milvus.Grpc) *collectionDDLTask {
	return &collectionDDLTask{
		option:     opt,
		collBackup: collBackup,
		targetNS:   targetNS,
		grpcCli:    grpcCli,
		logger:     log.With(zap.String("task_id", taskID), zap.String("target_ns", targetNS.String())),
	}
}

func (ddlt *collectionDDLTask) Execute(ctx context.Context) error {
	ddlt.logger.Info("start restore collection ddl")

	// restore collection schema
	if err := ddlt.dropExistedColl(ctx); err != nil {
		return fmt.Errorf("restore: drop exist collection: %w", err)
	}
	if err := ddlt.createColl(ctx); err != nil {
		return fmt.Errorf("restore: create collection: %w", err)
	}

	// restore collection index
	if err := ddlt.dropExistedIndex(ctx); err != nil {
		return fmt.Errorf("restore: drop exist index: %w", err)
	}
	if err := ddlt.createIndex(ctx); err != nil {
		return fmt.Errorf("restore: create index: %w", err)
	}

	// restore collection partitions
	if err := ddlt.createPartitions(ctx); err != nil {
		return fmt.Errorf("restore: create partitions: %w", err)
	}

	return nil
}

func (ddlt *collectionDDLTask) dropExistedColl(ctx context.Context) error {
	if !ddlt.option.DropExistCollection {
		ddlt.logger.Info("skip drop existed collection")
		return nil
	}

	ddlt.logger.Info("start drop existed collection")
	exist, err := ddlt.grpcCli.HasCollection(ctx, ddlt.targetNS.DBName(), ddlt.targetNS.CollName())
	if err != nil {
		return fmt.Errorf("collection: check collection exist: %w", err)
	}
	if !exist {
		ddlt.logger.Info("collection not exist, skip drop collection")
		return nil
	}

	if err := ddlt.grpcCli.DropCollection(ctx, ddlt.targetNS.DBName(), ddlt.targetNS.CollName()); err != nil {
		return fmt.Errorf("collection: drop collection: %w", err)
	}

	return nil
}

func (ddlt *collectionDDLTask) createColl(ctx context.Context) error {
	if ddlt.option.SkipCreateCollection {
		ddlt.logger.Info("skip create collection")
		return nil
	}

	fields, err := ddlt.convFields(ddlt.collBackup.GetSchema().GetFields())
	if err != nil {
		return fmt.Errorf("collection: get fields: %w", err)
	}
	ddlt.logger.Info("restore collection fields", zap.Any("fields", fields))

	functions := conv.Functions(ddlt.collBackup.GetSchema().GetFunctions())
	ddlt.logger.Info("restore collection functions", zap.Any("functions", functions))

	structArrayFields, err := ddlt.structArrayFields()
	if err != nil {
		return fmt.Errorf("restore: conv struct array fields: %w", err)
	}

	schema := &schemapb.CollectionSchema{
		Name:               ddlt.targetNS.CollName(),
		Description:        ddlt.collBackup.GetSchema().GetDescription(),
		AutoID:             ddlt.collBackup.GetSchema().GetAutoID(),
		Functions:          functions,
		Fields:             fields,
		EnableDynamicField: ddlt.collBackup.GetSchema().GetEnableDynamicField(),
		Properties:         pbconv.BakKVToMilvusKV(ddlt.collBackup.GetSchema().GetProperties()),
		StructArrayFields:  structArrayFields,
	}

	opt := milvus.CreateCollectionInput{
		DB:           ddlt.targetNS.DBName(),
		Schema:       schema,
		ConsLevel:    commonpb.ConsistencyLevel(ddlt.collBackup.GetConsistencyLevel()),
		ShardNum:     ddlt.shardNum(),
		PartitionNum: ddlt.partitionNum(),
		Properties:   pbconv.BakKVToMilvusKV(ddlt.collBackup.GetProperties(), ddlt.option.SkipParams.CollectionProperties...),
	}
	if err := ddlt.grpcCli.CreateCollection(ctx, opt); err != nil {
		return fmt.Errorf("restore: call create collection api after retry: %w", err)
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
			TypeParams:       pbconv.BakKVToMilvusKV(bakField.GetTypeParams(), ddlt.option.SkipParams.FieldTypeParams...),
			IndexParams:      pbconv.BakKVToMilvusKV(bakField.GetIndexParams(), ddlt.option.SkipParams.FieldIndexParams...),
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

// shardNum returns the shard number of the collection.
// if MaxShardNum is set and greater than the shard number in backup, use MaxShardNum
func (ddlt *collectionDDLTask) shardNum() int32 {
	// overwrite shardNum by request parameter
	shardNum := ddlt.collBackup.GetShardsNum()
	if ddlt.option.MaxShardNum > 0 && shardNum > ddlt.option.MaxShardNum {
		shardNum = ddlt.option.MaxShardNum
		ddlt.logger.Info("overwrite shardNum by request parameter",
			zap.Int32("old", ddlt.collBackup.GetShardsNum()),
			zap.Int32("new", shardNum))
	}

	return shardNum
}

// partitionNum returns the partition number of the collection
// if partition key was set, return the length of partition in backup
// else return 0.
func (ddlt *collectionDDLTask) partitionNum() int {
	var hasPartitionKey bool
	for _, field := range ddlt.collBackup.GetSchema().GetFields() {
		if field.GetIsPartitionKey() {
			hasPartitionKey = true
			break
		}
	}

	if hasPartitionKey {
		return len(ddlt.collBackup.GetPartitionBackups())
	}

	return 0
}

func (ddlt *collectionDDLTask) dropExistedIndex(ctx context.Context) error {
	if !ddlt.option.DropExistIndex {
		ddlt.logger.Info("skip drop existed index")
		return nil
	}

	ddlt.logger.Info("start drop existed index")
	indexes, err := ddlt.grpcCli.ListIndex(ctx, ddlt.targetNS.DBName(), ddlt.targetNS.CollName())
	if err != nil {
		ddlt.logger.Warn("fail in list index", zap.Error(err))
		return nil
	}

	for _, index := range indexes {
		err = ddlt.grpcCli.DropIndex(ctx, ddlt.targetNS.DBName(), ddlt.targetNS.CollName(), index.GetIndexName())
		if err != nil {
			return fmt.Errorf("restore: drop index %s: %w", index.IndexName, err)
		}
		ddlt.logger.Info("drop index", zap.String("field_name", index.FieldName),
			zap.String("index_name", index.IndexName))
	}

	return nil
}

func (ddlt *collectionDDLTask) createIndex(ctx context.Context) error {
	if !ddlt.option.RebuildIndex {
		ddlt.logger.Info("skip rebuild index")
		return nil
	}
	ddlt.logger.Info("start rebuild index")

	vectorFields := make(map[string]struct{})
	for _, field := range ddlt.collBackup.GetSchema().GetFields() {
		typStr, ok := schemapb.DataType_name[int32(field.DataType)]
		if !ok {
			return fmt.Errorf("restore: invalid field data type %d", field.DataType)
		}

		if strings.HasSuffix(strings.ToLower(typStr), "vector") {
			vectorFields[field.Name] = struct{}{}
		}
	}

	indexes := ddlt.collBackup.GetIndexInfos()
	var vectorIndexes, scalarIndexes []*backuppb.IndexInfo
	for _, index := range indexes {
		if _, ok := vectorFields[index.GetFieldName()]; ok {
			vectorIndexes = append(vectorIndexes, index)
		} else {
			scalarIndexes = append(scalarIndexes, index)
		}
	}

	if err := ddlt.restoreVectorFieldIdx(ctx, vectorIndexes); err != nil {
		return fmt.Errorf("restore: restore vector field index: %w", err)
	}
	if err := ddlt.restoreScalarFieldIdx(ctx, scalarIndexes); err != nil {
		return fmt.Errorf("restore: restore scalar field index: %w", err)
	}

	return nil
}

// hasSpecialChar checks if the index name contains special characters
// This function is mainly copied from milvus main repo
func (ddlt *collectionDDLTask) specialIndexName(indexName string) bool {
	indexName = strings.TrimSpace(indexName)

	if indexName == "" {
		return false
	}

	runes := []rune(indexName)
	firstChar := runes[0]
	if firstChar != '_' && !validate.IsAlpha(firstChar) {
		return true
	}

	return validate.HasSpecialChar(indexName)
}

func (ddlt *collectionDDLTask) restoreScalarFieldIdx(ctx context.Context, indexes []*backuppb.IndexInfo) error {
	for _, index := range indexes {
		ddlt.logger.Info("source index",
			zap.String("index_name", index.GetIndexName()),
			zap.Any("params", index.GetParams()))

		indexName := index.GetIndexName()
		if ddlt.specialIndexName(indexName) {
			// Skip index name for JSON path index (eg. /a/b/c) in Milvus 2.5 due to special character issue
			// If milvus changed the index name validation, we should also update this function
			// TODO: Handle other special character cases if found in the future
			indexName = ""
		}

		opt := milvus.CreateIndexInput{
			DB:             ddlt.targetNS.DBName(),
			CollectionName: ddlt.targetNS.CollName(),
			FieldName:      index.GetFieldName(),
			IndexName:      indexName,
			Params:         index.GetParams(),
		}
		if err := ddlt.grpcCli.CreateIndex(ctx, opt); err != nil {
			return fmt.Errorf("restore: restore scalar idx %s: %w", index.GetIndexName(), err)
		}
	}

	return nil
}

func (ddlt *collectionDDLTask) restoreVectorFieldIdx(ctx context.Context, indexes []*backuppb.IndexInfo) error {
	for _, index := range indexes {
		ddlt.logger.Info("source  index",
			zap.String("indexName", index.GetIndexName()),
			zap.Any("params", index.GetParams()))

		var params map[string]string
		if ddlt.option.UseAutoIndex {
			ddlt.logger.Info("use auto index", zap.String("field_name", index.GetFieldName()))
			params = map[string]string{"index_type": "AUTOINDEX", "metric_type": index.GetParams()["metric_type"]}
		} else {
			ddlt.logger.Info("use source index", zap.String("field_name", index.GetFieldName()))
			params = index.GetParams()
			if params["index_type"] == "marisa-trie" {
				params["index_type"] = "Trie"
			}
		}

		opt := milvus.CreateIndexInput{
			DB:             ddlt.targetNS.DBName(),
			CollectionName: ddlt.targetNS.CollName(),
			FieldName:      index.GetFieldName(),
			IndexName:      index.GetIndexName(),
			Params:         params,
		}
		if err := ddlt.grpcCli.CreateIndex(ctx, opt); err != nil {
			return fmt.Errorf("restore: restore vec idx %s: %w", index.GetIndexName(), err)
		}
	}

	return nil
}

func (ddlt *collectionDDLTask) createPartitions(ctx context.Context) error {
	for _, part := range ddlt.collBackup.GetPartitionBackups() {
		if err := ddlt.createPartition(ctx, part.GetPartitionName()); err != nil {
			return fmt.Errorf("restore: create partition: %w", err)
		}
	}

	return nil
}

func (ddlt *collectionDDLTask) createPartition(ctx context.Context, partitionName string) error {
	// pre-check whether partition exist, if not create it
	ddlt.logger.Debug("check partition exist", zap.String("partition_name", partitionName))
	exist, err := ddlt.grpcCli.HasPartition(ctx, ddlt.targetNS.DBName(), ddlt.targetNS.CollName(), partitionName)
	if err != nil {
		return fmt.Errorf("restore: failed to check partition exist: %w", err)
	}
	if exist {
		ddlt.logger.Info("partition exist, skip create partition")
		return nil
	}

	err = ddlt.grpcCli.CreatePartition(ctx, ddlt.targetNS.DBName(), ddlt.targetNS.CollName(), partitionName)
	if err != nil {
		return fmt.Errorf("restore: create partition %s: %w", partitionName, err)
	}
	ddlt.logger.Debug("create partition success", zap.String("partition_name", partitionName))
	return nil
}
