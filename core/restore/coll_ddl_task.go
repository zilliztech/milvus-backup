package restore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/core/restore/funcs"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/validate"
)

type collDDLTask struct {
	option       *Option
	collOverride CollOverride

	collBackup *backuppb.CollectionBackupInfo

	target  collref.Name
	grpcCli milvus.Grpc

	tracker *jobstate.RestoreCollDDLTracker

	logger *zap.Logger
}

func newCollDDLTask(taskID string, opt *Option, override CollOverride, collBackup *backuppb.CollectionBackupInfo, target collref.Name, grpcCli milvus.Grpc, tracker *jobstate.RestoreCollDDLTracker) *collDDLTask {
	return &collDDLTask{
		option:       opt,
		collOverride: override,
		collBackup:   collBackup,
		target:       target,
		grpcCli:      grpcCli,
		tracker:      tracker,
		logger:       log.With(zap.String("task_id", taskID), zap.String("target_coll", target.String())),
	}
}

// Execute restores the collection's schema side and settles the schema half's
// state itself: the collection's overall state is derived from its halves, so
// no one above the task settles it.
func (ddlt *collDDLTask) Execute(ctx context.Context) error {
	ddlt.tracker.SetExecuting()
	ddlt.logger.Info("start restore collection ddl")

	if err := ddlt.execute(ctx); err != nil {
		ddlt.tracker.SetFail(err)
		return err
	}

	ddlt.tracker.SetSuccess()
	return nil
}

func (ddlt *collDDLTask) execute(ctx context.Context) error {
	// Validate the complete request before a requested drop can remove the target.
	var input milvus.CreateCollectionInput
	var addFields []*schemapb.FieldSchema
	if !ddlt.option.SkipCreateCollection {
		var err error
		input, addFields, err = ddlt.createCollectionInput()
		if err != nil {
			return err
		}
	} else {
		// Skipping DDL does not make an older import understand new column shapes.
		schema, err := ddlt.sourceSchema()
		if err != nil {
			return err
		}
		if err := conv.ValidateLegacySchema(schema); err != nil {
			if !errors.Is(err, conv.ErrSchemaEvolutionUnsupported) || !ddlt.grpcCli.HasFeature(milvus.PreserveFieldIDs) {
				return err
			}
		}
	}
	// restore collection schema
	if err := ddlt.dropExistedColl(ctx); err != nil {
		return fmt.Errorf("restore: drop exist collection: %w", err)
	}
	if err := ddlt.createCollWithInput(ctx, input, addFields); err != nil {
		return fmt.Errorf("restore: create collection: %w", err)
	}
	if err := ddlt.restoreFuncRuntimeCheck(ctx); err != nil {
		return fmt.Errorf("restore: restore func runtime check: %w", err)
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

func (ddlt *collDDLTask) dropExistedColl(ctx context.Context) error {
	if !ddlt.option.DropExistCollection {
		ddlt.logger.Info("skip drop existed collection")
		return nil
	}

	ddlt.logger.Info("start drop existed collection")
	exist, err := ddlt.grpcCli.HasCollection(ctx, ddlt.target.DBName(), ddlt.target.CollName())
	if err != nil {
		return fmt.Errorf("collection: check collection exist: %w", err)
	}
	if !exist {
		ddlt.logger.Info("collection not exist, skip drop collection")
		return nil
	}

	if err := ddlt.grpcCli.DropCollection(ctx, ddlt.target.DBName(), ddlt.target.CollName()); err != nil {
		return fmt.Errorf("collection: drop collection: %w", err)
	}

	return nil
}

// fields returns two types of fields:
// 1. fields that can be created in CreateCollection API
// 2. fields that need to be created by addField API
//
// If the original collection had addField operations, the $meta field might not be at the end of the fields list.
// However, the CreateCollection API always appends the $meta field to the end.
// This discrepancy in field order between the new collection and the backup file can cause restore errors.
// Therefore, we stop processing fields if the field IDs are not continuous.
// The remaining fields will be imported via addField.
//
// The dynamic field ($meta) is never returned in either slice: when
// EnableDynamicField is true, Milvus auto-creates $meta inside CreateCollection,
// and the AddField API rejects dynamic fields. Newer backups carry the actual
// $meta schema in the backup metadata (see zilliztech/milvus-backup#1013) but
// for the regular restore path it is purely informational.
func (ddlt *collDDLTask) fields() ([]*schemapb.FieldSchema, []*schemapb.FieldSchema, error) {
	schema, err := ddlt.sourceSchema()
	if err != nil {
		return nil, nil, err
	}
	create, add := legacyFields(schema)
	return create, add, nil
}

func legacyFields(schema *schemapb.CollectionSchema) ([]*schemapb.FieldSchema, []*schemapb.FieldSchema) {
	fields := make([]*schemapb.FieldSchema, 0, len(schema.GetFields()))
	var dynamicID int64
	for _, field := range schema.GetFields() {
		if field.GetName() == common.NamespaceFieldName {
			continue
		}
		if field.GetIsDynamic() {
			dynamicID = field.GetFieldID()
			continue
		}
		fields = append(fields, field)
	}
	if !schema.GetEnableDynamicField() {
		return fields, nil
	}
	if dynamicID == 0 {
		dynamicID = funcs.GuessDynFieldID(fields)
	}
	var create, add []*schemapb.FieldSchema
	for _, field := range fields {
		if field.GetFieldID() < dynamicID {
			create = append(create, field)
		} else {
			add = append(add, field)
		}
	}
	return create, add
}

func (ddlt *collDDLTask) addFields(ctx context.Context, fields []*schemapb.FieldSchema) error {
	// add fields
	for _, field := range fields {
		if err := ddlt.grpcCli.AddField(ctx, ddlt.target.DBName(), ddlt.target.CollName(), field); err != nil {
			return fmt.Errorf("restore: add field %s id %d: %w", field.GetName(), field.GetFieldID(), err)
		}
	}

	return nil
}

// properties returns the properties of the collection.
// If milvus support func runtime check, add disable_auto_function to properties to avoid error when create collection.
func (ddlt *collDDLTask) properties() []*commonpb.KeyValuePair {
	return ddlt.propertiesForSchema(&schemapb.CollectionSchema{
		Properties: pbconv.BakKVToMilvusKV(ddlt.collBackup.GetSchema().GetProperties()),
		Functions:  conv.Functions(ddlt.collBackup.GetSchema().GetFunctions()),
	})
}

func (ddlt *collDDLTask) propertiesForSchema(schema *schemapb.CollectionSchema) []*commonpb.KeyValuePair {
	props := filterParams(schema.GetProperties(), ddlt.option.SkipParams.CollectionProperties)
	// This is a request capability, not a persistent collection property.
	props = lo.Filter(props, func(item *commonpb.KeyValuePair, _ int) bool {
		return item.GetKey() != preserveFieldIDsProperty
	})
	if len(schema.GetFunctions()) == 0 || !ddlt.grpcCli.HasFeature(milvus.FuncRuntimeCheck) {
		return props
	}
	props = lo.Filter(props, func(item *commonpb.KeyValuePair, _ int) bool {
		return item.GetKey() != common.DisableFuncRuntimeCheck
	})
	return append(props, &commonpb.KeyValuePair{Key: common.DisableFuncRuntimeCheck, Value: "true"})
}

// restoreFuncRuntimeCheck restores the disable_func_runtime_check property to original value from backup.
// properties function add disable_auto_function to properties, we need to restore the original value from backup.
func (ddlt *collDDLTask) restoreFuncRuntimeCheck(ctx context.Context) error {
	schema, err := ddlt.sourceSchema()
	if err != nil {
		return err
	}
	if len(schema.GetFunctions()) == 0 {
		ddlt.logger.Info("no functions, skip restore func runtime check")
		return nil
	}

	if !ddlt.grpcCli.HasFeature(milvus.FuncRuntimeCheck) {
		ddlt.logger.Info("milvus does not support func runtime check, skip restore")
		return nil
	}

	ddlt.logger.Info("milvus support func runtime check, restore original value")
	val := "false"
	for _, kv := range schema.GetProperties() {
		if kv.GetKey() == common.DisableFuncRuntimeCheck {
			val = kv.GetValue()
			break
		}
	}

	if val == "true" {
		ddlt.logger.Info("original disable_func_runtime_check is true, skip restore")
		return nil
	}

	ddlt.logger.Info("restore disable_func_runtime_check to false")
	props := []*commonpb.KeyValuePair{{Key: common.DisableFuncRuntimeCheck, Value: "false"}}
	if err := ddlt.grpcCli.AlterCollection(ctx, ddlt.target.DBName(), ddlt.target.CollName(), props); err != nil {
		return fmt.Errorf("restore: alter collection: %w", err)
	}

	return nil
}

func (ddlt *collDDLTask) createColl(ctx context.Context) error {
	if ddlt.option.SkipCreateCollection {
		return nil
	}
	input, addFields, err := ddlt.createCollectionInput()
	if err != nil {
		return err
	}
	return ddlt.createCollWithInput(ctx, input, addFields)
}

func (ddlt *collDDLTask) createCollWithInput(ctx context.Context, input milvus.CreateCollectionInput, addFields []*schemapb.FieldSchema) error {
	if ddlt.option.SkipCreateCollection {
		ddlt.logger.Info("skip create collection")
		return nil
	}
	ddlt.logger.Info("create collection", zap.Any("schema", input.Schema))
	if err := ddlt.grpcCli.CreateCollection(ctx, input); err != nil {
		return fmt.Errorf("restore: call create collection api after retry: %w", err)
	}
	return ddlt.addFields(ctx, addFields)
}

func (ddlt *collDDLTask) sourceSchema() (*schemapb.CollectionSchema, error) {
	return conv.SchemaFromCollection(ddlt.collBackup)
}

// shardNum returns the shard number of the collection.
// if MaxShardNum is set and greater than the shard number in backup, use MaxShardNum
func (ddlt *collDDLTask) shardNum() int32 {
	// per-collection override takes highest priority
	if ddlt.collOverride.ShardNum > 0 {
		ddlt.logger.Info("overwrite shardNum by collection override",
			zap.Int32("old", ddlt.collBackup.GetShardsNum()),
			zap.Int32("new", ddlt.collOverride.ShardNum))
		return ddlt.collOverride.ShardNum
	}

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

func (ddlt *collDDLTask) description() string {
	if ddlt.collOverride.Description != "" {
		return ddlt.collOverride.Description
	}
	return ddlt.collBackup.GetSchema().GetDescription()
}

func (ddlt *collDDLTask) dropExistedIndex(ctx context.Context) error {
	if !ddlt.option.DropExistIndex {
		ddlt.logger.Info("skip drop existed index")
		return nil
	}

	ddlt.logger.Info("start drop existed index")
	indexes, err := ddlt.grpcCli.ListIndex(ctx, ddlt.target.DBName(), ddlt.target.CollName())
	if err != nil {
		ddlt.logger.Warn("fail in list index", zap.Error(err))
		return nil
	}

	for _, index := range indexes {
		err = ddlt.grpcCli.DropIndex(ctx, ddlt.target.DBName(), ddlt.target.CollName(), index.GetIndexName())
		if err != nil {
			return fmt.Errorf("restore: drop index %s: %w", index.IndexName, err)
		}
		ddlt.logger.Info("drop index", zap.String("field_name", index.FieldName),
			zap.String("index_name", index.IndexName))
	}

	return nil
}

func (ddlt *collDDLTask) createIndex(ctx context.Context) error {
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
func (ddlt *collDDLTask) specialIndexName(indexName string) bool {
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

func (ddlt *collDDLTask) restoreScalarFieldIdx(ctx context.Context, indexes []*backuppb.IndexInfo) error {
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
			DB:             ddlt.target.DBName(),
			CollectionName: ddlt.target.CollName(),
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

func (ddlt *collDDLTask) restoreVectorFieldIdx(ctx context.Context, indexes []*backuppb.IndexInfo) error {
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
			DB:             ddlt.target.DBName(),
			CollectionName: ddlt.target.CollName(),
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

func (ddlt *collDDLTask) createPartitions(ctx context.Context) error {
	for _, part := range ddlt.collBackup.GetPartitionBackups() {
		if err := ddlt.createPartition(ctx, part.GetPartitionName()); err != nil {
			return fmt.Errorf("restore: create partition: %w", err)
		}
	}

	return nil
}

func (ddlt *collDDLTask) createPartition(ctx context.Context, partitionName string) error {
	// pre-check whether partition exist, if not create it
	ddlt.logger.Debug("check partition exist", zap.String("partition_name", partitionName))
	exist, err := ddlt.grpcCli.HasPartition(ctx, ddlt.target.DBName(), ddlt.target.CollName(), partitionName)
	if err != nil {
		return fmt.Errorf("restore: failed to check partition exist: %w", err)
	}
	if exist {
		ddlt.logger.Info("partition exist, skip create partition")
		return nil
	}

	err = ddlt.grpcCli.CreatePartition(ctx, ddlt.target.DBName(), ddlt.target.CollName(), partitionName)
	if err != nil {
		return fmt.Errorf("restore: create partition %s: %w", partitionName, err)
	}
	ddlt.logger.Debug("create partition success", zap.String("partition_name", partitionName))
	return nil
}
