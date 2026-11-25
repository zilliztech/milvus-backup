package restore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/core/restore/fieldid"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/validate"
)

type ddlStep int

const (
	ddlSchema ddlStep = iota
	ddlIndexes
	ddlPartitions
	ddlDone
)

type collDDLTask struct {
	step         ddlStep
	collectionID int64
	savedHasData bool

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
	if err := ctx.Err(); err != nil {
		return err
	}
	step, id := ddlt.tracker.DDLProgress()
	ddlt.step, ddlt.collectionID = ddlStep(step), id
	if ddlt.step == ddlDone {
		return nil
	}
	ddlt.tracker.SetExecuting()
	ddlt.logger.Info("start restore collection ddl")

	if err := ddlt.execute(ctx); err != nil {
		ddlt.tracker.SetFail(err)
		return err
	}

	ddlt.tracker.SetSuccess()
	return nil
}

func (ddlt *collDDLTask) checkOrBind(ctx context.Context) (*milvuspb.DescribeCollectionResponse, error) {
	info, err := ddlt.grpcCli.DescribeCollection(ctx, ddlt.target.DBName(), ddlt.target.CollName())
	if err != nil {
		return nil, err
	}
	id := info.GetCollectionID()
	if id == 0 {
		return nil, fmt.Errorf("restore: no collection ID returned for %s", ddlt.target)
	}
	if ddlt.collectionID == 0 {
		ddlt.collectionID = id
		ddlt.tracker.SetDDLProgress(int(ddlt.step), id)
	} else if ddlt.collectionID != id {
		return nil, fmt.Errorf("restore: target collection identity changed for %s (expected %d, got %d)", ddlt.target, ddlt.collectionID, id)
	}
	return info, nil
}

func (ddlt *collDDLTask) advance(step ddlStep) {
	ddlt.step = step
	ddlt.tracker.SetDDLProgress(int(step), ddlt.collectionID)
}

func (ddlt *collDDLTask) execute(ctx context.Context) error {
	// The schema path checks its target in createColl. Skipping creation or
	// resuming a later DDL step still needs the target's identity.
	if ddlt.option.EnableResume && (ddlt.option.SkipCreateCollection || ddlt.step != ddlSchema) {
		if _, err := ddlt.checkOrBind(ctx); err != nil {
			return err
		}
	}
	if ddlt.step == ddlSchema {
		var input milvus.CreateCollectionInput
		var addFields []*schemapb.FieldSchema
		if !ddlt.option.SkipCreateCollection {
			var err error
			input, addFields, err = ddlt.createCollectionInput()
			if err != nil {
				return err
			}
		}
		if err := ddlt.createColl(ctx, input, addFields); err != nil {
			return fmt.Errorf("restore: create collection: %w", err)
		}
		if err := ddlt.checkFieldIDs(ctx); err != nil {
			return err
		}
		if err := ddlt.restoreFuncRuntimeCheck(ctx); err != nil {
			return fmt.Errorf("restore: restore func runtime check: %w", err)
		}
		ddlt.advance(ddlIndexes)
	}
	if ddlt.step == ddlIndexes {
		if err := ddlt.dropExistedIndex(ctx); err != nil {
			return fmt.Errorf("restore: drop exist index: %w", err)
		}
		if err := ddlt.createIndex(ctx); err != nil {
			return fmt.Errorf("restore: create index: %w", err)
		}
		ddlt.advance(ddlPartitions)
	}
	if err := ddlt.createPartitions(ctx); err != nil {
		return fmt.Errorf("restore: create partitions: %w", err)
	}
	ddlt.advance(ddlDone)
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
	plan, err := fieldid.BuildPlan(ddlt.collBackup.GetSchema())
	if err != nil {
		return nil, nil, fmt.Errorf("collection: build field plan: %w", err)
	}

	createFields, err := ddlt.convFields(plan.Create)
	if err != nil {
		return nil, nil, fmt.Errorf("collection: get fields: %w", err)
	}
	addFields, err := ddlt.convFields(plan.Add)
	if err != nil {
		return nil, nil, fmt.Errorf("collection: get fields: %w", err)
	}

	ddlt.logger.Info("fields", zap.Any("create_fields", createFields), zap.Any("add_fields", addFields))
	return createFields, addFields, nil
}

// checkFieldIDs refuses to go on when the target did not reproduce the source
// field ids. The backup's columns are stored under those ids, so a mismatch is
// silent corruption: every row would read another column's values. The check
// runs after create and add field, before any index, partition or import work,
// and it also judges a collection created outside restore.
//
// A collection without data and a meta_only restore are exempt: no row can be
// read from the wrong slot.
func (ddlt *collDDLTask) checkFieldIDs(ctx context.Context) error {
	if ddlt.option.MetaOnly || (!ddlt.savedHasData && !backupHasData(ddlt.collBackup)) {
		return nil
	}
	if err := fieldid.Verify(ctx, ddlt.grpcCli, ddlt.target, ddlt.collBackup.GetSchema()); err != nil {
		return fmt.Errorf("restore: verify target field ids: %w", err)
	}

	return nil
}

// backupHasData reports whether the collection carries rows whose columns are
// addressed by field id. Older snapshot metadata does not enumerate segments,
// so a bundle counts as data.
func backupHasData(coll *backuppb.CollectionBackupInfo) bool {
	if coll.GetSnapshotBackup() != nil || len(coll.GetL0Segments()) != 0 || coll.GetSize() > 0 {
		return true
	}
	for _, part := range coll.GetPartitionBackups() {
		if len(part.GetSegmentBackups()) != 0 {
			return true
		}
	}

	return false
}

// Compare the planned AddField requests before issuing any writes.
func missingFields(planned, existing []*schemapb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	byName := lo.KeyBy(existing, (*schemapb.FieldSchema).GetName)
	byID := lo.KeyBy(existing, (*schemapb.FieldSchema).GetFieldID)
	var missing []*schemapb.FieldSchema
	for _, field := range planned {
		got := byName[field.GetName()]
		if got == nil && byID[field.GetFieldID()] == nil {
			missing = append(missing, field)
			continue
		}
		if got == nil || !sameFieldDefinition(got, field) {
			return nil, fmt.Errorf("restore: target field conflicts with planned field %s id %d", field.GetName(), field.GetFieldID())
		}
	}
	return missing, nil
}

// Resume identifies an already added field using the attributes older targets report.
func sameFieldDefinition(a, b *schemapb.FieldSchema) bool {
	return a.Name == b.Name && a.FieldID == b.FieldID &&
		a.DataType == b.DataType && a.ElementType == b.ElementType
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
	props := pbconv.BakKVToMilvusKV(ddlt.collBackup.GetSchema().GetProperties(), ddlt.option.SkipParams.CollectionProperties...)

	if len(ddlt.collBackup.GetSchema().GetFunctions()) == 0 {
		ddlt.logger.Info("no functions, skip disable_auto_function")
		return props
	}

	if !ddlt.grpcCli.HasFeature(milvus.FuncRuntimeCheck) {
		ddlt.logger.Info("milvus does not support func runtime check, skip disable")
		return props
	}

	ddlt.logger.Info("milvus support func runtime check, disable it")
	// remove original disable_func_runtime_check
	props = lo.Filter(props, func(item *commonpb.KeyValuePair, _ int) bool {
		return item.GetKey() != common.DisableFuncRuntimeCheck
	})
	props = append(props, &commonpb.KeyValuePair{Key: common.DisableFuncRuntimeCheck, Value: "true"})
	return props
}

// restoreFuncRuntimeCheck restores the disable_func_runtime_check property to original value from backup.
// properties function add disable_auto_function to properties, we need to restore the original value from backup.
func (ddlt *collDDLTask) restoreFuncRuntimeCheck(ctx context.Context) error {
	if len(ddlt.collBackup.GetSchema().GetFunctions()) == 0 {
		ddlt.logger.Info("no functions, skip restore func runtime check")
		return nil
	}

	if !ddlt.grpcCli.HasFeature(milvus.FuncRuntimeCheck) {
		ddlt.logger.Info("milvus does not support func runtime check, skip restore")
		return nil
	}

	ddlt.logger.Info("milvus support func runtime check, restore original value")
	val := "false"
	for _, kv := range ddlt.collBackup.GetSchema().GetProperties() {
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

func (ddlt *collDDLTask) resumeCreateColl(ctx context.Context, addFields []*schemapb.FieldSchema) error {
	savedID := ddlt.collectionID
	info, err := ddlt.checkOrBind(ctx)
	if err != nil {
		return err
	}
	if savedID == 0 {
		// The target was absent or dropped. A different writer creating it
		// before Resume would be indistinguishable from our own create.
		ddlt.logger.Warn("resuming with existing collection after create checkpoint without a recorded ID", zap.Int64("collection_id", ddlt.collectionID))
	}

	if len(addFields) > 0 && info.GetSchema() == nil {
		return fmt.Errorf("restore: target collection schema unavailable")
	}
	missing, err := missingFields(addFields, info.GetSchema().GetFields())
	if err != nil {
		return err
	}

	return ddlt.addFields(ctx, missing)
}

// createCollectionInput builds the CreateCollection request and the fields that
// follow it through AddField. It runs before any drop: converting the backup's
// schema is where corrupt metadata surfaces, and that must happen while the
// target collection still exists.
func (ddlt *collDDLTask) createCollectionInput() (milvus.CreateCollectionInput, []*schemapb.FieldSchema, error) {
	createFields, addFields, err := ddlt.fields()
	if err != nil {
		return milvus.CreateCollectionInput{}, nil, fmt.Errorf("collection: get fields: %w", err)
	}

	functions := conv.Functions(ddlt.collBackup.GetSchema().GetFunctions())
	structArrayFields, err := ddlt.structArrayFields()
	if err != nil {
		return milvus.CreateCollectionInput{}, nil, fmt.Errorf("restore: conv struct array fields: %w", err)
	}
	// CollectionSchema.AutoID is a deprecated proto field that Milvus proxy has zeroed
	// on entry of every supported target since 2.2, so the backup value is never
	// consumed there; the authoritative autoID is the per-field one in convFields.
	schema := &schemapb.CollectionSchema{
		Name:               ddlt.target.CollName(),
		Description:        ddlt.description(),
		Functions:          functions,
		Fields:             createFields,
		EnableDynamicField: ddlt.collBackup.GetSchema().GetEnableDynamicField(),
		Properties:         pbconv.BakKVToMilvusKV(ddlt.collBackup.GetSchema().GetProperties()),
		StructArrayFields:  structArrayFields,
	}
	ddlt.logger.Info("create collection", zap.Any("schema", schema))

	opt := milvus.CreateCollectionInput{
		DB:           ddlt.target.DBName(),
		Schema:       schema,
		ConsLevel:    commonpb.ConsistencyLevel(ddlt.collBackup.GetConsistencyLevel()),
		ShardNum:     ddlt.shardNum(),
		PartitionNum: ddlt.partitionNum(),
		Properties:   ddlt.properties(),
	}
	return opt, addFields, nil
}

func (ddlt *collDDLTask) createColl(ctx context.Context, opt milvus.CreateCollectionInput, addFields []*schemapb.FieldSchema) error {
	if ddlt.option.SkipCreateCollection {
		return nil
	}
	var err error
	var exists bool
	if ddlt.option.DropExistCollection || ddlt.option.EnableResume {
		exists, err = ddlt.grpcCli.HasCollection(ctx, ddlt.target.DBName(), ddlt.target.CollName())
		if err != nil {
			return fmt.Errorf("restore: check collection exists: %w", err)
		}
	}
	if ddlt.option.EnableResume {
		if exists && (ddlt.collectionID != 0 || ddlt.tracker.CreateTargetPrepared()) {
			return ddlt.resumeCreateColl(ctx, addFields)
		}
		if !exists && ddlt.collectionID != 0 {
			return fmt.Errorf("restore: bound collection %s (ID %d) no longer exists", ddlt.target, ddlt.collectionID)
		}
		if exists && !ddlt.option.DropExistCollection {
			return fmt.Errorf("restore: collection already exists: %s", ddlt.target)
		}
	}
	if ddlt.option.DropExistCollection && exists {
		ddlt.logger.Info("drop existing collection")
		if err := ddlt.grpcCli.DropCollection(ctx, ddlt.target.DBName(), ddlt.target.CollName()); err != nil {
			return fmt.Errorf("restore: drop existing collection: %w", err)
		}
	}
	if ddlt.option.EnableResume {
		ddlt.tracker.MarkCreateTargetPrepared()
	}

	if err := ddlt.grpcCli.CreateCollection(ctx, opt); err != nil {
		return fmt.Errorf("restore: create collection %s: %w", ddlt.target, err)
	}

	if ddlt.option.EnableResume {
		if _, err := ddlt.checkOrBind(ctx); err != nil {
			return err
		}
	}
	if err := ddlt.addFields(ctx, addFields); err != nil {
		return fmt.Errorf("restore: add fields: %w", err)
	}

	return nil
}

func (ddlt *collDDLTask) convFields(bakFields []*backuppb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	fields := make([]*schemapb.FieldSchema, 0, len(bakFields))

	for _, bakField := range bakFields {
		defaultValue, err := conv.DefaultValue(bakField)
		if err != nil {
			return nil, fmt.Errorf("restore: get default value: %w", err)
		}

		typeSchema, err := conv.TypeSchema(bakField)
		if err != nil {
			return nil, err
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
			ElementNullable:  bakField.GetElementNullable(),
			TypeSchema:       typeSchema,
		}
		// A recursive type schema carries type params at every level of the
		// nesting, so the skip list applies to the whole chain.
		for t := fieldRestore.GetTypeSchema(); t != nil; t = t.GetArrayElement() {
			t.TypeParams = filterParams(t.GetTypeParams(), ddlt.option.SkipParams.FieldTypeParams)
		}

		fields = append(fields, fieldRestore)
	}

	return fields, nil
}

// filterParams removes the skipped keys without mutating the input slice.
func filterParams(params []*commonpb.KeyValuePair, skip []string) []*commonpb.KeyValuePair {
	return slices.DeleteFunc(slices.Clone(params), func(kv *commonpb.KeyValuePair) bool {
		return slices.Contains(skip, kv.GetKey())
	})
}

func (ddlt *collDDLTask) structArrayFields() ([]*schemapb.StructArrayFieldSchema, error) {
	bakFields := ddlt.collBackup.GetSchema().GetStructArrayFields()
	structArrayFields := make([]*schemapb.StructArrayFieldSchema, 0, len(bakFields))
	for _, bakField := range bakFields {
		fields, err := ddlt.convFields(bakField.GetFields())
		if err != nil {
			return nil, fmt.Errorf("restore: convert struct array fields: %w", err)
		}

		structArrayField := &schemapb.StructArrayFieldSchema{
			FieldID:     bakField.GetFieldID(),
			Name:        bakField.GetName(),
			Description: bakField.GetDescription(),
			Fields:      fields,
			Nullable:    conv.ApplyStructNullable(bakField, fields),
		}

		structArrayFields = append(structArrayFields, structArrayField)
	}

	return structArrayFields, nil
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

// partitionNum returns the partition number of the collection
// if partition key was set, return the length of partition in backup
// else return 0.
func (ddlt *collDDLTask) partitionNum() int {
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
				params = maps.Clone(params)
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
