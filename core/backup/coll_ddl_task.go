package backup

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

type collDDLTask struct {
	taskID string

	ns namespace.NS

	grpc milvus.Grpc

	taskMgr     *taskmgr.Mgr
	metaBuilder *metaBuilder

	logger *zap.Logger
}

func newCollDDLTask(ns namespace.NS, args collTaskArgs) *collDDLTask {
	logger := log.L().With(zap.String("task_id", args.TaskID), zap.String("ns", ns.String()))

	return &collDDLTask{
		taskID:      args.TaskID,
		ns:          ns,
		grpc:        args.Grpc,
		taskMgr:     args.TaskMgr,
		metaBuilder: args.MetaBuilder,
		logger:      logger,
	}
}

func (ddlt *collDDLTask) convFields(fields []*schemapb.FieldSchema) ([]*backuppb.FieldSchema, error) {
	bakFields := make([]*backuppb.FieldSchema, 0, len(fields))
	for _, field := range fields {
		var defaultValueBase64 string
		if field.GetDefaultValue() != nil {
			bytes, err := proto.Marshal(field.GetDefaultValue())
			if err != nil {
				return nil, fmt.Errorf("backup: marshal default value")
			}
			defaultValueBase64 = base64.StdEncoding.EncodeToString(bytes)
		}

		f := &backuppb.FieldSchema{
			FieldID:            field.GetFieldID(),
			Name:               field.GetName(),
			IsPrimaryKey:       field.GetIsPrimaryKey(),
			Description:        field.GetDescription(),
			AutoID:             field.GetAutoID(),
			DataType:           backuppb.DataType(field.GetDataType()),
			TypeParams:         pbconv.MilvusKVToBakKV(field.GetTypeParams()),
			IndexParams:        pbconv.MilvusKVToBakKV(field.GetIndexParams()),
			IsDynamic:          field.GetIsDynamic(),
			IsPartitionKey:     field.GetIsPartitionKey(),
			Nullable:           field.GetNullable(),
			ElementType:        backuppb.DataType(field.GetElementType()),
			IsFunctionOutput:   field.GetIsFunctionOutput(),
			DefaultValueBase64: defaultValueBase64,
		}
		bakFields = append(bakFields, f)
	}

	return bakFields, nil
}

func (ddlt *collDDLTask) convFunctions(funs []*schemapb.FunctionSchema) []*backuppb.FunctionSchema {
	bakFuns := make([]*backuppb.FunctionSchema, 0, len(funs))
	for _, function := range funs {
		functionBak := &backuppb.FunctionSchema{
			Name:             function.GetName(),
			Id:               function.GetId(),
			Description:      function.GetDescription(),
			Type:             backuppb.FunctionType(function.GetType()),
			InputFieldNames:  function.GetInputFieldNames(),
			InputFieldIds:    function.GetInputFieldIds(),
			OutputFieldNames: function.GetOutputFieldNames(),
			OutputFieldIds:   function.GetOutputFieldIds(),
			Params:           pbconv.MilvusKVToBakKV(function.GetParams()),
		}
		bakFuns = append(bakFuns, functionBak)
	}

	return bakFuns
}

func (ddlt *collDDLTask) convStructArrayFields(fieldSchemas []*schemapb.StructArrayFieldSchema) ([]*backuppb.StructArrayFieldSchema, error) {
	bakFields := make([]*backuppb.StructArrayFieldSchema, 0, len(fieldSchemas))
	for _, fieldSchema := range fieldSchemas {
		fields, err := ddlt.convFields(fieldSchema.GetFields())
		if err != nil {
			return nil, fmt.Errorf("backup: convert struct array fields %w", err)
		}

		bakField := &backuppb.StructArrayFieldSchema{
			FieldID:     fieldSchema.GetFieldID(),
			Name:        fieldSchema.GetName(),
			Description: fieldSchema.GetDescription(),
			Fields:      fields,
		}

		bakFields = append(bakFields, bakField)
	}

	return bakFields, nil
}

func (ddlt *collDDLTask) convSchema(schema *schemapb.CollectionSchema) (*backuppb.CollectionSchema, error) {
	fields, err := ddlt.convFields(schema.Fields)
	if err != nil {
		return nil, fmt.Errorf("backup: convert fields %w", err)
	}

	functions := ddlt.convFunctions(schema.Functions)

	structArrayFields, err := ddlt.convStructArrayFields(schema.StructArrayFields)
	if err != nil {
		return nil, fmt.Errorf("backup: convert struct array fields %w", err)
	}

	bakSchema := &backuppb.CollectionSchema{
		Name:               schema.GetName(),
		Description:        schema.GetDescription(),
		AutoID:             schema.GetAutoID(),
		Fields:             fields,
		Properties:         pbconv.MilvusKVToBakKV(schema.GetProperties()),
		EnableDynamicField: schema.GetEnableDynamicField(),
		Functions:          functions,
		StructArrayFields:  structArrayFields,
	}

	return bakSchema, nil
}

func (ddlt *collDDLTask) backupIndexes(ctx context.Context) ([]*backuppb.IndexInfo, error) {
	ddlt.logger.Info("start backup indexes of collection")
	indexes, err := ddlt.grpc.ListIndex(ctx, ddlt.ns.DBName(), ddlt.ns.CollName())
	if err != nil && !strings.Contains(err.Error(), "index not found") {
		return nil, fmt.Errorf("backup: list index %w", err)
	}
	ddlt.logger.Info("indexes of collection", zap.Any("indexes", indexes))

	bakIndexes := make([]*backuppb.IndexInfo, 0, len(indexes))
	for _, index := range indexes {
		params := pbconv.MilvusKVToMap(index.GetParams())
		bakIndex := &backuppb.IndexInfo{
			IndexId:   index.GetIndexID(),
			FieldName: index.GetFieldName(),
			IndexName: index.GetIndexName(),
			IndexType: params["index_type"],
			Params:    params,
		}
		bakIndexes = append(bakIndexes, bakIndex)
	}

	return bakIndexes, nil
}

func (ddlt *collDDLTask) getPartLoadState(ctx context.Context, collLoadState string, partitionNames []string) (map[string]string, error) {
	partLoadState := make(map[string]string, len(partitionNames))
	// if the collection is loaded or not loaded, means all partitions are loaded or not loaded
	if collLoadState == meta.LoadStateLoaded || collLoadState == meta.LoadStateNotload {
		for _, partitionName := range partitionNames {
			partLoadState[partitionName] = collLoadState
		}

		return partLoadState, nil
	}

	for _, partName := range partitionNames {
		progress, err := ddlt.grpc.GetLoadingProgress(ctx, ddlt.ns.DBName(), partName)
		if err != nil {
			return nil, fmt.Errorf("backup: get loading progress %w", err)
		}

		switch progress {
		case 0:
			partLoadState[partName] = meta.LoadStateNotload
		case 100:
			partLoadState[partName] = meta.LoadStateLoaded
		default:
			partLoadState[partName] = meta.LoadStateLoading
		}
	}

	return partLoadState, nil
}

func (ddlt *collDDLTask) getCollLoadState(ctx context.Context) (string, error) {
	progress, err := ddlt.grpc.GetLoadingProgress(ctx, ddlt.ns.DBName(), ddlt.ns.CollName())
	if err != nil {
		return "", fmt.Errorf("backup: get loading progress %w", err)
	}

	switch progress {
	case 0:
		return meta.LoadStateNotload, nil
	case 100:
		return meta.LoadStateLoaded, nil
	default:
		return meta.LoadStateLoading, nil
	}
}

func (ddlt *collDDLTask) backupPartitionDDL(ctx context.Context, collID int64, collLoadState string) ([]*backuppb.PartitionBackupInfo, error) {
	ddlt.logger.Info("start backup partition ddl of collection")

	resp, err := ddlt.grpc.ShowPartitions(ctx, ddlt.ns.DBName(), ddlt.ns.CollName())
	if err != nil {
		return nil, fmt.Errorf("backup: show partitions %w", err)
	}
	ddlt.logger.Info("partitions of collection", zap.Strings("partitions", resp.PartitionNames))

	nameLen := len(resp.GetPartitionNames())
	idLen := len(resp.GetPartitionIDs())
	if nameLen != idLen {
		return nil, fmt.Errorf("backup: partition ids len = %d and names len = %d len not match", idLen, nameLen)
	}

	loadState, err := ddlt.getPartLoadState(ctx, collLoadState, resp.GetPartitionNames())
	if err != nil {
		return nil, fmt.Errorf("backup: get partition load state %w", err)
	}
	bakPartitions := make([]*backuppb.PartitionBackupInfo, 0, nameLen)
	for idx, id := range resp.GetPartitionIDs() {
		bakPartition := &backuppb.PartitionBackupInfo{
			PartitionId:   id,
			PartitionName: resp.GetPartitionNames()[idx],
			CollectionId:  collID,
			LoadState:     loadState[resp.GetPartitionNames()[idx]],
		}
		bakPartitions = append(bakPartitions, bakPartition)
	}

	return bakPartitions, nil
}

// Execute collects the collection DDL info, including schema, index and partition info.
// The segment info is not collected here, will be collect later in DML task.
func (ddlt *collDDLTask) Execute(ctx context.Context) error {
	ddlt.logger.Info("start to backup ddl of collection")

	ddlt.taskMgr.UpdateBackupTask(ddlt.taskID, taskmgr.SetBackupCollDDLExecuting(ddlt.ns))

	descResp, err := ddlt.grpc.DescribeCollection(ctx, ddlt.ns.DBName(), ddlt.ns.CollName())
	if err != nil {
		return fmt.Errorf("backup: describe collection %w", err)
	}
	ddlt.logger.Info("describe collection", zap.Any("resp", descResp))

	schema, err := ddlt.convSchema(descResp.GetSchema())
	if err != nil {
		return fmt.Errorf("backup: convert schema %w", err)
	}
	indexes, err := ddlt.backupIndexes(ctx)
	if err != nil {
		return fmt.Errorf("backup: backup indexes %w", err)
	}
	collLoadState, err := ddlt.getCollLoadState(ctx)
	if err != nil {
		return fmt.Errorf("backup: get collection load state %w", err)
	}
	partitions, err := ddlt.backupPartitionDDL(ctx, descResp.CollectionID, collLoadState)
	if err != nil {
		return fmt.Errorf("backup: backup partition ddl %w", err)
	}
	collBackup := &backuppb.CollectionBackupInfo{
		Id:                   ddlt.taskID,
		CollectionId:         descResp.GetCollectionID(),
		DbName:               descResp.GetDbName(),
		CollectionName:       descResp.GetCollectionName(),
		Schema:               schema,
		ShardsNum:            descResp.GetShardsNum(),
		ConsistencyLevel:     backuppb.ConsistencyLevel(descResp.ConsistencyLevel),
		HasIndex:             len(indexes) > 0,
		IndexInfos:           indexes,
		LoadState:            collLoadState,
		PartitionBackups:     partitions,
		Properties:           pbconv.MilvusKVToBakKV(descResp.GetProperties()),
		DbId:                 descResp.GetDbId(),
		CreatedTimestamp:     descResp.GetCreatedTimestamp(),
		VirtualChannelNames:  descResp.GetVirtualChannelNames(),
		PhysicalChannelNames: descResp.GetPhysicalChannelNames(),
		Aliases:              descResp.GetAliases(),
	}

	ddlt.metaBuilder.addCollection(ddlt.ns, collBackup)
	ddlt.taskMgr.UpdateBackupTask(ddlt.taskID, taskmgr.SetBackupCollDDLDone(ddlt.ns))

	ddlt.logger.Info("backup ddl of collection done")

	return nil
}
