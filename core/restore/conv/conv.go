package conv

import (
	"encoding/base64"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func DefaultValue(field *backuppb.FieldSchema) (*schemapb.ValueField, error) {
	// try to use DefaultValueBase64 first
	if field.GetDefaultValueBase64() != "" {
		bytes, err := base64.StdEncoding.DecodeString(field.GetDefaultValueBase64())
		if err != nil {
			return nil, fmt.Errorf("restore: failed to decode default value base64: %w", err)
		}
		var defaultValue schemapb.ValueField
		if err := proto.Unmarshal(bytes, &defaultValue); err != nil {
			return nil, fmt.Errorf("restore: failed to unmarshal default value: %w", err)
		}
		return &defaultValue, nil
	}

	// backward compatibility
	if field.GetDefaultValueProto() != "" {
		var defaultValue schemapb.ValueField
		err := proto.Unmarshal([]byte(field.GetDefaultValueProto()), &defaultValue)
		if err != nil {
			return nil, fmt.Errorf("restore: failed to unmarshal default value: %w", err)
		}
		return &defaultValue, nil
	}

	return nil, nil
}

func Functions(bakFuncs []*backuppb.FunctionSchema) []*schemapb.FunctionSchema {
	funcs := make([]*schemapb.FunctionSchema, 0, len(bakFuncs))
	for _, bakFunc := range bakFuncs {
		fun := &schemapb.FunctionSchema{
			Name:             bakFunc.Name,
			Id:               bakFunc.Id,
			Description:      bakFunc.Description,
			Type:             schemapb.FunctionType(bakFunc.Type),
			InputFieldNames:  bakFunc.InputFieldNames,
			InputFieldIds:    bakFunc.InputFieldIds,
			OutputFieldNames: bakFunc.OutputFieldNames,
			OutputFieldIds:   bakFunc.OutputFieldIds,
			Params:           pbconv.BakKVToMilvusKV(bakFunc.Params),
		}
		funcs = append(funcs, fun)
	}

	return funcs
}

func Fields(bakFields []*backuppb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	fields := make([]*schemapb.FieldSchema, 0, len(bakFields))

	for _, bakField := range bakFields {
		defaultValue, err := DefaultValue(bakField)
		if err != nil {
			return nil, fmt.Errorf("conv: get default value: %w", err)
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

func StructArrayFields(bakFields []*backuppb.StructArrayFieldSchema) ([]*schemapb.StructArrayFieldSchema, error) {
	structArrayFields := make([]*schemapb.StructArrayFieldSchema, 0, len(bakFields))
	for _, bakField := range bakFields {
		fields, err := Fields(bakField.GetFields())
		if err != nil {
			return nil, fmt.Errorf("conv: convert struct array fields: %w", err)
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

func Schema(bakSchema *backuppb.CollectionSchema) (*schemapb.CollectionSchema, error) {
	fields, err := Fields(bakSchema.GetFields())
	if err != nil {
		return nil, fmt.Errorf("conv: conv fields: %w", err)
	}

	functions := Functions(bakSchema.GetFunctions())

	structArrayFields, err := StructArrayFields(bakSchema.GetStructArrayFields())
	if err != nil {
		return nil, fmt.Errorf("conv: conv struct array fields: %w", err)
	}

	properties := pbconv.BakKVToMilvusKV(bakSchema.GetProperties())

	schema := &schemapb.CollectionSchema{
		Name:               bakSchema.GetName(),
		Description:        bakSchema.GetDescription(),
		Functions:          functions,
		Fields:             fields,
		EnableDynamicField: bakSchema.GetEnableDynamicField(),
		Properties:         properties,
		StructArrayFields:  structArrayFields,
	}

	return schema, nil
}
