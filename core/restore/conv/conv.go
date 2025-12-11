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
