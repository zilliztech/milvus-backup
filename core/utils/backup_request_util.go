package utils

import (
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func GetCreateDBCollections(request *backuppb.CreateBackupRequest) string {
	fieldValue := request.GetDbCollections()
	if fieldValue == nil {
		return ""
	}
	switch fieldValue.Kind.(type) {
	case *structpb.Value_StringValue:
		strVal := fieldValue.GetStringValue()
		return strVal
	case *structpb.Value_StructValue:
		jsonStruct := fieldValue.GetStructValue()
		jsonStr, err := (&jsonpb.Marshaler{}).MarshalToString(jsonStruct)
		if err != nil {
			return ""
		}
		return jsonStr
	default:
		return ""
	}
}

func GetRestoreDBCollections(request *backuppb.RestoreBackupRequest) string {
	fieldValue := request.GetDbCollections()
	if fieldValue == nil {
		return ""
	}
	switch fieldValue.Kind.(type) {
	case *structpb.Value_StringValue:
		strVal := fieldValue.GetStringValue()
		return strVal
	case *structpb.Value_StructValue:
		jsonStruct := fieldValue.GetStructValue()
		jsonStr, err := (&jsonpb.Marshaler{}).MarshalToString(jsonStruct)
		if err != nil {
			return ""
		}
		return jsonStr
	default:
		return ""
	}
}

func WrapDBCollections(dbCollections string) *structpb.Value {
	return &structpb.Value{
		Kind: &structpb.Value_StringValue{
			StringValue: dbCollections,
		},
	}
}
