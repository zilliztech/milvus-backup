package utils

import (
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

func GetDBCollections(val *structpb.Value) string {
	if val == nil {
		return ""
	}
	switch val.Kind.(type) {
	case *structpb.Value_StringValue:
		strVal := val.GetStringValue()
		return strVal
	case *structpb.Value_StructValue:
		jsonStruct := val.GetStructValue()
		jsonStr, err := (&jsonpb.Marshaler{}).MarshalToString(jsonStruct)
		if err != nil {
			return ""
		}
		return jsonStr
	default:
		return ""
	}
}
