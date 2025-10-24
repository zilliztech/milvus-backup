package pbconv

import (
	"encoding/base64"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func BakKVToMilvusKV(kv []*backuppb.KeyValuePair, skipKeys ...string) []*commonpb.KeyValuePair {
	skip := lo.SliceToMap(skipKeys, func(item string) (string, struct{}) {
		return item, struct{}{}
	})

	return lo.FilterMap(kv, func(item *backuppb.KeyValuePair, i int) (*commonpb.KeyValuePair, bool) {
		if _, ok := skip[item.GetKey()]; ok {
			return nil, false
		}

		return &commonpb.KeyValuePair{Key: item.GetKey(), Value: item.GetValue()}, true
	})
}

func MilvusKVToBakKV(kv []*commonpb.KeyValuePair) []*backuppb.KeyValuePair {
	return lo.Map(kv, func(item *commonpb.KeyValuePair, _ int) *backuppb.KeyValuePair {
		return &backuppb.KeyValuePair{
			Key:   item.GetKey(),
			Value: item.GetValue(),
		}
	})
}

func MilvusKVToMap(kvs []*commonpb.KeyValuePair) map[string]string {
	res := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		res[kv.GetKey()] = kv.GetValue()
	}
	return res
}

func RestoreCollTaskViewToResp(ns namespace.NS, taskView taskmgr.RestoreCollTaskView) *backuppb.RestoreCollectionTaskResponse {
	return &backuppb.RestoreCollectionTaskResponse{
		Id:                   taskView.ID(),
		StateCode:            taskView.StateCode(),
		ErrorMessage:         taskView.ErrorMessage(),
		StartTime:            taskView.StartTime().Unix(),
		EndTime:              taskView.EndTime().Unix(),
		Progress:             taskView.Progress(),
		TargetDbName:         ns.DBName(),
		TargetCollectionName: ns.CollName(),
	}
}

func RestoreTaskViewToResp(view taskmgr.RestoreTaskView) *backuppb.RestoreBackupTaskResponse {
	collTasks := view.CollTasks()
	collTaskResps := make([]*backuppb.RestoreCollectionTaskResponse, 0, len(collTasks))
	for ns, taskView := range collTasks {
		collTaskResps = append(collTaskResps, RestoreCollTaskViewToResp(ns, taskView))
	}

	return &backuppb.RestoreBackupTaskResponse{
		Id:                     view.ID(),
		StateCode:              view.StateCode(),
		ErrorMessage:           view.ErrorMessage(),
		StartTime:              view.StartTime().Unix(),
		EndTime:                view.EndTime().Unix(),
		Progress:               view.Progress(),
		CollectionRestoreTasks: collTaskResps,
	}
}

func Base64MsgPosition(position *msgpb.MsgPosition) (string, error) {
	positionByte, err := proto.Marshal(position)
	if err != nil {
		return "", fmt.Errorf("utils: encode msg position %w", err)
	}
	return base64.StdEncoding.EncodeToString(positionByte), nil
}

func Base64DecodeMsgPosition(position string) (*msgpb.MsgPosition, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(position)
	if err != nil {
		return nil, fmt.Errorf("utils: base64 decode msg position %w", err)
	}

	var msgPosition msgpb.MsgPosition
	if err = proto.Unmarshal(decodeBytes, &msgPosition); err != nil {
		return nil, fmt.Errorf("utils: unmarshal msg position %w", err)
	}
	return &msgPosition, nil
}
