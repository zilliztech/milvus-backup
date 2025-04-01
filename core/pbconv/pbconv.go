package pbconv

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/samber/lo"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
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
