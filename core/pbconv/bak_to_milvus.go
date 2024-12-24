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
		if _, ok := skip[item.Key]; ok {
			return nil, false
		}

		return &commonpb.KeyValuePair{Key: item.Key, Value: item.Value}, true
	})
}
