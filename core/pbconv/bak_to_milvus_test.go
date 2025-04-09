package pbconv

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestBakKVToMilvusKV(t *testing.T) {
	kvs := []*backuppb.KeyValuePair{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	expect := []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	res := BakKVToMilvusKV(kvs)
	assert.Len(t, res, len(expect))
	assert.ElementsMatch(t, res, expect)

	skip := []string{"key1"}
	expect = []*commonpb.KeyValuePair{{Key: "key2", Value: "value2"}}
	res = BakKVToMilvusKV(kvs, skip...)
	assert.Len(t, res, len(kvs)-len(skip))
	assert.ElementsMatch(t, res, expect)
}
