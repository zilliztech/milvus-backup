package backup

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

// fakeKV implements clientv3.KV but only supports prefix Get, which is all the
// index extra task needs. Any other method would panic via the embedded nil
// interface, surfacing accidental use.
type fakeKV struct {
	clientv3.KV

	data map[string][]byte
}

func (f *fakeKV) Get(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp := &clientv3.GetResponse{}
	for k, v := range f.data {
		if strings.HasPrefix(k, key) {
			resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{Key: []byte(k), Value: v})
		}
	}

	return resp, nil
}

func mustMarshalFieldIndex(t *testing.T, idx *indexpb.FieldIndex) []byte {
	data, err := proto.Marshal(idx)
	require.NoError(t, err)
	return data
}

func TestCollIndexExtraTaskExecute(t *testing.T) {
	const rootPath = "by-dev"

	newBuilder := func() *metaBuilder {
		builder := newMetaBuilder("task1", "backup1")
		builder.addCollection(namespace.New("db1", "coll1"), &backuppb.CollectionBackupInfo{
			CollectionId:   1,
			DbName:         "db1",
			CollectionName: "coll1",
			IndexInfos:     []*backuppb.IndexInfo{{IndexId: 100, FieldName: "vec", IndexName: "vec_idx"}},
		})

		return builder
	}

	t.Run("ScopesToBackupCollections", func(t *testing.T) {
		builder := newBuilder()
		kv := &fakeKV{data: map[string][]byte{
			// backed-up collection, matching index -> merged
			fmt.Sprintf("%s/meta/field-index/1/100", rootPath): mustMarshalFieldIndex(t, &indexpb.FieldIndex{
				IndexInfo:  &indexpb.IndexInfo{CollectionID: 1, IndexID: 100, FieldID: 5},
				CreateTime: 123,
			}),
			// deleted index of the backed-up collection -> skipped
			fmt.Sprintf("%s/meta/field-index/1/101", rootPath): mustMarshalFieldIndex(t, &indexpb.FieldIndex{
				IndexInfo: &indexpb.IndexInfo{CollectionID: 1, IndexID: 101},
				Deleted:   true,
			}),
			// index of a collection NOT in this backup -> must not be fetched,
			// otherwise it would have no matching IndexInfo and fail.
			fmt.Sprintf("%s/meta/field-index/999/200", rootPath): mustMarshalFieldIndex(t, &indexpb.FieldIndex{
				IndexInfo: &indexpb.IndexInfo{CollectionID: 999, IndexID: 200},
			}),
		}}

		task := newCollIndexExtraTask("task1", kv, rootPath, builder)
		assert.NoError(t, task.Execute(context.Background()))

		got := builder.collectionBackups[1].GetIndexInfos()[0]
		assert.Equal(t, int64(5), got.GetFieldId())
		assert.Equal(t, uint64(123), got.GetCreateTime())
	})

	t.Run("MissingIndexInBackupReturnsError", func(t *testing.T) {
		builder := newBuilder()
		kv := &fakeKV{data: map[string][]byte{
			// backed-up collection, but index id is not in IndexInfos
			fmt.Sprintf("%s/meta/field-index/1/300", rootPath): mustMarshalFieldIndex(t, &indexpb.FieldIndex{
				IndexInfo: &indexpb.IndexInfo{CollectionID: 1, IndexID: 300},
			}),
		}}

		task := newCollIndexExtraTask("task1", kv, rootPath, builder)
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not in backup index infos")
	})
}
