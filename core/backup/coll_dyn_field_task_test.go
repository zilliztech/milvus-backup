package backup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

func mustMarshalFieldSchema(t *testing.T, field *schemapb.FieldSchema) []byte {
	data, err := proto.Marshal(field)
	require.NoError(t, err)
	return data
}

func TestCollDynFieldTaskExecute(t *testing.T) {
	const rootPath = "by-dev"

	newBuilder := func() *metaBuilder {
		builder := newMetaBuilder("task1", "backup1")
		builder.addCollection(collref.New("db1", "coll1"), &backuppb.CollectionBackupInfo{
			CollectionId:   1,
			DbName:         "db1",
			CollectionName: "coll1",
			Schema:         &backuppb.CollectionSchema{EnableDynamicField: true},
		})

		return builder
	}

	// The field key is "root-coord/fields/{collectionID}/{fieldID}", so the scan
	// is one read per backed-up collection. A collection outside the backup must
	// not be read at all: its fields would be fetched and discarded, and on a
	// large instance that is most of what comes back.
	t.Run("ScopesToBackupCollections", func(t *testing.T) {
		builder := newBuilder()
		kv := &fakeKV{data: map[string][]byte{
			fmt.Sprintf("%s/meta/root-coord/fields/1/100", rootPath): mustMarshalFieldSchema(t, &schemapb.FieldSchema{
				FieldID: 100, Name: "id",
			}),
			fmt.Sprintf("%s/meta/root-coord/fields/1/101", rootPath): mustMarshalFieldSchema(t, &schemapb.FieldSchema{
				FieldID: 101, Name: "$meta", IsDynamic: true, Nullable: true,
			}),
			// Not in this backup. Reading it would be waste, and its dynamic
			// field must not reach the meta builder.
			fmt.Sprintf("%s/meta/root-coord/fields/2/101", rootPath): mustMarshalFieldSchema(t, &schemapb.FieldSchema{
				FieldID: 101, Name: "$meta", IsDynamic: true,
			}),
		}}

		task := newCollDynFieldTask("task1", newEtcdMeta(kv, []string{"127.0.0.1:2379"}), rootPath, builder)
		require.NoError(t, task.Execute(context.Background()))

		require.Len(t, kv.gotKeys, 1)
		assert.Equal(t, fmt.Sprintf("%s/meta/root-coord/fields/1/", rootPath), kv.gotKeys[0])

		colls := builder.data.GetCollectionBackups()
		require.Len(t, colls, 1)
		fields := colls[0].GetSchema().GetFields()
		require.Len(t, fields, 1)
		assert.Equal(t, "$meta", fields[0].GetName())
		assert.True(t, fields[0].GetNullable())
	})

	t.Run("UnreachableEtcdNamesEndpoint", func(t *testing.T) {
		etcd := newEtcdMeta(&blockingEtcd{}, []string{"127.0.0.1:2379"})
		etcd.timeout = 100 * time.Millisecond

		task := newCollDynFieldTask("task1", etcd, rootPath, newBuilder())
		err := task.Execute(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "127.0.0.1:2379")
	})
}
