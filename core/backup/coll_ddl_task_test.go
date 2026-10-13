package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func newTestCollDDLTask() *collDDLTask { return &collDDLTask{logger: zap.NewNop()} }

func TestCollDDLTask_convSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:               "name",
		Description:        "description",
		AutoID:             true,
		EnableDynamicField: true,
		Properties:         []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}},
	}

	ct := newTestCollDDLTask()
	bakSchema, err := ct.convSchema(schema)
	assert.NoError(t, err)
	assert.Equal(t, schema.GetName(), bakSchema.GetName())
	assert.Equal(t, schema.GetDescription(), bakSchema.GetDescription())
	assert.Equal(t, schema.GetAutoID(), bakSchema.GetAutoID()) //nolint:staticcheck // SA1019: testing deprecated field
	assert.Equal(t, schema.GetEnableDynamicField(), bakSchema.GetEnableDynamicField())
	assert.ElementsMatch(t, schema.GetProperties(), pbconv.BakKVToMilvusKV(bakSchema.GetProperties()))
}

func TestCollDDLTask_Execute(t *testing.T) {
	collRef := collref.New("db1", "coll1")

	newArgs := func(t *testing.T, cli milvus.Grpc) (collTaskArgs, *jobstate.BackupTracker, *metaBuilder) {
		mgr := jobstate.NewStore()
		tracker, err := mgr.AddBackupTask("task-1", "mybackup")
		require.NoError(t, err)

		builder := newMetaBuilder("task-1", "mybackup")

		return collTaskArgs{TaskID: "task-1", tracker: tracker, MetaBuilder: builder, Grpc: cli}, tracker, builder
	}

	t.Run("SettlesSuccess", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().DescribeCollection(mock.Anything, "db1", "coll1").
			Return(&milvuspb.DescribeCollectionResponse{
				CollectionID:   1,
				DbName:         "db1",
				CollectionName: "coll1",
				Schema:         &schemapb.CollectionSchema{Name: "coll1"},
				ShardsNum:      1,
			}, nil)
		cli.EXPECT().ListIndex(mock.Anything, "db1", "coll1").Return(nil, nil)
		cli.EXPECT().GetLoadingProgress(mock.Anything, "db1", "coll1").Return(int64(100), nil)
		cli.EXPECT().ShowPartitions(mock.Anything, "db1", "coll1").
			Return(&milvuspb.ShowPartitionsResponse{PartitionNames: []string{"_default"}, PartitionIDs: []int64{10}}, nil)

		args, tracker, builder := newArgs(t, cli)
		require.NoError(t, newCollDDLTask(collRef, args).Execute(context.Background()))

		require.Len(t, builder.data.GetCollectionBackups(), 1)
		ddl := tracker.Snapshot().Colls[collRef].DDL
		require.NotNil(t, ddl)
		assert.Equal(t, jobstate.BackupCollDDLStateSuccess, ddl.State)
		assert.False(t, ddl.EndTime.IsZero())
	})

	t.Run("SettlesFail", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().DescribeCollection(mock.Anything, "db1", "coll1").Return(nil, errors.New("boom"))

		args, tracker, _ := newArgs(t, cli)
		err := newCollDDLTask(collRef, args).Execute(context.Background())
		assert.ErrorContains(t, err, "boom")

		ddl := tracker.Snapshot().Colls[collRef].DDL
		require.NotNil(t, ddl)
		assert.Equal(t, jobstate.BackupCollDDLStateFail, ddl.State)
		assert.Contains(t, ddl.ErrorMessage, "boom")
	})
}
