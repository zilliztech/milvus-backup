package backup

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestTask_runRBACTask(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().BackupRBAC(mock.Anything).Return(&milvuspb.BackupRBACMetaResponse{}, nil).Once()

		task := &Task{
			option:      Option{BackupRBAC: true},
			logger:      zap.NewNop(),
			taskID:      "backup1",
			metaBuilder: newMetaBuilder("task1", "backup1"),
			grpc:        cli,
		}
		err := task.backupRBAC(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Skip", func(t *testing.T) {
		task := &Task{logger: zap.NewNop()}
		err := task.backupRBAC(context.Background())
		assert.NoError(t, err)
	})
}

func TestTask_listDBAndNSS(t *testing.T) {
	ctx := context.Background()

	t.Run("NoFilterSupportMultiDB", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasFeature(milvus.MultiDatabase).Return(true)
		mockGrpc.EXPECT().ListDatabases(ctx).Return([]string{"db1", "db2"}, nil)
		mockGrpc.EXPECT().ListCollections(ctx, "db1").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1", "coll2"},
		}, nil)
		mockGrpc.EXPECT().ListCollections(ctx, "db2").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll3"},
		}, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1", "db2"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
			namespace.New("db2", "coll3"),
		}, nss)
	})

	t.Run("NoFilterNotSupportMultiDB", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasFeature(milvus.MultiDatabase).Return(false)
		mockGrpc.EXPECT().ListCollections(ctx, "default").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1", "coll2"},
		}, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"default"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("default", "coll1"),
			namespace.New("default", "coll2"),
		}, nss)
	})

	t.Run("DBPattern", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().ListCollections(ctx, "db1").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1", "coll2"},
		}, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{
				DBCollFilter: map[string]filter.CollFilter{
					"db1": {AllowAll: true},
				},
			}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
		}, nss)
	})

	t.Run("TargetedPattern", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasCollection(ctx, "db1", "coll1").Return(true, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{
				DBCollFilter: map[string]filter.CollFilter{
					"db1": {CollName: map[string]struct{}{"coll1": {}}},
				},
			}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("db1", "coll1"),
		}, nss)
	})

	t.Run("CollectionNotFound", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasCollection(ctx, "db1", "not_exist").Return(false, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{
				DBCollFilter: map[string]filter.CollFilter{
					"db1": {CollName: map[string]struct{}{"not_exist": {}}},
				},
			}},
		}

		_, _, err := task.listDBAndNSS(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "filter collection db1.not_exist not found")
	})
}
