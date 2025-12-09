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

func TestTask_filterDBAndNSS(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		dbNames := []string{"db1", "db2"}
		nss := []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
			namespace.New("db2", "coll1"),
			namespace.New("db2", "coll2"),
		}
		task := &Task{option: Option{Filter: filter.Filter{}}}
		filteredDBNames, filteredNSS, err := task.filterDBAndNSS(dbNames, nss)
		assert.NoError(t, err)
		assert.ElementsMatch(t, dbNames, filteredDBNames)
		assert.ElementsMatch(t, nss, filteredNSS)
	})

	t.Run("Filter", func(t *testing.T) {
		dbNames := []string{"db1", "db2"}
		nss := []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
			namespace.New("db2", "coll1"),
			namespace.New("db2", "coll2"),
		}

		f := filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll1": {}}},
		}}
		task := &Task{option: Option{Filter: f}}
		filteredDBNames, filteredNSS, err := task.filterDBAndNSS(dbNames, nss)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1"}, filteredDBNames)
		assert.ElementsMatch(t, []namespace.NS{namespace.New("db1", "coll1")}, filteredNSS)
	})

	t.Run("FilterDBNotFound", func(t *testing.T) {
		dbNames := []string{"db1", "db2"}
		nss := []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db2", "coll1"),
		}

		f := filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db3": {CollName: map[string]struct{}{"coll1": {}}},
		}}

		task := &Task{option: Option{Filter: f}}
		_, _, err := task.filterDBAndNSS(dbNames, nss)
		assert.Error(t, err)
	})

	t.Run("FilterCollNotFound", func(t *testing.T) {
		dbNames := []string{"db1", "db2"}
		nss := []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
			namespace.New("db2", "coll1"),
			namespace.New("db2", "coll2"),
		}

		f := filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"db1": {CollName: map[string]struct{}{"coll3": {}}},
		}}
		task := &Task{option: Option{Filter: f}}
		_, _, err := task.filterDBAndNSS(dbNames, nss)
		assert.Error(t, err)
	})
}

func TestTask_listNS(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().ListCollections(mock.Anything, "db1").
		Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
		Once()

	task := &Task{grpc: cli, logger: zap.NewNop()}
	nss, err := task.listNS(context.Background(), "db1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []namespace.NS{
		namespace.New("db1", "coll1"),
		namespace.New("db1", "coll2"),
	}, nss)
}

func TestTask_listAllDBAndNSS(t *testing.T) {
	t.Run("SupportMultiDB", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.MultiDatabase).Return(true).Once()
		cli.EXPECT().ListCollections(mock.Anything, "default").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
			Once()
		cli.EXPECT().ListDatabases(mock.Anything).Return([]string{"default", "db1", "db2"}, nil).Once()
		cli.EXPECT().ListCollections(mock.Anything, "db1").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
			Once()
		cli.EXPECT().ListCollections(mock.Anything, "db2").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll3", "coll4"}}, nil).
			Once()
		task := &Task{grpc: cli, logger: zap.NewNop()}
		dbNames, nss, err := task.listAllDBAndNSS(context.Background())
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"default", "db1", "db2"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("default", "coll1"),
			namespace.New("default", "coll2"),
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
			namespace.New("db2", "coll3"),
			namespace.New("db2", "coll4"),
		}, nss)
	})

	t.Run("NotSupportMultiDB", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.MultiDatabase).Return(false).Once()
		cli.EXPECT().ListCollections(mock.Anything, "default").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
			Once()
		task := &Task{grpc: cli, logger: zap.NewNop()}
		dbNames, nss, err := task.listAllDBAndNSS(context.Background())
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"default"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("default", "coll1"),
			namespace.New("default", "coll2"),
		}, nss)
	})
}

func TestTask_listDBAndNSS(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.MultiDatabase).Return(true).Once()
		cli.EXPECT().ListDatabases(mock.Anything).Return([]string{"default", "db1"}, nil).Once()
		cli.EXPECT().ListCollections(mock.Anything, "default").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
			Once()
		cli.EXPECT().ListCollections(mock.Anything, "db1").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
			Once()
		task := &Task{grpc: cli, logger: zap.NewNop()}
		dbNames, nss, err := task.listDBAndNSS(context.Background())
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"default", "db1"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("default", "coll1"),
			namespace.New("default", "coll2"),
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
		}, nss)
	})

	t.Run("WithFilter", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.MultiDatabase).Return(true).Once()
		cli.EXPECT().ListDatabases(mock.Anything).Return([]string{"default", "db1"}, nil).Once()
		cli.EXPECT().ListCollections(mock.Anything, "default").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
			Once()
		cli.EXPECT().ListCollections(mock.Anything, "db1").
			Return(&milvuspb.ShowCollectionsResponse{CollectionNames: []string{"coll1", "coll2"}}, nil).
			Once()

		f := filter.Filter{DBCollFilter: map[string]filter.CollFilter{
			"default": {CollName: map[string]struct{}{"coll1": {}}},
			"db1":     {CollName: map[string]struct{}{"coll2": {}}},
		}}
		task := &Task{grpc: cli, logger: zap.NewNop(), option: Option{Filter: f}}
		dbNames, nss, err := task.listDBAndNSS(context.Background())
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"default", "db1"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("default", "coll1"),
			namespace.New("db1", "coll2"),
		}, nss)
	})
}
