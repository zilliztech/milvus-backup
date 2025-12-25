package backup

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestDatabaseTask_Execute(t *testing.T) {
	t.Run("SupportDescribeDatabase", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		manage := milvus.NewMockManage(t)

		cli.EXPECT().HasFeature(milvus.DescribeDatabase).Return(true).Once()
		cli.EXPECT().DescribeDatabase(mock.Anything, "db1").Return(&milvuspb.DescribeDatabaseResponse{
			DbID: 1,
			Properties: []*commonpb.KeyValuePair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			},
		}, nil).Once()

		builder := newMetaBuilder("task1", "backup1")

		task := newDatabaseTask("backup1", "db1", cli, manage, builder)
		err := task.Execute(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, int64(1), builder.data.DatabaseBackups[0].GetDbId())
		assert.Equal(t, "db1", builder.data.DatabaseBackups[0].GetDbName())
		assert.Equal(t, 2, len(builder.data.DatabaseBackups[0].GetProperties()))
	})

	t.Run("NotSupportDescribeDatabase", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		manage := milvus.NewMockManage(t)

		cli.EXPECT().HasFeature(milvus.DescribeDatabase).Return(false).Once()

		builder := newMetaBuilder("task1", "backup1")

		task := newDatabaseTask("backup1", namespace.DefaultDBName, cli, manage, builder)
		err := task.Execute(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, int64(0), builder.data.DatabaseBackups[0].GetDbId())
		assert.Equal(t, namespace.DefaultDBName, builder.data.DatabaseBackups[0].GetDbName())
		assert.Empty(t, builder.data.DatabaseBackups[0].GetProperties())
	})
}

func TestDatabaseTask_cipherEnabled(t *testing.T) {
	task := &databaseTask{}

	t.Run("Enabled", func(t *testing.T) {
		assert.True(t, task.cipherEnabled([]*backuppb.KeyValuePair{
			{Key: _cipherEnabledKey, Value: "true"},
		}))
	})

	t.Run("Disabled", func(t *testing.T) {
		assert.False(t, task.cipherEnabled([]*backuppb.KeyValuePair{
			{Key: _cipherEnabledKey, Value: "false"},
		}))
	})

	t.Run("NotSet", func(t *testing.T) {
		assert.False(t, task.cipherEnabled([]*backuppb.KeyValuePair{
			{Key: "other", Value: "true"},
		}))
	})
}
