package backup

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

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

		task := NewDatabaseTask("backup1", "db1", false, cli, manage, builder)
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

		task := NewDatabaseTask("backup1", namespace.DefaultDBName, false, cli, manage, builder)
		err := task.Execute(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, int64(0), builder.data.DatabaseBackups[0].GetDbId())
		assert.Equal(t, namespace.DefaultDBName, builder.data.DatabaseBackups[0].GetDbName())
		assert.Empty(t, builder.data.DatabaseBackups[0].GetProperties())
	})

	t.Run("BackupEZK", func(t *testing.T) {
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

		manage.EXPECT().GetEZK(mock.Anything, "db1").Return("ezk1", nil).Once()

		builder := newMetaBuilder("task1", "backup1")

		task := NewDatabaseTask("backup1", "db1", true, cli, manage, builder)
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "ezk1", builder.data.DatabaseBackups[0].GetEzk())
	})

	t.Run("NotBackupEZK", func(t *testing.T) {
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

		task := NewDatabaseTask("backup1", "db1", false, cli, manage, builder)
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "", builder.data.DatabaseBackups[0].GetEzk())
	})
}
