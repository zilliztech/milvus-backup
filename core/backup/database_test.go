package backup

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func TestDatabaseTask_Execute(t *testing.T) {
	t.Run("SupportDescribeDatabase", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)

		cli.EXPECT().HasFeature(milvus.DescribeDatabase).Return(true).Once()
		cli.EXPECT().DescribeDatabase(mock.Anything, "db1").Return(&milvuspb.DescribeDatabaseResponse{
			DbID: 1,
			Properties: []*commonpb.KeyValuePair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			},
		}, nil).Once()

		metaMgr := meta.NewMetaManager()
		metaMgr.AddBackup(&backuppb.BackupInfo{Id: "backup1"})

		task := NewDatabaseTask("backup1", "db1", cli, metaMgr)
		err := task.Execute(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, int64(1), metaMgr.GetBackup("backup1").GetDatabaseBackups()[0].GetDbId())
		assert.Equal(t, "db1", metaMgr.GetBackup("backup1").GetDatabaseBackups()[0].GetDbName())
		assert.Equal(t, 2, len(metaMgr.GetBackup("backup1").GetDatabaseBackups()[0].GetProperties()))
	})

	t.Run("NotSupportDescribeDatabase", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)

		cli.EXPECT().HasFeature(milvus.DescribeDatabase).Return(false).Once()

		metaMgr := meta.NewMetaManager()
		metaMgr.AddBackup(&backuppb.BackupInfo{Id: "backup1"})

		task := NewDatabaseTask("backup1", namespace.DefaultDBName, cli, metaMgr)
		err := task.Execute(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, int64(0), metaMgr.GetBackup("backup1").GetDatabaseBackups()[0].GetDbId())
		assert.Equal(t, namespace.DefaultDBName, metaMgr.GetBackup("backup1").GetDatabaseBackups()[0].GetDbName())
		assert.Empty(t, metaMgr.GetBackup("backup1").GetDatabaseBackups()[0].GetProperties())
	})
}
