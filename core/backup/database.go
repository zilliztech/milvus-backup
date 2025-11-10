package backup

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

type DatabaseTask struct {
	taskID string

	dbName string

	meta *meta.MetaManager
	grpc milvus.Grpc
}

func NewDatabaseTask(taskID, dbName string, grpc milvus.Grpc, meta *meta.MetaManager) *DatabaseTask {
	return &DatabaseTask{taskID: taskID, dbName: dbName, meta: meta, grpc: grpc}
}

func (dt *DatabaseTask) Execute(ctx context.Context) error {
	var properties []*backuppb.KeyValuePair
	var dbID int64

	// if milvus does not support database, skip describe database
	if dt.grpc.HasFeature(milvus.DescribeDatabase) {
		resp, err := dt.grpc.DescribeDatabase(ctx, dt.dbName)
		if err != nil {
			return fmt.Errorf("backup: describe database %s: %w", dt.dbName, err)
		}
		properties = pbconv.MilvusKVToBakKV(resp.GetProperties())
		dbID = resp.GetDbID()
	}

	bakDB := &backuppb.DatabaseBackupInfo{DbName: dt.dbName, DbId: dbID, Properties: properties}
	dt.meta.UpdateBackup(dt.taskID, meta.AddDatabase(bakDB))

	return nil
}
