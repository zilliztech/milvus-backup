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

	backupEZK bool

	meta   *meta.MetaManager
	grpc   milvus.Grpc
	manage milvus.Manage
}

func NewDatabaseTask(taskID, dbName string, backupEZK bool, grpc milvus.Grpc, manage milvus.Manage, meta *meta.MetaManager) *DatabaseTask {
	return &DatabaseTask{taskID: taskID, dbName: dbName, backupEZK: backupEZK, meta: meta, grpc: grpc, manage: manage}
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

	if dt.backupEZK {
		ezk, err := dt.manage.GetEZK(ctx, dt.dbName)
		if err != nil {
			return fmt.Errorf("backup: get ezk: %w", err)
		}
		bakDB.Ezk = ezk
	}

	dt.meta.UpdateBackup(dt.taskID, meta.AddDatabase(bakDB))

	return nil
}
