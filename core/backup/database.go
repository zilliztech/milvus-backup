package backup

import (
	"context"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/pbconv"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type DatabaseTask struct {
	backupID string

	dbName string

	meta *meta.MetaManager
	grpc client.Grpc
}

func NewDatabaseTask(backupID, dbName string, grpc client.Grpc, meta *meta.MetaManager) *DatabaseTask {
	return &DatabaseTask{backupID: backupID, dbName: dbName, meta: meta, grpc: grpc}
}

func (dt *DatabaseTask) Execute(ctx context.Context) error {
	var properties []*backuppb.KeyValuePair
	var dbID int64

	// if milvus does not support database, skip describe database
	if dt.grpc.SupportMultiDatabase() {
		resp, err := dt.grpc.DescribeDatabase(ctx, dt.dbName)
		if err != nil {
			return fmt.Errorf("backup: describe database %s: %w", dt.dbName, err)
		}
		properties = pbconv.MilvusKVToBakKV(resp.GetProperties())
		dbID = resp.GetDbID()
	}

	bakDB := &backuppb.DatabaseBackupInfo{DbName: dt.dbName, DbId: dbID, Properties: properties}
	dt.meta.UpdateBackup(dt.backupID, meta.AddDatabase(bakDB))

	return nil
}
