package backup

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

const _cipherEnabledKey = "cipher.enabled"

type databaseTask struct {
	taskID string

	dbName string

	metaBuilder *metaBuilder

	grpc   milvus.Grpc
	manage milvus.Manage

	logger *zap.Logger
}

func newDatabaseTask(taskID, dbName string, grpc milvus.Grpc, manage milvus.Manage, builder *metaBuilder) *databaseTask {
	return &databaseTask{
		taskID: taskID,

		dbName: dbName,

		metaBuilder: builder,

		grpc:   grpc,
		manage: manage,

		logger: log.With(zap.String("task_id", taskID), zap.String("db_name", dbName)),
	}
}

func (dt *databaseTask) cipherEnabled(properties []*backuppb.KeyValuePair) bool {
	for _, prop := range properties {
		if prop.GetKey() == _cipherEnabledKey && prop.GetValue() == "true" {
			return true
		}
	}

	return false
}

func (dt *databaseTask) Execute(ctx context.Context) error {
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

	if dt.cipherEnabled(properties) {
		dt.logger.Info("backup ezk")
		ezk, err := dt.manage.GetEZK(ctx, dt.dbName)
		if err != nil {
			return fmt.Errorf("backup: get ezk: %w", err)
		}
		dt.logger.Info("get ezk done", zap.Int("length", len(ezk)))
		bakDB.Ezk = ezk
	}

	dt.metaBuilder.addDatabase(bakDB)

	return nil
}
