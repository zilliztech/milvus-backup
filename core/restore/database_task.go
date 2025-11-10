package restore

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type databaseTask struct {
	grpcCli milvus.Grpc

	logger *zap.Logger

	targetName string
	dbBackup   *backuppb.DatabaseBackupInfo
}

func newDatabaseTask(grpc milvus.Grpc, dbBackup *backuppb.DatabaseBackupInfo, targetName string) *databaseTask {
	logger := log.L().With(zap.String("database_name", dbBackup.GetDbName()),
		zap.Int64("database_id", dbBackup.GetDbId()),
		zap.String("target_db_name", targetName))

	return &databaseTask{grpcCli: grpc, logger: logger, dbBackup: dbBackup, targetName: targetName}
}

func (dt *databaseTask) Execute(ctx context.Context) error {
	dt.logger.Info("create database")
	if err := dt.grpcCli.CreateDatabase(ctx, dt.targetName); err != nil {
		return fmt.Errorf("restore: create database %s: %w", dt.targetName, err)
	}

	// for now, we will not restore database properties
	return nil
}
