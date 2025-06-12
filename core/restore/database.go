package restore

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type DatabaseTask struct {
	grpcCli milvus.Grpc

	logger *zap.Logger

	task *backuppb.RestoreDatabaseTask
}

func NewDatabaseTask(grpc milvus.Grpc, task *backuppb.RestoreDatabaseTask) *DatabaseTask {
	logger := log.L().With(zap.String("database_name", task.GetDbBackup().GetDbName()),
		zap.Int64("database_id", task.GetDbBackup().GetDbId()),
		zap.String("target_db_name", task.GetTargetDbName()))

	return &DatabaseTask{grpcCli: grpc, logger: logger, task: task}
}

func (dt *DatabaseTask) Execute(ctx context.Context) error {
	dt.logger.Info("create database")
	if err := dt.grpcCli.CreateDatabase(ctx, dt.task.GetTargetDbName()); err != nil {
		return fmt.Errorf("restore: create database %s: %w", dt.task.GetTargetDbName(), err)
	}

	// for now, we will not restore database properties
	return nil
}
