package secondary

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

type databaseTask struct {
	taskID string

	backupInfo *backuppb.BackupInfo
	dbBackup   *backuppb.DatabaseBackupInfo

	streamCli milvus.Stream
	logger    *zap.Logger
}

func newDatabaseTask(taskID string, backupInfo *backuppb.BackupInfo, dbBackup *backuppb.DatabaseBackupInfo, streamCli milvus.Stream) (*databaseTask, error) {
	task := &databaseTask{
		taskID: taskID,

		backupInfo: backupInfo,
		dbBackup:   dbBackup,

		streamCli: streamCli,
		logger:    log.With(zap.String("task_id", taskID)),
	}

	return task, nil
}

func (dbt *databaseTask) Execute(_ context.Context) error {
	header := &message.CreateDatabaseMessageHeader{
		DbName: dbt.dbBackup.GetDbName(),
		DbId:   dbt.dbBackup.GetDbId(),
	}

	body := &message.CreateDatabaseMessageBody{
		Properties: pbconv.BakKVToMilvusKV(dbt.dbBackup.GetProperties()),
	}

	builder := message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast([]string{dbt.backupInfo.GetControlChannelName()})

	broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
	msgs := broadcast.SplitIntoMutableMessage()

	for _, msg := range msgs {
		if err := dbt.streamCli.Send(msg); err != nil {
			return fmt.Errorf("collection: broadcast create collection: %w", err)
		}
	}

	return nil
}
