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

	tsAlloc *tsAlloc

	streamCli milvus.Stream
	logger    *zap.Logger
}

type databaseTaskArgs struct {
	TaskID string

	BackupInfo *backuppb.BackupInfo
	TSAlloc    *tsAlloc

	StreamCli milvus.Stream
}

func newDatabaseTask(args databaseTaskArgs, dbBackup *backuppb.DatabaseBackupInfo) (*databaseTask, error) {
	task := &databaseTask{
		taskID: args.TaskID,

		backupInfo: args.BackupInfo,
		dbBackup:   dbBackup,

		tsAlloc: args.TSAlloc,

		streamCli: args.StreamCli,
		logger:    log.With(zap.String("task_id", args.TaskID)),
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
		ts := dbt.tsAlloc.Alloc()
		immutableMessage := msg.WithTimeTick(ts).
			WithLastConfirmed(newFakeMessageID(ts)).
			IntoImmutableMessage(newFakeMessageID(ts)).
			IntoImmutableMessageProto()

		if err := dbt.streamCli.Send(immutableMessage); err != nil {
			return fmt.Errorf("collection: broadcast create collection: %w", err)
		}
	}

	return nil
}
