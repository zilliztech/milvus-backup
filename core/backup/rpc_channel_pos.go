package backup

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type rpcChannelPOSTask struct {
	taskID         string
	rpcChannelName string

	grpc        milvus.Grpc
	metaBuilder *metaBuilder

	logger *zap.Logger
}

func newRPCChannelPOSTask(taskID, rpcChannelName string, grpc milvus.Grpc, metaBuilder *metaBuilder) *rpcChannelPOSTask {
	return &rpcChannelPOSTask{
		taskID: taskID,

		rpcChannelName: rpcChannelName,

		grpc:        grpc,
		logger:      log.L().With(zap.String("task_id", taskID)),
		metaBuilder: metaBuilder,
	}
}

func (rt *rpcChannelPOSTask) Execute(ctx context.Context) error {
	rt.logger.Info("try to get rpc channel pos", zap.String("rpc_channel", rt.rpcChannelName))
	pos, err := rt.grpc.ReplicateMessage(ctx, rt.rpcChannelName)
	if err != nil {
		return fmt.Errorf("backup: call replicate message failed: %w", err)
	}

	rt.logger.Info("get rpc channel pos done", zap.String("pos", pos))

	rt.metaBuilder.setRPCChannelInfo(&backuppb.RPCChannelInfo{Name: rt.rpcChannelName, Position: pos})

	return nil
}
