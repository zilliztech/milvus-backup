package backup

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type RPCChannelPOSTask struct {
	backupID       string
	rpcChannelName string

	grpc   milvus.Grpc
	meta   *meta.MetaManager
	logger *zap.Logger
}

func NewRPCChannelPOSTask(backupID, rpcChannelName string, grpc milvus.Grpc, meta *meta.MetaManager) *RPCChannelPOSTask {
	return &RPCChannelPOSTask{
		backupID: backupID,

		rpcChannelName: rpcChannelName,

		grpc:   grpc,
		logger: log.L().With(zap.String("backup_id", backupID)),
		meta:   meta,
	}
}

func (rt *RPCChannelPOSTask) Execute(ctx context.Context) error {
	rt.logger.Info("try to get rpc channel pos", zap.String("rpc_channel", rt.rpcChannelName))
	pos, err := rt.grpc.ReplicateMessage(ctx, rt.rpcChannelName)
	if err != nil {
		return fmt.Errorf("backup: call replicate message failed: %w", err)
	}

	rt.logger.Info("get rpc channel pos done", zap.String("pos", pos))

	rt.meta.UpdateBackup(rt.backupID, meta.SetRPCChannelPos(rt.rpcChannelName, pos))

	return nil
}
