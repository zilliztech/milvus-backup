package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
)

func TestTask_backupRPCChannelPOS(t *testing.T) {
	t.Run("SkipWhenFeatureNotSupported", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.ReplicateMessage).Return(false)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   cli,
		}
		task.backupRPCChannelPOS(context.Background())
		// ReplicateMessage should not be called
		cli.AssertNotCalled(t, "ReplicateMessage", mock.Anything, mock.Anything)
	})

	t.Run("ExecuteWhenFeatureSupported", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.ReplicateMessage).Return(true)
		cli.EXPECT().ReplicateMessage(mock.Anything, "by-dev-replicate-msg").Return("pos123", nil)

		task := &Task{
			taskID:         "task1",
			rpcChannelName: "by-dev-replicate-msg",
			logger:         zap.NewNop(),
			grpc:           cli,
			metaBuilder:    newMetaBuilder("task1", "backup1"),
		}
		task.backupRPCChannelPOS(context.Background())

		rpcInfo := task.metaBuilder.data.RpcChannelInfo
		assert.NotNil(t, rpcInfo)
		assert.Equal(t, "by-dev-replicate-msg", rpcInfo.Name)
		assert.Equal(t, "pos123", rpcInfo.Position)
	})

	t.Run("WarnOnError", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasFeature(milvus.ReplicateMessage).Return(true)
		cli.EXPECT().ReplicateMessage(mock.Anything, "by-dev-replicate-msg").Return("", errors.New("connection refused"))

		task := &Task{
			taskID:         "task1",
			rpcChannelName: "by-dev-replicate-msg",
			logger:         zap.NewNop(),
			grpc:           cli,
			metaBuilder:    newMetaBuilder("task1", "backup1"),
		}
		task.backupRPCChannelPOS(context.Background())

		// Should not set rpc channel info on error
		assert.Nil(t, task.metaBuilder.data.RpcChannelInfo)
	})
}
