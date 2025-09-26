package backup

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestTask_runRBACTask(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().BackupRBAC(mock.Anything).Return(&milvuspb.BackupRBACMetaResponse{}, nil).Once()

		metaMgr := meta.NewMetaManager()
		metaMgr.AddBackup("backup1", &backuppb.BackupInfo{})

		task := &Task{
			request: &backuppb.CreateBackupRequest{Rbac: true},
			logger:  zap.NewNop(),
			taskID:  "backup1",
			meta:    metaMgr,
			grpc:    cli,
		}
		err := task.runRBACTask(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Skip", func(t *testing.T) {
		task := &Task{logger: zap.NewNop()}
		err := task.runRBACTask(context.Background())
		assert.NoError(t, err)
	})
}
