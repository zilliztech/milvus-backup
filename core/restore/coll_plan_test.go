package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/meta"
)

// The plan is picked by the backup's format and the restore option: a snapshot
// backup goes to Milvus's restore api, a binlog backup goes through the grpc or
// restful import api.
func TestTask_selectPlan(t *testing.T) {
	t.Run("BinlogGRPC", func(t *testing.T) {
		task := newTestTask()
		task.format = meta.FormatBinlog
		task.args.Option = &Option{}

		assert.IsType(t, &binlogGRPCPlan{}, task.selectPlan(nil))
	})

	t.Run("BinlogRESTFul", func(t *testing.T) {
		task := newTestTask()
		task.format = meta.FormatBinlog
		task.args.Option = &Option{UseV2Restore: true}

		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(true).Once()
		task.grpc = grpcCli

		assert.IsType(t, &binlogRESTFulPlan{}, task.selectPlan(nil))
	})

	t.Run("RestoreSnap", func(t *testing.T) {
		task := newTestTask()
		task.format = meta.FormatSnapshot
		task.args.Option = &Option{}

		assert.IsType(t, &restoreSnapPlan{}, task.selectPlan(nil))
	})
}
