package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestNewGCController(t *testing.T) {
	// enable in request
	params := &paramtable.BackupParams{BackupCfg: paramtable.BackupConfig{}}
	req := &backuppb.CreateBackupRequest{GcPauseEnable: true, GcPauseAddress: "http://localhost:9091"}
	ctrl, err := newGCController(req, params)
	assert.NoError(t, err)
	assert.True(t, ctrl.enable)

	// enable in params
	params = &paramtable.BackupParams{BackupCfg: paramtable.BackupConfig{
		GcPauseEnable:  true,
		GcPauseAddress: "http://localhost:9091",
	}}
	req = &backuppb.CreateBackupRequest{}
	ctrl, err = newGCController(req, params)
	assert.NoError(t, err)
	assert.True(t, ctrl.enable)

	// enable in both but no address
	params = &paramtable.BackupParams{BackupCfg: paramtable.BackupConfig{GcPauseEnable: true}}
	req = &backuppb.CreateBackupRequest{}
	ctrl, err = newGCController(req, params)
	assert.Error(t, err)

	// disable in both
	params = &paramtable.BackupParams{BackupCfg: paramtable.BackupConfig{}}
	req = &backuppb.CreateBackupRequest{}
	ctrl, err = newGCController(req, params)
	assert.NoError(t, err)
	assert.False(t, ctrl.enable)
}

func TestGCController_Pause(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		manage := milvus.NewMockManage(t)
		manage.EXPECT().PauseGC(mock.Anything, int32(_defaultPauseDuration.Seconds())).
			Return("ok", nil).Once()

		ctrl := &gcController{enable: true, manage: manage, logger: zap.NewNop()}
		ctrl.Pause(context.Background())
	})

	t.Run("Disable", func(t *testing.T) {
		ctrl := &gcController{enable: false, logger: zap.NewNop()}
		ctrl.Pause(context.Background())
	})
}

func TestGCController_Resume(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		manage := milvus.NewMockManage(t)
		manage.EXPECT().ResumeGC(mock.Anything).Return("ok", nil).Once()

		ctrl := &gcController{enable: true, manage: manage, logger: zap.NewNop(), stop: make(chan struct{}, 1)}
		ctrl.Resume(context.Background())
	})

	t.Run("Disable", func(t *testing.T) {
		ctrl := &gcController{enable: false, logger: zap.NewNop()}
		ctrl.Resume(context.Background())
	})
}
