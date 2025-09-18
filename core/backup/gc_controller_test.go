package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
)

func TestGCController_Pause(t *testing.T) {
	manage := milvus.NewMockManage(t)
	manage.EXPECT().PauseGC(mock.Anything, int32(_defaultPauseDuration.Seconds())).
		Return("ok", nil).Once()

	ctrl := &gcController{manage: manage, logger: zap.NewNop()}
	ctrl.Pause(context.Background())
}

func TestGCController_Resume(t *testing.T) {

	manage := milvus.NewMockManage(t)
	manage.EXPECT().ResumeGC(mock.Anything).Return("ok", nil).Once()

	ctrl := &gcController{manage: manage, logger: zap.NewNop(), stop: make(chan struct{}, 1)}
	ctrl.Resume(context.Background())

}
