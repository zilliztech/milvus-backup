package backup

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

const _gcWarnMessage = "Pause GC Failed," +
	"This warn won't fail the backup process. " +
	"Pause GC can protect data not to be GCed during backup, " +
	"it is necessary to backup very large data(cost more than a hour)."

var _defaultPauseDuration = 1 * time.Hour

type gcController struct {
	enable bool

	manage milvus.Manage
	stop   chan struct{}

	logger *zap.Logger
}

func newGCController(request *backuppb.CreateBackupRequest, params *paramtable.BackupParams) (*gcController, error) {
	if !request.GetGcPauseEnable() && !params.BackupCfg.GcPauseEnable {
		return &gcController{enable: false}, nil
	}

	var addr string
	if len(request.GetGcPauseAddress()) != 0 {
		addr = request.GetGcPauseAddress()
	} else if len(params.BackupCfg.GcPauseAddress) != 0 {
		addr = params.BackupCfg.GcPauseAddress
	} else {
		return nil, errors.New("enable gc pause but no address provided")
	}

	manage := milvus.NewManage(addr)
	logger := log.L().With(zap.String("component", "gc-pauser"))
	return &gcController{enable: true, manage: manage, logger: logger, stop: make(chan struct{})}, nil
}

func (t *gcController) Pause(ctx context.Context) {
	if !t.enable {
		return
	}

	resp, err := t.manage.PauseGC(ctx, int32(_defaultPauseDuration.Seconds()))
	if err != nil {
		t.logger.Warn(_gcWarnMessage, zap.Error(err), zap.String("resp", resp))
		return
	}

	t.logger.Info("pause gc success", zap.String("resp", resp))
	go t.renewalLease()
}

func (t *gcController) renewalLease() {
	for {
		select {
		case <-time.After(_defaultPauseDuration / 2):
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			resp, err := t.manage.PauseGC(ctx, int32(_defaultPauseDuration.Seconds()))
			if err != nil {
				t.logger.Warn("renewal pause gc lease failed", zap.Error(err), zap.String("resp", resp))
			}
			t.logger.Info("renewal pause gc lease done", zap.String("resp", resp))
			cancel()
		case <-t.stop:
			t.logger.Info("stop renewal pause gc lease")
			return
		}
	}
}

func (t *gcController) Resume(ctx context.Context) {
	if !t.enable {
		return
	}

	select {
	case t.stop <- struct{}{}:
	default:
		t.logger.Info("stop channel is full, maybe already renewal lease goroutine not running")
	}

	resp, err := t.manage.ResumeGC(ctx)
	if err != nil {
		t.logger.Warn("resume gc failed", zap.Error(err), zap.String("resp", resp))
	} else {
		t.logger.Info("resume gc done", zap.String("resp", resp))
	}
}
