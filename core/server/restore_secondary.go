package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

// restoreSecondaryUC is the slice of app.RestoreSecondary the handler needs.
// The consumer defines it: this narrow interface is what handler tests stub
// out.
type restoreSecondaryUC interface {
	Start(ctx context.Context, req app.RestoreSecondaryRequest) (app.RestoreJob, error)
}

// RestoreBackup Restore interface
// @Summary Restore interface
// @Description Submit a request to restore the data from backup
// @Tags Restore
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body backuppb.RestoreSecondaryRequest   true  "RestoreBackupRequest JSON"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /restore_secondary [post]
func (s *Server) handleRestoreSecondary(c *gin.Context) {
	var request backuppb.RestoreSecondaryRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request body: %s", err)})
		return
	}

	log.Info("receive restore secondary request", zap.Any("request", &request))

	resp := s.restoreSecondary(c.Request.Context(), &request)

	writeResponse(c, "restore secondary fail", resp)
}

// restoreSecondary keeps the v1 contract of this endpoint: the request id is
// defaulted and echoed by every response, doubles as the restore task id, and
// the async flag makes the server run the job in its own goroutine.
func (s *Server) restoreSecondary(ctx context.Context, request *backuppb.RestoreSecondaryRequest) *backuppb.RestoreBackupResponse {
	completeRestoreSecondaryRequest(request)

	resp := &backuppb.RestoreBackupResponse{RequestId: request.GetRequestId()}
	if err := validateRestoreSecondaryRequest(request); err != nil {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = err.Error()
		return resp
	}

	// The v1 path field asks for a different backup location for this call.
	// That is a config override, not a request option: fork the loaded
	// config with it as one more override layer. The fork is
	// reload-equivalent, so unset backup.storage leaves still cascade from
	// milvus.storage, and the server's own config is never mutated.
	params := s.params
	if path := request.GetPath(); path != "" {
		var err error
		params, err = params.Fork(map[string]string{"backup.storage.rootPath": path})
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
			return resp
		}
	}

	uc, err := s.config.newRestoreSecondary(ctx, params)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	job, err := uc.Start(ctx, newRestoreSecondaryRequest(request))
	if err != nil {
		// A backup that is not there is the caller's mistake; everything
		// else is the server's.
		resp.Code = backuppb.ResponseCode_Fail
		if errors.Is(err, app.ErrBackupNotFound) {
			resp.Code = backuppb.ResponseCode_Parameter_Error
		}
		resp.Msg = err.Error()
		return resp
	}

	if request.GetAsync() {
		return s.restoreSecondaryAsync(job, request, resp)
	}

	if err := job.Run(ctx); err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	view, err := s.restoreTaskView(ctx, request.GetRequestId())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.RestoreTaskViewToResp(view)
	return resp
}

// restoreSecondaryAsync launches the job in the server's own goroutine —
// async is a deployment concern of the HTTP server, not of the usecase — then
// reports the freshly registered job's view.
func (s *Server) restoreSecondaryAsync(job app.RestoreJob, request *backuppb.RestoreSecondaryRequest, resp *backuppb.RestoreBackupResponse) *backuppb.RestoreBackupResponse {
	go func() {
		if err := job.Run(context.Background()); err != nil {
			log.Error("restore backup task execute fail", zap.String("request_id", request.GetRequestId()), zap.Error(err))
		}
	}()

	view, err := s.restoreTaskView(context.Background(), request.GetRequestId())
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "restore backup is executing asynchronously"
	resp.Data = pbconv.RestoreTaskViewToResp(view)
	return resp
}

// completeRestoreSecondaryRequest defaults the request id. It doubles as the
// restore task id, which is why the secondary endpoints have no separate one.
func completeRestoreSecondaryRequest(request *backuppb.RestoreSecondaryRequest) {
	if len(request.GetRequestId()) == 0 {
		request.RequestId = uuid.NewString()
	}
}

func validateRestoreSecondaryRequest(request *backuppb.RestoreSecondaryRequest) error {
	if len(request.GetBackupName()) == 0 {
		return errors.New("backup name is required")
	}

	if len(request.GetSourceClusterID()) == 0 {
		return errors.New("source cluster id is required")
	}

	if len(request.GetTargetClusterID()) == 0 {
		return errors.New("target cluster id is required")
	}

	return nil
}

func newRestoreSecondaryRequest(request *backuppb.RestoreSecondaryRequest) app.RestoreSecondaryRequest {
	return app.RestoreSecondaryRequest{
		TaskID:          request.GetRequestId(),
		BackupName:      request.GetBackupName(),
		SourceClusterID: request.GetSourceClusterID(),
		TargetClusterID: request.GetTargetClusterID(),
	}
}
