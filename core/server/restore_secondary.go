package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/apiv1"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// restoreSecondaryJobFactory is the secondary-restore counterpart of
// restoreJobFactory.
type restoreSecondaryJobFactory func(ctx context.Context, params *cfg.Config, req app.RestoreSecondaryRequest) (restoreJob, error)

// RestoreBackup Restore interface
// @Summary Restore interface
// @Description Submit a request to restore the data from backup
// @Tags Restore
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body apiv1.RestoreSecondaryRequest   true  "RestoreSecondaryRequest JSON"
// @Success 200 {object} apiv1.RestoreBackupResponse
// @Router /restore_secondary [post]
func (s *Server) handleRestoreSecondary(c *echo.Context) error {
	var request apiv1.RestoreSecondaryRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&request); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("invalid request body: %s", err)})
	}

	log.Info("receive restore secondary request", zap.Any("request", &request))

	resp := s.restoreSecondary(c.Request().Context(), &request)

	return writeResponse(c, resp)
}

// restoreSecondary keeps the v1 contract of this endpoint: the request id is
// defaulted and echoed by every response, doubles as the restore task id, and
// the async flag makes the server run the job in its own goroutine.
func (s *Server) restoreSecondary(ctx context.Context, request *apiv1.RestoreSecondaryRequest) *apiv1.RestoreBackupResponse {
	completeRestoreSecondaryRequest(request)

	resp := &apiv1.RestoreBackupResponse{RequestId: request.RequestId}
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
	if path := request.Path; path != "" {
		var err error
		params, err = params.Fork(map[string]string{"backup.storage.rootPath": path})
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
			return resp
		}
	}

	job, err := s.config.newRestoreSecondaryJob(ctx, params, newRestoreSecondaryRequest(request))
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

	if request.Async {
		return s.restoreSecondaryAsync(job, request, resp)
	}

	// Run takes a process-scoped context, not the request's: the restore
	// outlives the connection. Wait takes the request's, because a client
	// giving up only stops the waiting, never the job.
	job.Run(context.Background())
	status, err := job.Wait(ctx)
	if err != nil {
		// The sync path settles the wait failure here, mirroring the async
		// path's log — writeResponse no longer logs on the way out.
		log.Error("restore secondary task wait fail", zap.String("request_id", request.RequestId), zap.Error(err))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}
	if status.State == jobstate.RestoreStateFail {
		log.Error("restore secondary task execute fail", zap.String("request_id", request.RequestId), zap.String("message", status.ErrorMessage))
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = status.ErrorMessage
		return resp
	}

	view, err := s.restoreTaskView(ctx, request.RequestId)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = apiv1.NewRestoreTaskResp(view)
	return resp
}

// restoreSecondaryAsync launches the job in the background — async is a
// deployment concern of the HTTP server, not of the job — then reports the
// freshly registered job's view.
func (s *Server) restoreSecondaryAsync(job restoreJob, request *apiv1.RestoreSecondaryRequest, resp *apiv1.RestoreBackupResponse) *apiv1.RestoreBackupResponse {
	job.Run(context.Background())

	view, err := s.restoreTaskView(context.Background(), request.RequestId)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "restore backup is executing asynchronously"
	resp.Data = apiv1.NewRestoreTaskResp(view)
	return resp
}

// completeRestoreSecondaryRequest defaults the request id. It doubles as the
// restore task id, which is why the secondary endpoints have no separate one.
func completeRestoreSecondaryRequest(request *apiv1.RestoreSecondaryRequest) {
	if len(request.RequestId) == 0 {
		request.RequestId = uuid.NewString()
	}
}

func validateRestoreSecondaryRequest(request *apiv1.RestoreSecondaryRequest) error {
	if len(request.BackupName) == 0 {
		return errors.New("backup name is required")
	}

	if len(request.SourceClusterID) == 0 {
		return errors.New("source cluster id is required")
	}

	if len(request.TargetClusterID) == 0 {
		return errors.New("target cluster id is required")
	}

	return nil
}

func newRestoreSecondaryRequest(request *apiv1.RestoreSecondaryRequest) app.RestoreSecondaryRequest {
	return app.RestoreSecondaryRequest{
		TaskID:          request.RequestId,
		BackupName:      request.BackupName,
		SourceClusterID: request.SourceClusterID,
		TargetClusterID: request.TargetClusterID,
	}
}
