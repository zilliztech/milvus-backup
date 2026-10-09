package server

import (
	"context"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// deleteJob is the slice of app.DeleteJob the handler needs. The consumer
// defines it: app returns concrete types, and this narrow interface is what
// handler tests stub out.
type deleteJob interface {
	// Run releases the job into the background.
	Run(ctx context.Context)
	// Wait answers with the outcome the job records when it settles; it is
	// what the handler blocks on, since v1 exposes no async form.
	Wait(ctx context.Context) (jobstate.DeleteStatus, error)
}

// deleteJobFactory builds and registers the job for one delete request: a
// delete job is per-request, so building it and registering it are one step.
type deleteJobFactory func(ctx context.Context, params *cfg.Config, req app.DeleteBackupRequest) (deleteJob, error)

// DeleteBackup Delete backup interface
// @Summary Delete backup interface
// @Description Delete a backup with the given name
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param backup_name query string true "backup_name"
// @Success 200 {object} backuppb.DeleteBackupResponse
// @Router /delete [delete]
func (s *Server) handleDeleteBackup(c *echo.Context) error {
	requestID := c.Request().Header.Get("request_id")
	if len(requestID) == 0 {
		requestID = uuid.NewString()
	}

	resp := &backuppb.DeleteBackupResponse{RequestId: requestID}
	backupName := c.QueryParam("backup_name")
	if len(backupName) == 0 {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "backup name is required"
		return writeResponse(c, "delete backup fail", resp)
	}

	job, err := s.config.newDeleteJob(c.Request().Context(), s.params,
		app.DeleteBackupRequest{TaskID: requestID, BackupName: backupName})
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return writeResponse(c, "delete backup fail", resp)
	}

	// v1 exposes no async form: the job runs detached from the request — a
	// client disconnect must not kill a half-done delete — and the handler
	// answers with the outcome it settles on.
	job.Run(context.Background())

	status, err := job.Wait(c.Request().Context())
	if err != nil {
		// The request context died while waiting; the delete itself keeps
		// running to completion in the background.
		return err
	}
	if status.State == jobstate.DeleteStateFail {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = status.ErrorMessage
		return writeResponse(c, "delete backup fail", resp)
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"

	return writeResponse(c, "delete backup fail", resp)
}
