package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/zilliztech/milvus-backup/core/apiv1"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func readRestoreControl(c *echo.Context) (apiv1.RestoreControlRequest, error) {
	var req apiv1.RestoreControlRequest
	d := json.NewDecoder(c.Request().Body)
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		return req, err
	}
	if req.ID == "" {
		return req, errors.New("empty restore id")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return req, errors.New("expected one JSON object")
	}
	return req, nil
}

func restoreControlError(c *echo.Context, err error) error {
	code := http.StatusBadRequest
	if errors.Is(err, jobstate.ErrPersistence) {
		code = http.StatusInternalServerError
	}
	if errors.Is(err, jobstate.ErrTaskNotFound) {
		code = http.StatusNotFound
	}
	if errors.Is(err, jobstate.ErrRestoreConflict) {
		code = http.StatusConflict
	}
	return c.JSON(code, map[string]string{"error": err.Error()})
}

// handleResumeRestore accepts only identity, never a replacement plan.
// @Summary Resume the original immutable restore plan from memory or persisted state (Alpha)
// @Description Restore Resume is Alpha (experimental). Its HTTP endpoints and request parameters, CLI commands and flags, configuration keys, and persisted state format may change without backward compatibility.
// @Tags Restore
// @Accept json
// @Produce json
// @Param request body apiv1.RestoreControlRequest true "Restore identity"
// @Success 200 {object} apiv1.RestoreBackupResponse
// @Failure 400,404,409,500 {object} map[string]string
// @Router /resume_restore [post]
func (s *Server) handleResumeRestore(c *echo.Context) error {
	req, err := readRestoreControl(c)
	if err != nil {
		return restoreControlError(c, err)
	}
	store := jobstate.Default()
	err = restore.Resume(context.Background(), store, req.ID)
	if err != nil {
		return restoreControlError(c, err)
	}
	status, err := store.GetRestoreTask(req.ID)
	if err != nil {
		return restoreControlError(c, err)
	}
	return c.JSON(http.StatusOK, &apiv1.RestoreBackupResponse{RequestId: req.RequestID, Msg: "success", Data: apiv1.NewRestoreTaskResp(status)})
}

// @Summary Abandon a failed restore and clean its temporary files in one pass (Alpha)
// @Description Restore Resume is Alpha (experimental). Its HTTP endpoints and request parameters, CLI commands and flags, configuration keys, and persisted state format may change without backward compatibility.
// @Tags Restore
// @Accept json
// @Produce json
// @Param request body apiv1.RestoreControlRequest true "Restore identity"
// @Success 200 {object} apiv1.RestoreBackupResponse
// @Failure 400,404,409 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /cleanup_failed_restore [post]
func (s *Server) handleCleanupFailedRestore(c *echo.Context) error {
	req, err := readRestoreControl(c)
	if err != nil {
		return restoreControlError(c, err)
	}
	store := jobstate.Default()
	err = restore.CleanupFailed(c.Request().Context(), store, req.ID)
	if err != nil {
		if errors.Is(err, jobstate.ErrTaskNotFound) || errors.Is(err, jobstate.ErrRestoreConflict) {
			return restoreControlError(c, err)
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	status, err := store.GetRestoreTask(req.ID)
	if err != nil {
		return restoreControlError(c, err)
	}
	return c.JSON(http.StatusOK, &apiv1.RestoreBackupResponse{RequestId: req.RequestID, Msg: "success", Data: apiv1.NewRestoreTaskResp(status)})
}
