package server

import (
	"context"
	"errors"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// getBackupUC is the slice of app.GetBackup the handler needs. The consumer
// defines it: app returns concrete types, and this narrow interface is what
// handler tests stub out.
type getBackupUC interface {
	Execute(ctx context.Context, name string) (*backuppb.BackupInfo, int64, error)
}

// getBackupTaskUC is the job-half counterpart of getBackupUC, the slice of
// app.GetBackupTask the handler needs.
type getBackupTaskUC interface {
	Execute(ctx context.Context, req app.GetBackupTaskRequest) (taskmgr.BackupTaskView, error)
}

// Get backup Get backup interface
// @Summary Get backup interface
// @Description Get the backup with the given name or id
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param backup_name query string false "backup_name"
// @Param backup_id query string false "backup_id"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /get_backup [get]
//
// handleGetBackup serves the v1 contract, which merges the two resource
// halves into one response: the persisted artifact read through GetBackup and
// the ephemeral job read through GetBackupTask. The app layer keeps the two
// separate on purpose; the merge rules below are v1's wire contract, not a
// property of either resource.
func (s *Server) handleGetBackup(c *gin.Context) {
	requestID := c.GetHeader("request_id")
	if requestID == "" {
		requestID = uuid.NewString()
	}

	name := c.Query("backup_name")
	id := c.Query("backup_id")
	path := c.Query("path")
	log.Info("receive get backup request",
		zap.String("backup_name", name), zap.String("backup_id", id), zap.String("path", path))

	resp := &backuppb.BackupInfoResponse{RequestId: requestID}
	if name == "" && id == "" {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = "server: empty backup name and backup id, please set a backup name or id"
		writeResponse(c, "get backup fail", resp)
		return
	}

	ctx := c.Request.Context()

	taskUC, err := s.config.newGetBackupTask()
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "get backup fail", resp)
		return
	}
	// The v1 path parameter asks for a different backup location for this
	// call. That is a config override, not a selector: fork the loaded config
	// with it as one more override layer, keeping the request itself
	// selector-only. The fork is reload-equivalent, so unset backup.storage
	// leaves still cascade from milvus.storage, and the server's own config
	// is never mutated.
	params := s.params
	if path != "" {
		params, err = params.Fork(map[string]string{"backup.storage.rootPath": path})
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
			writeResponse(c, "get backup fail", resp)
			return
		}
	}
	backupUC, err := s.config.newGetBackup(ctx, params)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "get backup fail", resp)
		return
	}

	// The job half. An ID selects the job directly and resolves the backup
	// name through it; an unknown ID has nothing to fall back to. A name only
	// probes for a job — most backups outlive their creating process, so a
	// miss is the common case, not an error.
	var task taskmgr.BackupTaskView
	if id != "" {
		task, err = taskUC.Execute(ctx, app.GetBackupTaskRequest{ID: id})
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
			writeResponse(c, "get backup fail", resp)
			return
		}
		name = task.Name()
	} else {
		task, err = taskUC.Execute(ctx, app.GetBackupTaskRequest{Name: name})
		switch {
		case err == nil:
		case errors.Is(err, taskmgr.ErrTaskNotFound):
			task = nil
		default:
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
			writeResponse(c, "get backup fail", resp)
			return
		}
	}

	// The artifact half. A miss is not an error either while the job is
	// still in flight and has persisted nothing yet.
	metaInfo, metaSize, err := backupUC.Execute(ctx, name)
	switch {
	case err == nil:
	case errors.Is(err, app.ErrBackupNotFound):
		metaInfo, metaSize = nil, 0
	default:
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		writeResponse(c, "get backup fail", resp)
		return
	}

	// The merge: the artifact is the source of truth, the job overlays
	// progress. A job that reports success while its artifact is missing is
	// an error — the backup it claims to have produced is gone. A selector
	// with neither half behind it is not found, not success with zeros.
	switch {
	case metaInfo != nil:
		// The artifact is the answer; the job, when known, overlays progress.
	case task != nil && task.StateCode() == backuppb.BackupTaskStateCode_BACKUP_SUCCESS:
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "server: backup task " + name + " reports success but its meta is missing"
		writeResponse(c, "get backup fail", resp)
		return
	case task != nil:
		// An in-flight or failed job that has persisted nothing yet: the
		// job view alone is the answer.
	default:
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = "server: backup " + name + " not found"
		writeResponse(c, "get backup fail", resp)
		return
	}

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "success"
	resp.Data = pbconv.NewBackupInfoBrief(task, metaInfo, metaSize)

	log.Info("response get backup response", zap.Any("resp", resp))
	writeResponse(c, "get backup fail", resp)
}
