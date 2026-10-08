package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/labstack/echo/v5"
	"go.uber.org/zap"

	corel0 "github.com/zilliztech/milvus-backup/core/l0compact"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

const (
	l0CompactExecuting = 1
	l0CompactSuccess   = 2
	l0CompactFailed    = 3
)

type l0CompactRequest struct {
	BackupName string `json:"backup_name"`
	OutputName string `json:"output_name"`
	Path       string `json:"path"`
	BucketName string `json:"bucket_name"`
}

type l0CompactResponse struct {
	StateCode    int    `json:"state_code"`
	BackupName   string `json:"backup_name"`
	OutputName   string `json:"output_name"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type hasL0Response struct {
	BackupName string `json:"backup_name"`
	HasL0      bool   `json:"has_l0"`
}

type l0APIResponse struct {
	RequestID string                `json:"requestId,omitempty"`
	Code      backuppb.ResponseCode `json:"code"`
	Msg       string                `json:"msg"`
	Data      any                   `json:"data,omitempty"`
}

func (r l0APIResponse) GetRequestId() string           { return r.RequestID }
func (r l0APIResponse) GetCode() backuppb.ResponseCode { return r.Code }
func (r l0APIResponse) GetMsg() string                 { return r.Msg }

func writeL0Response(c *echo.Context, code backuppb.ResponseCode, msg string, data any) error {
	return writeResponse(c, "l0 request fail", l0APIResponse{
		RequestID: c.Request().Header.Get("request_id"), Code: code, Msg: msg, Data: data,
	})
}

type l0CompactJob struct {
	mu      sync.Mutex
	request l0CompactRequest
	state   int
	err     string
}

func (j *l0CompactJob) response() l0CompactResponse {
	j.mu.Lock()
	defer j.mu.Unlock()
	return l0CompactResponse{
		StateCode: j.state, BackupName: j.request.BackupName,
		OutputName: j.request.OutputName, ErrorMessage: j.err,
	}
}

func (s *Server) handleHasL0(c *echo.Context) error {
	backupName := c.QueryParam("backup_name")
	backupPath := c.QueryParam("path")
	if backupName == "" || backupPath == "" {
		return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, "backup_name and path are required", nil)
	}
	overrides := map[string]string{"backup.storage.rootPath": backupPath}
	if bucket := c.QueryParam("bucket_name"); bucket != "" {
		overrides["backup.storage.bucketName"] = bucket
	}
	params, err := s.params.Fork(overrides)
	if err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, err.Error(), nil)
	}
	cli, err := storage.NewClient(c.Request().Context(), storage.BackupStorageConfig(params))
	if err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Fail, err.Error(), nil)
	}
	info, err := meta.Read(c.Request().Context(), cli, mpath.BackupDir(params.Backup.Storage.RootPath.Val, backupName))
	if err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Fail, err.Error(), nil)
	}
	return writeL0Response(c, backuppb.ResponseCode_Success, "success",
		hasL0Response{BackupName: backupName, HasL0: corel0.HasL0(info)})
}

func (s *Server) handleL0Compact(c *echo.Context) error {
	var req l0CompactRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, err.Error(), nil)
	}
	if err := validateL0CompactRequest(req); err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, err.Error(), nil)
	}
	key := l0CompactKey(req.BucketName, req.Path, req.OutputName)
	s.compactMu.Lock()
	if existing := s.compactJobs[key]; existing != nil {
		s.compactMu.Unlock()
		if existing.request.BackupName != req.BackupName {
			return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, "output belongs to a different source backup", nil)
		}
		return writeL0Response(c, backuppb.ResponseCode_Success, "success", existing.response())
	}
	params := s.params
	overrides := map[string]string{"backup.storage.rootPath": req.Path}
	if req.BucketName != "" {
		overrides["backup.storage.bucketName"] = req.BucketName
	}
	var err error
	params, err = params.Fork(overrides)
	if err != nil {
		s.compactMu.Unlock()
		return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, err.Error(), nil)
	}
	cli, err := storage.NewBackupStorage(c.Request().Context(), params)
	if err != nil {
		s.compactMu.Unlock()
		return writeL0Response(c, backuppb.ResponseCode_Fail, err.Error(), nil)
	}
	if info, readErr := meta.Read(c.Request().Context(), cli,
		mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.OutputName)); readErr == nil && info.GetName() == req.OutputName {
		s.compactMu.Unlock()
		return writeL0Response(c, backuppb.ResponseCode_Success, "success", l0CompactResponse{
			StateCode: l0CompactSuccess, BackupName: req.BackupName, OutputName: req.OutputName,
		})
	}
	job := &l0CompactJob{request: req, state: l0CompactExecuting}
	if s.compactJobs == nil {
		s.compactJobs = make(map[string]*l0CompactJob)
	}
	s.compactJobs[key] = job
	s.compactMu.Unlock()

	src := mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.BackupName)
	dst := mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.OutputName)
	go func() {
		err := corel0.NewTask(cli, src, dst, corel0.WithForce(true)).Execute(context.Background())
		job.mu.Lock()
		defer job.mu.Unlock()
		if err != nil {
			job.state = l0CompactFailed
			job.err = err.Error()
			log.Error("l0compact task failed", zap.String("source", req.BackupName),
				zap.String("output", req.OutputName), zap.Error(err))
			return
		}
		job.state = l0CompactSuccess
	}()
	return writeL0Response(c, backuppb.ResponseCode_Success, "success", job.response())
}

func (s *Server) handleGetL0Compact(c *echo.Context) error {
	req := l0CompactRequest{
		BackupName: c.QueryParam("backup_name"), OutputName: c.QueryParam("output_name"),
		Path: c.QueryParam("path"), BucketName: c.QueryParam("bucket_name"),
	}
	if err := validateL0CompactRequest(req); err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, err.Error(), nil)
	}
	s.compactMu.Lock()
	job := s.compactJobs[l0CompactKey(req.BucketName, req.Path, req.OutputName)]
	s.compactMu.Unlock()
	if job != nil {
		if job.request.BackupName != req.BackupName {
			return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, "output belongs to a different source backup", nil)
		}
		return writeL0Response(c, backuppb.ResponseCode_Success, "success", job.response())
	}
	params := s.params
	overrides := map[string]string{"backup.storage.rootPath": req.Path}
	if req.BucketName != "" {
		overrides["backup.storage.bucketName"] = req.BucketName
	}
	params, err := params.Fork(overrides)
	if err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Parameter_Error, err.Error(), nil)
	}
	cli, err := storage.NewBackupStorage(c.Request().Context(), params)
	if err != nil {
		return writeL0Response(c, backuppb.ResponseCode_Fail, err.Error(), nil)
	}
	info, err := meta.Read(c.Request().Context(), cli, mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.OutputName))
	if err != nil || info.GetName() != req.OutputName {
		return writeL0Response(c, backuppb.ResponseCode_Request_Object_Not_Found, "l0compact task and completed output not found", nil)
	}
	return writeL0Response(c, backuppb.ResponseCode_Success, "success", l0CompactResponse{
		StateCode: l0CompactSuccess, BackupName: req.BackupName, OutputName: req.OutputName,
	})
}

func validateL0CompactRequest(req l0CompactRequest) error {
	if req.BackupName == "" || req.OutputName == "" || req.Path == "" {
		return errors.New("backup_name, output_name and path are required")
	}
	if req.BackupName == req.OutputName {
		return errors.New("output_name must differ from backup_name")
	}
	return nil
}

func l0CompactKey(bucket, path, output string) string {
	return bucket + "\x00" + path + "\x00" + output
}
