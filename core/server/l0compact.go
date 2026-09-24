package server

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/backup"
	corel0 "github.com/zilliztech/milvus-backup/core/l0compact"
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
	BackupName string `json:"backup_name" binding:"required"`
	OutputName string `json:"output_name" binding:"required"`
	Path       string `json:"path" binding:"required"`
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

func (s *Server) handleHasL0(c *gin.Context) {
	backupName := c.Query("backup_name")
	backupPath := c.Query("path")
	if backupName == "" || backupPath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "backup_name and path are required"})
		return
	}
	if err := backup.ValidateName(backupName); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	overrides := map[string]string{"backup.storage.rootPath": backupPath}
	if bucket := c.Query("bucket_name"); bucket != "" {
		overrides["backup.storage.bucketName"] = bucket
	}
	params, err := s.params.Fork(overrides)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cli, err := storage.NewClient(c.Request.Context(), storage.BackupStorageConfig(params))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	info, err := meta.Read(c.Request.Context(), cli, mpath.BackupDir(params.Backup.Storage.RootPath.Val, backupName))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, hasL0Response{BackupName: backupName, HasL0: corel0.HasL0(info)})
}

func (s *Server) handleL0Compact(c *gin.Context) {
	var req l0CompactRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateL0CompactRequest(req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := l0CompactKey(req.BucketName, req.Path, req.OutputName)
	s.compactMu.Lock()
	if existing := s.compactJobs[key]; existing != nil {
		s.compactMu.Unlock()
		if existing.request.BackupName != req.BackupName {
			c.JSON(http.StatusConflict, gin.H{"error": "output belongs to a different source backup"})
			return
		}
		c.JSON(http.StatusOK, existing.response())
		return
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
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cli, err := storage.NewBackupStorage(c.Request.Context(), params)
	if err != nil {
		s.compactMu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if info, readErr := meta.Read(c.Request.Context(), cli,
		mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.OutputName)); readErr == nil && info.GetName() == req.OutputName {
		s.compactMu.Unlock()
		c.JSON(http.StatusOK, l0CompactResponse{
			StateCode: l0CompactSuccess, BackupName: req.BackupName, OutputName: req.OutputName,
		})
		return
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
	c.JSON(http.StatusOK, job.response())
}

func (s *Server) handleGetL0Compact(c *gin.Context) {
	req := l0CompactRequest{
		BackupName: c.Query("backup_name"), OutputName: c.Query("output_name"),
		Path: c.Query("path"), BucketName: c.Query("bucket_name"),
	}
	if err := validateL0CompactRequest(req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.compactMu.Lock()
	job := s.compactJobs[l0CompactKey(req.BucketName, req.Path, req.OutputName)]
	s.compactMu.Unlock()
	if job != nil {
		if job.request.BackupName != req.BackupName {
			c.JSON(http.StatusConflict, gin.H{"error": "output belongs to a different source backup"})
			return
		}
		c.JSON(http.StatusOK, job.response())
		return
	}
	params := s.params
	overrides := map[string]string{"backup.storage.rootPath": req.Path}
	if req.BucketName != "" {
		overrides["backup.storage.bucketName"] = req.BucketName
	}
	params, err := params.Fork(overrides)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cli, err := storage.NewBackupStorage(c.Request.Context(), params)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	info, err := meta.Read(c.Request.Context(), cli, mpath.BackupDir(params.Backup.Storage.RootPath.Val, req.OutputName))
	if err != nil || info.GetName() != req.OutputName {
		c.JSON(http.StatusNotFound, gin.H{"error": "l0compact task and completed output not found"})
		return
	}
	c.JSON(http.StatusOK, l0CompactResponse{
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
	if err := backup.ValidateName(req.BackupName); err != nil {
		return err
	}
	return backup.ValidateName(req.OutputName)
}

func l0CompactKey(bucket, path, output string) string {
	return bucket + "\x00" + path + "\x00" + output
}
