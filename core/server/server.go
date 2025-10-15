package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// Server is the Backup Server
type Server struct {
	backupContext *core.BackupContext
	engine        *gin.Engine
	config        *config
	params        *paramtable.BackupParams
}

func New(ctx context.Context, params *paramtable.BackupParams, opts ...Option) (*Server, error) {
	cfg := newDefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	backupContext := core.CreateBackupContext(ctx, params)
	err := backupContext.Start()
	if err != nil {
		return nil, fmt.Errorf("server: start backup context: %w", err)
	}

	s := &Server{backupContext: backupContext, config: cfg, params: params}
	s.initEngine()

	return s, nil
}

func (s *Server) Run() error {
	err := s.engine.Run(s.config.port)
	if err != nil {
		return fmt.Errorf("server: run http server: %w", err)
	}

	return nil
}

// registerHTTPServer register the http server, panic when failed
func (s *Server) initEngine() {
	if !s.params.HTTPCfg.DebugMode {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.Default()
	pprof.Register(engine)

	s.engine = engine

	engine.Any("", wrapHandler(s.handleHello))

	apiv1 := engine.Group("/api/v1")

	apiv1.GET("/hello", wrapHandler(s.handleHello))
	apiv1.POST("/create", wrapHandler(s.handleCreateBackup))
	apiv1.GET("/list", wrapHandler(s.handleListBackups))
	apiv1.GET("/get_backup", wrapHandler(s.handleGetBackup))
	apiv1.DELETE("/delete", wrapHandler(s.handleDeleteBackup))
	apiv1.POST("/restore", s.handleRestoreBackup)
	apiv1.GET("/get_restore", wrapHandler(s.handleGetRestore))
	apiv1.GET("/check", wrapHandler(s.handleCheck))
	apiv1.GET("/docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
}

// handlerFunc handles http request with gin context
type handlerFunc func(c *gin.Context) (interface{}, error)

// wrapHandler wraps a handlerFunc into a gin.HandlerFunc
func wrapHandler(handle handlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		handle(c)
	}
}

func (s *Server) handleHello(c *gin.Context) (any, error) {
	c.String(200, "Hello, This is backup service")
	return nil, nil
}

// CreateBackup Create backup interface
// @Summary Create backup interface
// @Description Create a backup with the given name and collections
// @Tags Backup
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body backuppb.CreateBackupRequest   true  "CreateBackupRequest JSON"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /create [post]
func (s *Server) handleCreateBackup(c *gin.Context) (interface{}, error) {
	var requestBody backuppb.CreateBackupRequest
	if err := c.ShouldBindJSON(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return nil, nil
	}
	requestBody.RequestId = c.GetHeader("request_id")
	resp := s.backupContext.CreateBackup(context.Background(), &requestBody)
	if s.params.HTTPCfg.SimpleResponse {
		resp = meta.SimpleBackupResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// ListBackups List Backups interface
// @Summary List Backups interface
// @Description List all backups in current storage
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param collection_name query string false "collection_name"
// @Success 200 {object} backuppb.ListBackupsResponse
// @Router /list [get]
func (s *Server) handleListBackups(c *gin.Context) (interface{}, error) {
	req := backuppb.ListBackupsRequest{
		RequestId:      c.GetHeader("request_id"),
		CollectionName: c.Query("collection_name"),
	}
	resp := s.backupContext.ListBackups(context.Background(), &req)
	if s.params.HTTPCfg.SimpleResponse {
		resp = meta.SimpleListBackupsResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// RestoreBackup Get backup interface
// @Summary Get backup interface
// @Description Get the backup with the given name or id
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param backup_name query string true "backup_name"
// @Param backup_id query string true "backup_id"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /get_backup [get]
func (s *Server) handleGetBackup(c *gin.Context) (interface{}, error) {
	req := backuppb.GetBackupRequest{
		RequestId:  c.GetHeader("request_id"),
		BackupName: c.Query("backup_name"),
		BackupId:   c.Query("backup_id"),
	}
	resp := s.backupContext.GetBackup(context.Background(), &req)
	if s.params.HTTPCfg.SimpleResponse {
		resp = meta.SimpleBackupResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// DeleteBackup Delete backup interface
// @Summary Delete backup interface
// @Description Delete a backup with the given name
// @Tags Backup
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param backup_name query string true "backup_name"
// @Success 200 {object} backuppb.DeleteBackupResponse
// @Router /delete [delete]
func (s *Server) handleDeleteBackup(c *gin.Context) (interface{}, error) {
	req := backuppb.DeleteBackupRequest{
		RequestId:  c.GetHeader("request_id"),
		BackupName: c.Query("backup_name"),
	}
	resp := s.backupContext.DeleteBackup(context.Background(), &req)
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// GetRestore Get restore interface
// @Summary Get restore interface
// @Description Get restore task state with the given id
// @Tags Restore
// @Produce application/json
// @Param request_id header string false "request_id"
// @param id query string true "id"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /get_restore [get]
func (s *Server) handleGetRestore(c *gin.Context) (interface{}, error) {
	req := backuppb.GetRestoreStateRequest{
		RequestId: c.GetHeader("request_id"),
		Id:        c.Query("id"),
	}
	resp := s.backupContext.GetRestore(context.Background(), &req)
	log.Info("End to GetRestoreStateRequest", zap.Any("resp", resp))
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

func (s *Server) handleCheck(c *gin.Context) (interface{}, error) {
	resp := s.backupContext.Check(context.Background())
	c.JSON(http.StatusOK, resp)
	return nil, nil
}
