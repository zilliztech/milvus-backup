package core

import (
	"context"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"go.uber.org/zap"
	"net/http"
	"net/http/pprof"
)

const (
	HELLO_API          = "/hello"
	CREATE_BACKUP_API  = "/create"
	LIST_BACKUPS_API   = "/list"
	GET_BACKUP_API     = "/get_backup"
	DELETE_BACKUP_API  = "/delete"
	RESTORE_BACKUP_API = "/restore"
	GET_RESTORE_API    = "/get_restore"

	API_V1_PREFIX = "/api/v1"

	DOCS_API = "/docs/*any"

	CHECK_API = "/check"
)

// Server is the Backup Server
type Server struct {
	backupContext *BackupContext
	engine        *gin.Engine
	config        *BackupConfig
}

func NewServer(ctx context.Context, params paramtable.BackupParams, opts ...BackupOption) (*Server, error) {
	c := newDefaultBackupConfig()
	for _, opt := range opts {
		opt(c)
	}
	backupContext := CreateBackupContext(ctx, params)
	err := backupContext.Start()
	if err != nil {
		return nil, err
	}
	return &Server{
		backupContext: backupContext,
		config:        c,
	}, nil
}

func (s *Server) Init() {
	s.registerHTTPServer()
}

func (s *Server) Start() {
	s.registerProfilePort()
	err := s.engine.Run(s.config.port)
	if err != nil {
		log.Error("Failed to start server", zap.Error(err))
		panic(err)
	}
	log.Info("Start backup server backend")
}

// registerHTTPServer register the http server, panic when failed
func (s *Server) registerHTTPServer() {
	if !s.backupContext.params.HTTPCfg.DebugMode {
		gin.SetMode(gin.ReleaseMode)
	}
	ginHandler := gin.Default()
	apiv1 := ginHandler.Group(API_V1_PREFIX)
	ginHandler.Any("", wrapHandler(handleHello))
	NewHandlers(s.backupContext).RegisterRoutesTo(apiv1)
	http.Handle("/", ginHandler)
	s.engine = ginHandler
}

// registerHTTPServer register the http server, panic when failed
func (s *Server) registerProfilePort() {
	go func() {
		http.HandleFunc("/debug/pprof/heap", pprof.Index)
		http.ListenAndServe("localhost:8089", nil)
	}()
}

func handleHello(c *gin.Context) (interface{}, error) {
	c.String(200, "Hello, This is backup service")
	return nil, nil
}

type Handlers struct {
	backupContext *BackupContext
}

// NewHandlers creates a new Handlers
func NewHandlers(backupContext *BackupContext) *Handlers {
	return &Handlers{
		backupContext: backupContext,
	}
}

// RegisterRouters registers routes to given router
func (h *Handlers) RegisterRoutesTo(router gin.IRouter) {
	router.GET(HELLO_API, wrapHandler(handleHello))
	router.POST(CREATE_BACKUP_API, wrapHandler(h.handleCreateBackup))
	router.GET(LIST_BACKUPS_API, wrapHandler(h.handleListBackups))
	router.GET(GET_BACKUP_API, wrapHandler(h.handleGetBackup))
	router.DELETE(DELETE_BACKUP_API, wrapHandler(h.handleDeleteBackup))
	router.POST(RESTORE_BACKUP_API, wrapHandler(h.handleRestoreBackup))
	router.GET(GET_RESTORE_API, wrapHandler(h.handleGetRestore))
	router.GET(CHECK_API, wrapHandler(h.handleCheck))
	router.GET(DOCS_API, ginSwagger.WrapHandler(swaggerFiles.Handler))
}

// handlerFunc handles http request with gin context
type handlerFunc func(c *gin.Context) (interface{}, error)

// wrapHandler wraps a handlerFunc into a gin.HandlerFunc
func wrapHandler(handle handlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		handle(c)
	}
}

// CreateBackup Create backup interface
// @Summary Create backup interface
// @Description Create a backup with the given name and collections
// @Tags Backup
// @Accept application/json
// @Produce application/json
// @Param request_id header string true "request_id"
// @Param object body backuppb.CreateBackupRequest   true  "CreateBackupRequest JSON"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /create [post]
func (h *Handlers) handleCreateBackup(c *gin.Context) (interface{}, error) {
	requestBody := backuppb.CreateBackupRequest{}
	if err := c.ShouldBindJSON(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return nil, nil
	}
	requestBody.RequestId = c.GetHeader("request_id")
	resp := h.backupContext.CreateBackup(h.backupContext.ctx, &requestBody)
	if h.backupContext.params.HTTPCfg.SimpleResponse {
		resp = SimpleBackupResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// ListBackups List Backups interface
// @Summary List Backups interface
// @Description List all backups in current storage
// @Tags Backup
// @Produce application/json
// @Param request_id header string true "request_id"
// @Param collection_name query string true "collection_name"
// @Success 200 {object} backuppb.ListBackupsResponse
// @Router /list [get]
func (h *Handlers) handleListBackups(c *gin.Context) (interface{}, error) {
	req := backuppb.ListBackupsRequest{
		RequestId:      c.GetHeader("request_id"),
		CollectionName: c.Query("collection_name"),
	}
	resp := h.backupContext.ListBackups(h.backupContext.ctx, &req)
	if h.backupContext.params.HTTPCfg.SimpleResponse {
		resp = SimpleListBackupsResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// RestoreBackup Get backup interface
// @Summary Get backup interface
// @Description Get the backup with the given name or id
// @Tags Backup
// @Produce application/json
// @Param request_id header string true "request_id"
// @Param backup_name query string true "backup_name"
// @Param backup_id query string true "backup_id"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /get_backup [get]
func (h *Handlers) handleGetBackup(c *gin.Context) (interface{}, error) {
	req := backuppb.GetBackupRequest{
		RequestId:  c.GetHeader("request_id"),
		BackupName: c.Query("backup_name"),
		BackupId:   c.Query("backup_id"),
	}
	resp := h.backupContext.GetBackup(h.backupContext.ctx, &req)
	if h.backupContext.params.HTTPCfg.SimpleResponse {
		resp = SimpleBackupResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// DeleteBackup Delete backup interface
// @Summary Delete backup interface
// @Description Delete a backup with the given name
// @Tags Backup
// @Produce application/json
// @Param request_id header string true "request_id"
// @Param backup_name query string true "backup_name"
// @Success 200 {object} backuppb.DeleteBackupResponse
// @Router /delete [delete]
func (h *Handlers) handleDeleteBackup(c *gin.Context) (interface{}, error) {
	req := backuppb.DeleteBackupRequest{
		RequestId:  c.GetHeader("request_id"),
		BackupName: c.Query("backup_name"),
	}
	resp := h.backupContext.DeleteBackup(h.backupContext.ctx, &req)
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// RestoreBackup Restore interface
// @Summary Restore interface
// @Description Submit a request to restore the data from backup
// @Tags Restore
// @Accept application/json
// @Produce application/json
// @Param request_id header string true "request_id"
// @Param object body backuppb.RestoreBackupRequest   true  "RestoreBackupRequest JSON"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /restore [post]
func (h *Handlers) handleRestoreBackup(c *gin.Context) (interface{}, error) {
	requestBody := backuppb.RestoreBackupRequest{
		// default setting
		MetaOnly: false,
	}
	//c.BindJSON(&json)
	if err := c.ShouldBindJSON(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return nil, nil
	}

	requestBody.RequestId = c.GetHeader("request_id")
	resp := h.backupContext.RestoreBackup(h.backupContext.ctx, &requestBody)
	if h.backupContext.params.HTTPCfg.SimpleResponse {
		resp = SimpleRestoreResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

// GetRestore Get restore interface
// @Summary Get restore interface
// @Description Get restore task state with the given id
// @Tags Restore
// @Produce application/json
// @Param request_id header string true "request_id"
// @param id query string true "id"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /get_restore [get]
func (h *Handlers) handleGetRestore(c *gin.Context) (interface{}, error) {
	req := backuppb.GetRestoreStateRequest{
		RequestId: c.GetHeader("request_id"),
		Id:        c.Query("id"),
	}
	resp := h.backupContext.GetRestore(h.backupContext.ctx, &req)
	if h.backupContext.params.HTTPCfg.SimpleResponse {
		resp = SimpleRestoreResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

func (h *Handlers) handleCheck(c *gin.Context) (interface{}, error) {
	resp := h.backupContext.Check(h.backupContext.ctx)
	c.JSON(http.StatusOK, resp)
	return nil, nil
}
