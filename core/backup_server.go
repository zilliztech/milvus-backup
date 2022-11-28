package core

import (
	"context"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
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
	s.engine.Run(s.config.port)
	log.Info("Start backup server backend")
}

// registerHTTPServer register the http server, panic when failed
func (s *Server) registerHTTPServer() {
	if !s.backupContext.params.HTTPCfg.DebugMode {
		gin.SetMode(gin.ReleaseMode)
	}
	ginHandler := gin.Default()
	apiv1 := ginHandler.Group(API_V1_PREFIX)
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
	router.GET(HELLO_API, wrapHandler(h.handleHello))
	router.POST(CREATE_BACKUP_API, wrapHandler(h.handleCreateBackup))
	router.GET(LIST_BACKUPS_API, wrapHandler(h.handleListBackups))
	router.GET(GET_BACKUP_API, wrapHandler(h.handleGetBackup))
	router.DELETE(DELETE_BACKUP_API, wrapHandler(h.handleDeleteBackup))
	router.POST(RESTORE_BACKUP_API, wrapHandler(h.handleRestoreBackup))
	router.GET(GET_RESTORE_API, wrapHandler(h.handleGetRestore))
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

func (h *Handlers) handleHello(c *gin.Context) (interface{}, error) {
	c.String(200, "Hello, This is backup service")
	return nil, nil
}

// CreateBackup Create backup interface
// @Summary Create backup interface
// @Description Create a backup with the given name and collections
// @Tags Backup
// @Accept application/json
// @Produce application/json
// @Param object body backuppb.CreateBackupRequest   true  "CreateBackupRequest JSON"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /create [post]
func (h *Handlers) handleCreateBackup(c *gin.Context) (interface{}, error) {
	json := backuppb.CreateBackupRequest{}
	c.BindJSON(&json)
	resp := h.backupContext.CreateBackup(h.backupContext.ctx, &json)
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
// @Accept application/json
// @Produce application/json
// @Param object body backuppb.ListBackupsRequest   true  "ListBackupsRequest JSON"
// @Success 200 {object} backuppb.ListBackupsResponse
// @Router /list [get]
func (h *Handlers) handleListBackups(c *gin.Context) (interface{}, error) {
	req := backuppb.ListBackupsRequest{
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
// @Accept application/json
// @Produce application/json
// @Param object body backuppb.GetBackupRequest   true  "GetBackupRequest JSON"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /get_backup [get]
func (h *Handlers) handleGetBackup(c *gin.Context) (interface{}, error) {
	req := backuppb.GetBackupRequest{
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
// @Accept application/json
// @Produce application/json
// @Param object body backuppb.DeleteBackupRequest   true  "DeleteBackupRequest JSON"
// @Success 200 {object} backuppb.DeleteBackupResponse
// @Router /delete [delete]
func (h *Handlers) handleDeleteBackup(c *gin.Context) (interface{}, error) {
	req := backuppb.DeleteBackupRequest{
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
// @Param object body backuppb.RestoreBackupRequest   true  "RestoreBackupRequest JSON"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /restore [post]
func (h *Handlers) handleRestoreBackup(c *gin.Context) (interface{}, error) {
	json := backuppb.RestoreBackupRequest{}
	c.BindJSON(&json)
	resp := h.backupContext.RestoreBackup(h.backupContext.ctx, &json)
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
// @Accept application/json
// @Produce application/json
// @Param object body backuppb.GetRestoreStateRequest   true  "GetRestoreStateRequest JSON"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /get_restore [get]
func (h *Handlers) handleGetRestore(c *gin.Context) (interface{}, error) {
	id := c.Query("id")
	req := backuppb.GetRestoreStateRequest{
		Id: id,
	}
	resp := h.backupContext.GetRestore(h.backupContext.ctx, &req)
	if h.backupContext.params.HTTPCfg.SimpleResponse {
		resp = SimpleRestoreResponse(resp)
	}
	c.JSON(http.StatusOK, resp)
	return nil, nil
}
