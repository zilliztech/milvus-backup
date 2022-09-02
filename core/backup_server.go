package core

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"net/http"
	"net/http/pprof"
)

var Params paramtable.GrpcServerConfig
var HTTPParams paramtable.HTTPConfig

const (
	HELLO_API         = "/hello"
	CREATE_BACKUP_API = "/create_backup"
	LIST_BACKUPS_API  = "/list_backups"
	GET_BACKUP_API    = "/get_backup"
	DELETE_BACKUP_API = "/delete_backup"
	LOAD_BACKUP_API   = "/load_backup"

	API_V1_PREFIX = "/api/v1"
)

// Server is the Backup Server
type Server struct {
	backupContext *BackupContext
	engine        *gin.Engine
}

func NewServer(ctx context.Context, opts ...BackupOption) (*Server, error) {
	//c := newDefaultBackupConfig()
	//for _, opt := range opts {
	//	opt(c)
	//}
	return &Server{
		backupContext: CreateBackupContext(ctx, opts...),
		//config:        c,
	}, nil
}

func (s *Server) Init() {
	s.registerHTTPServer()
}

func (s *Server) Start() {
	s.registerProfilePort()
	s.engine.Run(s.Config().port)
	log.Info("Start backup server backend")
}

func (s *Server) Config() *BackupConfig {
	return s.backupContext.config
}

// registerHTTPServer register the http server, panic when failed
func (s *Server) registerHTTPServer() {
	if !HTTPParams.DebugMode {
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
	router.POST(LOAD_BACKUP_API, wrapHandler(h.handleLoadBackup))
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

func (h *Handlers) handleCreateBackup(c *gin.Context) (interface{}, error) {
	json := backuppb.CreateBackupRequest{}
	c.BindJSON(&json)
	resp, _ := h.backupContext.CreateBackup(h.backupContext.ctx, &json)
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

func (h *Handlers) handleListBackups(c *gin.Context) (interface{}, error) {
	json := backuppb.ListBackupsRequest{}
	c.BindJSON(&json)
	resp, _ := h.backupContext.ListBackups(h.backupContext.ctx, &json)
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

func (h *Handlers) handleGetBackup(c *gin.Context) (interface{}, error) {
	json := backuppb.GetBackupRequest{}
	c.BindJSON(&json)
	resp, _ := h.backupContext.GetBackup(h.backupContext.ctx, &json)
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

func (h *Handlers) handleDeleteBackup(c *gin.Context) (interface{}, error) {
	json := backuppb.DeleteBackupRequest{}
	c.BindJSON(&json)
	resp, _ := h.backupContext.DeleteBackup(h.backupContext.ctx, &json)
	c.JSON(http.StatusOK, resp)
	return nil, nil
}

func (h *Handlers) handleLoadBackup(c *gin.Context) (interface{}, error) {
	return nil, nil
}
