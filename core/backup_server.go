package core

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"net/http"
	"net/http/pprof"
)

var Params paramtable.GrpcServerConfig
var HTTPParams paramtable.HTTPConfig

const apiPathPrefix = "/api/v1"

// Server is the Backup Server
type Server struct {
	backupContext *BackupContext
	engine        *gin.Engine
}

func NewServer(ctx context.Context, params paramtable.ComponentParam) (*Server, error) {

	var err error
	server := &Server{}

	server.backupContext = CreateBackupContext(ctx, params)
	return server, err
}

func (s *Server) Init() {
	s.registerHTTPServer()
}

func (s *Server) Start() {
	s.registerProfilePort()
	s.engine.Run()
}

// registerHTTPServer register the http server, panic when failed
func (s *Server) registerHTTPServer() {
	if !HTTPParams.DebugMode {
		gin.SetMode(gin.ReleaseMode)
	}
	ginHandler := gin.Default()
	apiv1 := ginHandler.Group(apiPathPrefix)
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
	router.GET("/hello", wrapHandler(h.handleHello))

	router.POST("/create_backup", wrapHandler(h.handleCreateBackup))
	router.GET("/list_backups", wrapHandler(h.handleListBackups))
	router.GET("/get_backup", wrapHandler(h.handleGetBackup))
	router.DELETE("/delete_backup", wrapHandler(h.handleDeleteBackup))
	router.POST("/list_backups", wrapHandler(h.handleLoadBackup))
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
	//username := c.PostForm("username")
	//password := c.DefaultPostForm("password", "000000") // 可设置默认值
	//
	//c.JSON(http.StatusOK, gin.H{
	//	"username": username,
	//	"password": password,
	//})
	return nil, nil
}

func (h *Handlers) handleListBackups(c *gin.Context) (interface{}, error) {
	return nil, nil
}

func (h *Handlers) handleGetBackup(c *gin.Context) (interface{}, error) {
	return nil, nil
}

func (h *Handlers) handleDeleteBackup(c *gin.Context) (interface{}, error) {
	return nil, nil
}

func (h *Handlers) handleLoadBackup(c *gin.Context) (interface{}, error) {
	return nil, nil
}
