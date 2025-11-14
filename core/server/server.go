package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
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

	engine.Any("", s.handleHello)

	apiv1 := engine.Group("/api/v1")

	apiv1.GET("/hello", s.handleHello)
	apiv1.POST("/create", s.handleCreateBackup)
	apiv1.GET("/list", s.handleListBackups)
	apiv1.GET("/get_backup", s.handleGetBackup)
	apiv1.DELETE("/delete", s.handleDeleteBackup)
	apiv1.POST("/restore", s.handleRestoreBackup)
	apiv1.GET("/get_restore", s.handleGetRestore)
	apiv1.GET("/check", s.handleCheck)
	apiv1.GET("/docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
}

func (s *Server) handleHello(c *gin.Context) {
	c.String(200, "Hello, This is backup service")
}

func (s *Server) handleCheck(c *gin.Context) {
	resp := s.backupContext.Check(context.Background())
	c.JSON(http.StatusOK, resp)
}
