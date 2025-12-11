package server

import (
	"fmt"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

// Server is the Backup Server
type Server struct {
	engine *gin.Engine
	config *config
	params *paramtable.BackupParams
}

func New(params *paramtable.BackupParams, opts ...Option) (*Server, error) {
	cfg := newDefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	s := &Server{config: cfg, params: params}
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
	apiv1.POST("/restore_secondary", s.handleRestoreSecondary)
	apiv1.GET("/get_restore", s.handleGetRestore)
	apiv1.GET("/check", s.handleCheck)
	apiv1.GET("/docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
}

func (s *Server) handleHello(c *gin.Context) {
	c.String(200, "Hello, This is backup service")
}
