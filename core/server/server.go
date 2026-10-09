package server

import (
	"fmt"
	"net/http"
	"sync"

	// Registers pprof handlers on http.DefaultServeMux; the mux is mounted
	// below under /debug/pprof, so nothing else on it is reachable.
	_ "net/http/pprof"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	echoSwagger "github.com/swaggo/echo-swagger/v2"

	"github.com/zilliztech/milvus-backup/docs"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// Server is the Backup Server
type Server struct {
	engine      *echo.Echo
	config      *config
	params      *v2.Config
	compactMu   sync.Mutex
	compactJobs map[string]*l0CompactJob
}

func New(params *v2.Config, opts ...Option) (*Server, error) {
	cfg := newDefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	s := &Server{config: cfg, params: params}
	s.initEngine()

	return s, nil
}

func (s *Server) Run() error {
	// A bare http.Server instead of echo's Start keeps the old process
	// semantics: no signal handling, so SIGTERM kills the process outright
	// instead of draining long-running backup or restore requests.
	srv := &http.Server{Addr: s.config.port, Handler: s.engine}
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("server: run http server: %w", err)
	}

	return nil
}

// initEngine registers the http server routes.
//
// @title           Milvus Backup Service
// @version         1.0
// @description     A data backup & restore tool for Milvus
// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath  /api/v1
func (s *Server) initEngine() {
	engine := echo.New()
	engine.Use(middleware.RequestLogger(), middleware.Recover())
	engine.Any("/debug/pprof", echo.WrapHandler(http.DefaultServeMux))
	engine.Any("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))

	s.engine = engine

	if bp := s.params.Server.SwaggerBasePath.Val; bp != "" {
		docs.SwaggerInfo.BasePath = bp
	}

	engine.Any("/", s.handleHello)

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
	apiv1.POST("/l0compact", s.handleL0Compact)
	apiv1.GET("/get_l0compact", s.handleGetL0Compact)
	apiv1.GET("/has_l0", s.handleHasL0)
	apiv1.GET("/docs/*", echoSwagger.EchoWrapHandler())
}

func (s *Server) handleHello(c *echo.Context) error {
	return c.String(http.StatusOK, "Hello, This is backup service")
}
