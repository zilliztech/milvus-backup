package server

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// checkUC is the slice of app.Check the handler needs. The consumer defines
// it: app returns concrete types, and this narrow interface is what handler
// tests stub out.
type checkUC interface {
	Execute(ctx context.Context, output io.Writer) error
}

func (s *Server) handleCheck(c *echo.Context) error {
	ctx := c.Request().Context()

	// Lead the response with the effective configuration and the source of each
	// value, so a successful check reports what it actually ran against.
	var buff strings.Builder
	buff.WriteString("Configuration:\n")
	if err := s.params.WriteTable(&buff); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}

	uc, err := s.config.newCheck(ctx, s.params)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}

	if err := uc.Execute(ctx, &buff); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}

	return c.String(http.StatusOK, buff.String())
}
