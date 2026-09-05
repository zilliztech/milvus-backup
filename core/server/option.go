package server

import (
	"context"
	"strings"

	"github.com/zilliztech/milvus-backup/app"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// Config for setting params used by server.
type config struct {
	port string

	// newListBackups builds the usecase a list request runs through. The
	// default wires the real one; tests replace it with a stub so handler
	// tests never touch storage.
	newListBackups func(ctx context.Context, params *v2.Config) (listBackupsUC, error)

	// newDeleteBackup is the delete counterpart of newListBackups.
	newDeleteBackup func(ctx context.Context, params *v2.Config) (deleteBackupUC, error)

	// newGetRestore is the get-restore counterpart of newListBackups. It
	// takes no config: restore state is process-local, so there is no client
	// to build and construction cannot fail. The error stays so the seam
	// matches the other constructors.
	newGetRestore func() (getRestoreUC, error)
}

func newDefaultConfig() *config {
	return &config{
		port: ":8080",
		// Go function types do not convert covariantly, so the concrete
		// *app.ListBackups needs this thin wrapper to become the interface.
		newListBackups: func(ctx context.Context, params *v2.Config) (listBackupsUC, error) {
			return app.NewListBackups(ctx, params)
		},
		newDeleteBackup: func(ctx context.Context, params *v2.Config) (deleteBackupUC, error) {
			return app.NewDeleteBackup(ctx, params)
		},
		newGetRestore: func() (getRestoreUC, error) {
			return app.NewGetRestore(), nil
		},
	}
}

// Option is used to config the retry function.
type Option func(cfg *config)

// Port is the addr the HTTP server listens on.
func Port(port string) Option {
	return func(c *config) {
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}
		c.port = port
	}
}
