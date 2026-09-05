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

	// newRestoreBackup builds the usecase a restore request runs through.
	// Unlike list and delete its constructor cannot fail: the storage clients
	// are created per Start call, once the request's own bucket override is
	// known.
	newRestoreBackup func(params *v2.Config) restoreBackupUC

	// newRestoreSecondary is the secondary-restore counterpart of
	// newRestoreBackup.
	newRestoreSecondary func(params *v2.Config) restoreSecondaryUC
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
		newRestoreBackup: func(params *v2.Config) restoreBackupUC {
			return app.NewRestore(params)
		},
		newRestoreSecondary: func(params *v2.Config) restoreSecondaryUC {
			return app.NewRestoreSecondary(params)
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
