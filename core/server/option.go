package server

import (
	"context"
	"strings"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// Config for setting params used by server.
type config struct {
	port string

	// newListBackups builds the usecase a list request runs through. The
	// default wires the real one; tests replace it with a stub so handler
	// tests never touch storage.
	newListBackups func(ctx context.Context, params *cfg.Config) (listBackupsUC, error)

	// newDeleteBackup is the delete counterpart of newListBackups.
	newDeleteBackup func(ctx context.Context, params *cfg.Config) (deleteBackupUC, error)

	// newGetRestore is the get-restore counterpart of newListBackups. It
	// takes no config: restore state is process-local, so there is no client
	// to build and construction cannot fail. The error stays so the seam
	// matches the other constructors.
	newGetRestore func() (getRestoreUC, error)

	// newCheck is the check counterpart of newListBackups.
	newCheck func(ctx context.Context, params *cfg.Config) (checkUC, error)

	// newGetBackup is the get counterpart of newListBackups.
	newGetBackup func(ctx context.Context, params *cfg.Config) (getBackupUC, error)

	// newGetBackupTask is the get-backup-task counterpart of newGetRestore:
	// job state is process-local, so there is no client to build and
	// construction cannot fail. The error stays so the seam matches the
	// other constructors.
	newGetBackupTask func() (getBackupTaskUC, error)

	// newBackupJob is the create counterpart of newListBackups, extended with
	// the request: a backup job is per-request, so building it and registering
	// it are one step.
	newBackupJob backupJobFactory

	// newRestoreJob builds and registers the job for one restore request: a
	// restore job is per-request, so building it and registering it are one
	// step.
	newRestoreJob restoreJobFactory

	// newRestoreSecondaryJob is the secondary-restore counterpart of
	// newRestoreJob.
	newRestoreSecondaryJob restoreSecondaryJobFactory
}

func newDefaultConfig() *config {
	return &config{
		port: ":8080",
		// Go function types do not convert covariantly, so the concrete
		// *app.ListBackups needs this thin wrapper to become the interface.
		newListBackups: func(ctx context.Context, params *cfg.Config) (listBackupsUC, error) {
			return app.NewListBackups(ctx, params)
		},
		newDeleteBackup: func(ctx context.Context, params *cfg.Config) (deleteBackupUC, error) {
			return app.NewDeleteBackup(ctx, params)
		},
		newGetRestore: func() (getRestoreUC, error) {
			return app.NewGetRestore(jobstate.Default()), nil
		},
		newCheck: func(ctx context.Context, params *cfg.Config) (checkUC, error) {
			return app.NewCheck(ctx, params)
		},
		newGetBackup: func(ctx context.Context, params *cfg.Config) (getBackupUC, error) {
			return app.NewGetBackup(ctx, params)
		},
		newGetBackupTask: func() (getBackupTaskUC, error) {
			return app.NewGetBackupTask(jobstate.Default()), nil
		},
		newBackupJob: func(ctx context.Context, params *cfg.Config, req app.CreateBackupRequest) (backupJob, error) {
			return app.NewBackupJob(ctx, params, jobstate.Default(), req)
		},
		newRestoreJob: func(ctx context.Context, params *cfg.Config, req app.RestoreRequest) (restoreJob, error) {
			return app.NewRestoreJob(ctx, params, jobstate.Default(), req)
		},
		newRestoreSecondaryJob: func(ctx context.Context, params *cfg.Config, req app.RestoreSecondaryRequest) (restoreJob, error) {
			return app.NewRestoreSecondaryJob(ctx, params, jobstate.Default(), req)
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
