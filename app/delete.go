package app

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// DeleteBackup deletes one backup artifact from the backup storage.
type DeleteBackup struct {
	cli      storage.Client
	rootPath string
}

// NewDeleteBackup builds the usecase from config, creating the backup storage
// client itself so the transports never import internal/storage. The client
// is created per call; sharing one across calls is a lifecycle decision this
// layer deliberately does not make.
func NewDeleteBackup(ctx context.Context, params *v2.Config) (*DeleteBackup, error) {
	cli, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return &DeleteBackup{
		cli:      cli,
		rootPath: params.Backup.Storage.RootPath.Val,
	}, nil
}

// Execute removes the named backup's directory. The meta must be readable
// first: a delete that cannot prove what it is deleting is refused, so a
// wrong root path or a corrupted artifact stops here instead of wiping
// whatever prefix happens to sit under the same path. A name that has no
// backup behind it is an error, not a silent success.
func (uc *DeleteBackup) Execute(ctx context.Context, name string) error {
	backupDir := mpath.BackupDir(uc.rootPath, name)
	if _, err := meta.Read(ctx, uc.cli, backupDir); err != nil {
		return fmt.Errorf("app: read backup info: %w", err)
	}

	log.Info("start delete backup", zap.String("backup_dir", backupDir))
	start := time.Now()
	if err := storage.DeletePrefix(ctx, uc.cli, backupDir); err != nil {
		return fmt.Errorf("app: delete backup dir %s: %w", backupDir, err)
	}
	log.Info("delete backup done", zap.String("backup_dir", backupDir), zap.Duration("cost", time.Since(start)))

	return nil
}
