// Package app is the usecase layer between the transports and the internals.
// Each action is its own struct holding exactly the dependencies it needs;
// there is no shared object, so actions cannot couple to each other. Input and
// output are transport-neutral: flag parsing, query parameters, rejection of
// deprecated fields and rendering stay in cmd and core/server, which import
// this package and nothing below it.
package app

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// ListBackups lists the backup artifacts kept in the backup storage.
type ListBackups struct {
	cli      storage.Client
	rootPath string
}

// NewListBackups builds the usecase from config, creating the backup storage
// client itself so the transports never import internal/storage. The client
// is created per call; sharing one across calls is a lifecycle decision this
// layer deliberately does not make.
func NewListBackups(ctx context.Context, params *v2.Config) (*ListBackups, error) {
	cli, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return &ListBackups{
		cli:      cli,
		rootPath: params.Backup.Storage.RootPath.Val,
	}, nil
}

// BackupSummary is the artifact summary as list presents it. It is not
// backuppb.BackupSummary on purpose: that type is the v1 wire shape, and wire
// shapes belong to the transports, which render this view into whatever their
// contract says.
type BackupSummary struct {
	ID            string
	Name          string
	Size          int64
	MilvusVersion string
}

// Execute returns one summary per readable backup. A backup whose meta cannot
// be read is skipped, so one corrupted backup does not hide the rest.
func (uc *ListBackups) Execute(ctx context.Context) ([]BackupSummary, error) {
	backupDirs, _, err := storage.ListPrefixFlat(ctx, uc.cli, mpath.BackupRootDir(uc.rootPath), false)
	if err != nil {
		return nil, fmt.Errorf("app: list backup root %s: %w", uc.rootPath, err)
	}
	log.Info("list backup dirs", zap.Strings("dirs", backupDirs))

	summaries := make([]BackupSummary, 0, len(backupDirs))
	for _, backupDir := range backupDirs {
		info, err := meta.Read(ctx, uc.cli, backupDir)
		if err != nil {
			log.Warn("can not read backup info, skip it", zap.String("backup_dir", backupDir))
			continue
		}

		summaries = append(summaries, BackupSummary{
			ID:            info.GetId(),
			Name:          info.GetName(),
			Size:          info.GetSize(),
			MilvusVersion: info.GetMilvusVersion(),
		})
	}

	return summaries, nil
}
