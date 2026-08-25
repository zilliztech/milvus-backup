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

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
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

// Execute returns one summary per readable backup. A backup whose meta cannot
// be read is skipped, so one corrupted backup does not hide the rest.
func (uc *ListBackups) Execute(ctx context.Context) ([]*backuppb.BackupSummary, error) {
	return meta.List(ctx, uc.cli, uc.rootPath)
}
