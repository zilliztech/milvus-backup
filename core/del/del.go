package del

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

type Task struct {
	backupStorage storage.Client
	backupDir     string

	logger *zap.Logger
}

func NewTask(backupStorage storage.Client, backupDir string) *Task {
	return &Task{
		backupStorage: backupStorage,
		backupDir:     backupDir,
		logger:        log.With(zap.String("backup_dir", backupDir)),
	}
}

func (t *Task) Execute(ctx context.Context) error {
	_, err := meta.Read(ctx, t.backupStorage, t.backupDir)
	if err != nil {
		return fmt.Errorf("delete: read backup info: %w", err)
	}

	t.logger.Info("start delete backup")
	start := time.Now()
	if err := storage.DeletePrefix(ctx, t.backupStorage, t.backupDir); err != nil {
		return fmt.Errorf("delete: delete backup dir: %w", err)
	}
	t.logger.Info("delete backup done", zap.Duration("cost", time.Since(start)))

	return nil
}
