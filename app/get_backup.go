package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// ErrBackupNotFound reports a selector with no artifact behind it. Callers
// that merge the artifact with the job half (the v1 get_backup contract) use
// it to tell "no backup persisted yet" apart from a storage failure.
var ErrBackupNotFound = errors.New("backup not found")

// GetBackup reads one backup artifact: the persisted meta in the backup
// storage. The job that produced it is a different resource with its own
// usecase, GetBackupTask — the two are read separately so transports with a
// split API (v2) map one endpoint to one usecase, and a transport with a
// merged contract (v1) does the merging itself.
type GetBackup struct {
	cli      storage.Client
	rootPath string
}

// NewGetBackup builds the usecase from config, creating the backup storage
// client itself so the transports never import internal/storage. The client
// is created per call; sharing one across calls is a lifecycle decision this
// layer deliberately does not make.
func NewGetBackup(ctx context.Context, params *v2.Config) (*GetBackup, error) {
	cli, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return &GetBackup{
		cli:      cli,
		rootPath: params.Backup.Storage.RootPath.Val,
	}, nil
}

// Execute returns the persisted meta of the named backup and the size of its
// meta dir. A name with nothing behind it is ErrBackupNotFound, not a silent
// success: an in-flight job that has persisted nothing yet is answered
// through GetBackupTask, not here.
func (uc *GetBackup) Execute(ctx context.Context, name string) (*backuppb.BackupInfo, int64, error) {
	if name == "" {
		return nil, 0, fmt.Errorf("app: empty backup name")
	}

	backupDir := mpath.BackupDir(uc.rootPath, name)
	exist, err := meta.Exist(ctx, uc.cli, backupDir)
	if err != nil {
		return nil, 0, fmt.Errorf("app: check backup exist %w", err)
	}
	if !exist {
		return nil, 0, fmt.Errorf("app: backup %s: %w", name, ErrBackupNotFound)
	}

	metaInfo, err := meta.Read(ctx, uc.cli, backupDir)
	if err != nil {
		return nil, 0, fmt.Errorf("app: read backup meta %w", err)
	}

	metaSize, err := storage.Size(ctx, uc.cli, mpath.MetaDir(backupDir))
	if err != nil {
		return nil, 0, fmt.Errorf("app: get meta size %w", err)
	}

	return metaInfo, metaSize, nil
}
