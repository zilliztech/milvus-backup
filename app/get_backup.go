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
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// GetBackup reads one backup: the persisted meta of the artifact overlaid,
// when a task for it is known, with the ephemeral progress of the job that
// produced or is producing it.
type GetBackup struct {
	cli      storage.Client
	taskMgr  *taskmgr.Mgr
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
		taskMgr:  taskmgr.DefaultMgr(),
		rootPath: params.Backup.Storage.RootPath.Val,
	}, nil
}

// GetBackupRequest selects the backup to read. Name and ID are alternative
// selectors; the ID wins when both are set, because only the ID can pin down
// one of several tasks that shared the name. Path overrides the configured
// backup root path.
type GetBackupRequest struct {
	Name string
	ID   string
	Path string
}

// BackupView keeps the two resource halves separate. Merging them is a
// rendering decision of the transport, not a property of the action.
type BackupView struct {
	// Task is the job half; nil when no task is known, e.g. for an artifact
	// whose creating process has since restarted.
	Task taskmgr.BackupTaskView
	// Meta is the artifact half; nil while the job is still in flight and
	// has persisted nothing yet.
	Meta     *backuppb.BackupInfo
	MetaSize int64
}

// Execute reads the persisted meta whenever it exists and overlays the task
// view when one is known. It errors only when neither exists: a selector
// with nothing behind it is an error, not a silent success. A task that
// reports success while its meta is unreadable is an error too — the
// artifact the task claims to have produced is gone.
func (uc *GetBackup) Execute(ctx context.Context, req GetBackupRequest) (*BackupView, error) {
	if req.Name == "" && req.ID == "" {
		return nil, fmt.Errorf("app: empty backup name and backup id")
	}

	rootPath := uc.rootPath
	if req.Path != "" {
		rootPath = req.Path
	}

	var task taskmgr.BackupTaskView
	name := req.Name
	if req.ID != "" {
		byID, err := uc.taskMgr.GetBackupTask(req.ID)
		if err != nil {
			return nil, fmt.Errorf("app: get backup task %w", err)
		}
		task = byID
		name = byID.Name()
	} else {
		byName, err := uc.taskMgr.GetBackupTaskByName(req.Name)
		if err != nil && !errors.Is(err, taskmgr.ErrTaskNotFound) {
			return nil, fmt.Errorf("app: get backup task %w", err)
		}
		task = byName
	}

	metaInfo, metaSize, err := uc.readMeta(ctx, rootPath, name)
	if err != nil {
		return nil, err
	}

	switch {
	case metaInfo != nil:
		return &BackupView{Task: task, Meta: metaInfo, MetaSize: metaSize}, nil
	case task != nil && task.StateCode() == backuppb.BackupTaskStateCode_BACKUP_SUCCESS:
		return nil, fmt.Errorf("app: backup task %s reports success but its meta is missing", name)
	case task != nil:
		return &BackupView{Task: task}, nil
	default:
		return nil, fmt.Errorf("app: backup %s not found", name)
	}
}

// readMeta returns nils when the backup dir has no meta, which is the
// in-flight case, and an error when storage fails or the meta is corrupted.
func (uc *GetBackup) readMeta(ctx context.Context, rootPath, name string) (*backuppb.BackupInfo, int64, error) {
	backupDir := mpath.BackupDir(rootPath, name)
	exist, err := meta.Exist(ctx, uc.cli, backupDir)
	if err != nil {
		return nil, 0, fmt.Errorf("app: check backup exist %w", err)
	}
	if !exist {
		return nil, 0, nil
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
