package core

import (
	"context"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type Backup interface {
	// Create backuppb
	CreateBackup(context.Context, *backuppb.CreateBackupRequest) (*backuppb.BackupInfoResponse, error)
	// Get backuppb with the chosen name
	GetBackup(context.Context, *backuppb.GetBackupRequest) (*backuppb.BackupInfoResponse, error)
	// List backups that contains the given collection name, if collection is not given, return all backups in the cluster
	ListBackups(context.Context, *backuppb.ListBackupsRequest) (*backuppb.ListBackupsResponse, error)
	// Delete backuppb by given backuppb name
	DeleteBackup(context.Context, *backuppb.DeleteBackupRequest) (*backuppb.DeleteBackupResponse, error)
	// Restore backuppb to milvus, return backup restore report
	RestoreBackup(context.Context, *backuppb.RestoreBackupRequest) (*backuppb.RestoreBackupResponse, error)
	// Get restore state by given id
	GetRestore(context.Context, *backuppb.GetRestoreStateRequest) (*backuppb.RestoreBackupResponse, error)

	// Copy backuppb between buckets
	//CopyBackup(context.Context, *backuppb.CopyBackupRequest) (*backuppb.CopyBackupResponse, error)
}
