package core

import (
	"context"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type Backup interface {
	// Create backuppb
	CreateBackup(context.Context, *backuppb.CreateBackupRequest) *backuppb.BackupInfoResponse
	// Get backuppb with the chosen name
	GetBackup(context.Context, *backuppb.GetBackupRequest) *backuppb.BackupInfoResponse
	// List backups that contains the given collection name, if collection is not given, return all backups in the cluster
	ListBackups(context.Context, *backuppb.ListBackupsRequest) *backuppb.ListBackupsResponse
	// Delete backuppb by given backuppb name
	DeleteBackup(context.Context, *backuppb.DeleteBackupRequest) *backuppb.DeleteBackupResponse
	// Get restore state by given id
	GetRestore(context.Context, *backuppb.GetRestoreStateRequest) *backuppb.RestoreBackupResponse
}
