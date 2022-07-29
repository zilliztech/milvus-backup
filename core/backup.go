package core

import (
	"context"
	backuppb "github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type Backup interface {
	// Create backuppb
	CreateBackup(context.Context, *backuppb.CreateBackupRequest) (*backuppb.CreateBackupResponse, error)
	// Get backuppb with the chosen name
	GetBackup(context.Context, *backuppb.GetBackupRequest) (*backuppb.GetBackupResponse, error)
	// List backups that contains the given collection name, if collection is not given, return all backups in the cluster
	ListBackups(context.Context, *backuppb.ListBackupsRequest) (*backuppb.ListBackupsResponse, error)
	// Delete backuppb by given backuppb name
	DeleteBackup(context.Context, *backuppb.DeleteBackupRequest) (*backuppb.DeleteBackupResponse, error)
	// Load backuppb to milvus, return backuppb load report
	LoadBackup(context.Context, *backuppb.LoadBackupRequest) (*backuppb.LoadBackupResponse, error)
}
