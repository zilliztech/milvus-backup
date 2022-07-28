package core

import (
	"context"
	proto "github.com/zilliztech/milvus-backup/proto/backuppb"
)

type Backup interface {
	// Create backup
	CreateBackup(context.Context, *proto.CreateBackupRequest) (*proto.CreateBackupResponse, error)
	// Get backup with the chosen name
	GetBackup(context.Context, *proto.GetBackupRequest) (*proto.GetBackupResponse, error)
	// List backups that contains the given collection name, if collection is not given, return all backups in the cluster
	ListBackups(context.Context, *proto.ListBackupsRequest) (*proto.ListBackupsResponse, error)
	// Delete backup by given backup name
	DeleteBackup(context.Context, *proto.DeleteBackupRequest) (*proto.DeleteBackupResponse, error)
	// Load backup to milvus, return backup load report
	LoadBackup(context.Context, *proto.LoadBackupRequest) (*proto.LoadBackupResponse, error)
}
