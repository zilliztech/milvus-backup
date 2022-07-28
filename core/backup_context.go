package core

import (
	"context"
	"sync"

	proto "github.com/zilliztech/milvus-backup/proto/backuppb"
)

// makes sure BackupContext implements `Backup`
var _ Backup = (*BackupContext)(nil)

type BackupContext struct {
	ctx           context.Context
	mivlus_source MilvusSource
	backupInfos   []proto.BackupInfo
	// lock to make sure only one backup is creating or loading
	mu sync.Mutex
	//milvusClient
	//metaClient   etcdclient
	//storageClient minioclient
}

func NewBackupContext(ctx context.Context) *BackupContext {
	return &BackupContext{
		ctx: ctx,
		//mivlus_source: mivlus_source,
		//backupInfos: backupInfos,
	}
}

func (b BackupContext) CreateBackup(ctx context.Context, request *proto.CreateBackupRequest) (*proto.CreateBackupResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (b BackupContext) GetBackup(ctx context.Context, request *proto.GetBackupRequest) (*proto.GetBackupResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (b BackupContext) ListBackups(ctx context.Context, request *proto.ListBackupsRequest) (*proto.ListBackupsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (b BackupContext) DeleteBackup(ctx context.Context, request *proto.DeleteBackupRequest) (*proto.DeleteBackupResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (b BackupContext) LoadBackup(ctx context.Context, request *proto.LoadBackupRequest) (*proto.LoadBackupResponse, error) {
	//TODO implement me
	panic("implement me")
}
