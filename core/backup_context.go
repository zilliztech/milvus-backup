package core

import (
	"context"
	"sync"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
)

// makes sure BackupContext implements `Backup`
var _ Backup = (*BackupContext)(nil)

type BackupContext struct {
	ctx          context.Context
	milvusSource *MilvusSource
	backupInfos  []proto.BackupInfo
	// lock to make sure only one backup is creating or loading
	mu sync.Mutex
	//milvusClient
	//metaClient   etcdclient
	//storageClient minioclient
}

func (b *BackupContext) GetMilvusSource() *MilvusSource {
	return b.milvusSource
}

func (b *BackupContext) SetMilvusSource(milvusSource *MilvusSource) {
	b.milvusSource = milvusSource
}

func CreateBackupContext(ctx context.Context, params paramtable.ComponentParam) *BackupContext {
	return &BackupContext{
		ctx: ctx,
		milvusSource: &MilvusSource{
			params: params,
		},
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
