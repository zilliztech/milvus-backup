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
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1, get collection list

	// 2, get collection partition segment meta

	// 3, Flush

	// 4, copy data

	// 5, wrap meta

	panic("implement me")
}

func (b BackupContext) GetBackup(ctx context.Context, request *proto.GetBackupRequest) (*proto.GetBackupResponse, error) {
	// 1, trigger inner sync to get the newest backup list in the milvus cluster

	// 2, get wanted backup
	panic("implement me")
}

func (b BackupContext) ListBackups(ctx context.Context, request *proto.ListBackupsRequest) (*proto.ListBackupsResponse, error) {
	// 1, trigger inner sync to get the newest backup list in the milvus cluster

	// 2, list wanted backup
	panic("implement me")
}

func (b BackupContext) DeleteBackup(ctx context.Context, request *proto.DeleteBackupRequest) (*proto.DeleteBackupResponse, error) {
	// 1, delete the backup
	panic("implement me")
}

func (b BackupContext) LoadBackup(ctx context.Context, request *proto.LoadBackupRequest) (*proto.LoadBackupResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 1, validate

	// 2, create collections

	// 3, execute bulkload

	// 4, collect stats and return report
	panic("implement me")
}
