package core

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"

	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

// MilvusClient wrap db into milvus API to make it thread safe
type MilvusClient struct {
	mu     sync.Mutex
	client gomilvus.Client
}

func (m *MilvusClient) Close() error {
	return m.client.Close()
}

func (m *MilvusClient) GetVersion(ctx context.Context) (string, error) {
	return m.client.GetVersion(ctx)
}

func (m *MilvusClient) CreateDatabase(ctx context.Context, dbName string) error {
	return m.client.CreateDatabase(ctx, dbName)
}

func (m *MilvusClient) ListDatabases(ctx context.Context) ([]entity.Database, error) {
	return m.client.ListDatabases(ctx)
}

func (m *MilvusClient) DescribeCollection(ctx context.Context, db, collName string) (*entity.Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return nil, err
	}
	return m.client.DescribeCollection(ctx, collName)
}

func (m *MilvusClient) DescribeIndex(ctx context.Context, db, collName, fieldName string) ([]entity.Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return nil, err
	}
	return m.client.DescribeIndex(ctx, collName, fieldName)
}

func (m *MilvusClient) ShowPartitions(ctx context.Context, db, collName string) ([]*entity.Partition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return nil, err
	}
	return m.client.ShowPartitions(ctx, collName)
}

func (m *MilvusClient) GetLoadingProgress(ctx context.Context, db, collName string, partitionNames []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return 0, err
	}
	return m.client.GetLoadingProgress(ctx, collName, partitionNames)
}

func (m *MilvusClient) GetPersistentSegmentInfo(ctx context.Context, db, collName string) ([]*entity.Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return nil, err
	}
	return m.client.GetPersistentSegmentInfo(ctx, collName)
}

func (m *MilvusClient) FlushV2(ctx context.Context, db, collName string, async bool) ([]int64, []int64, int64, map[string]msgpb.MsgPosition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	return m.client.FlushV2(ctx, collName, async)
}

func (m *MilvusClient) ListCollections(ctx context.Context, db string) ([]*entity.Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return nil, err
	}
	return m.client.ListCollections(ctx)
}

func (m *MilvusClient) HasCollection(ctx context.Context, db, collName string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return false, err
	}
	return m.client.HasCollection(ctx, collName)
}

func (m *MilvusClient) BulkInsert(ctx context.Context, db, collName string, partitionName string, files []string, opts ...gomilvus.BulkInsertOption) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return 0, err
	}
	return m.client.BulkInsert(ctx, collName, partitionName, files, opts...)
}

func (m *MilvusClient) GetBulkInsertState(ctx context.Context, taskID int64) (*entity.BulkInsertTaskState, error) {
	return m.client.GetBulkInsertState(ctx, taskID)
}

func (m *MilvusClient) CreateCollection(ctx context.Context, db string, schema *entity.Schema, shardsNum int32, opts ...gomilvus.CreateCollectionOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return err
	}
	// add retry to make sure won't be block by rate control
	return retry.Do(ctx, func() error {
		return m.client.CreateCollection(ctx, schema, shardsNum, opts...)
	}, retry.Sleep(2*time.Second), retry.Attempts(10))
}

func (m *MilvusClient) DropCollection(ctx context.Context, db string, collectionName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return err
	}
	// add retry to make sure won't be block by rate control
	return retry.Do(ctx, func() error {
		return m.client.DropCollection(ctx, collectionName)
	}, retry.Sleep(2*time.Second), retry.Attempts(10))
}

func (m *MilvusClient) CreatePartition(ctx context.Context, db, collName string, partitionName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return err
	}
	return retry.Do(ctx, func() error {
		return m.client.CreatePartition(ctx, collName, partitionName)
	}, retry.Sleep(2*time.Second), retry.Attempts(10))
}

func (m *MilvusClient) HasPartition(ctx context.Context, db, collName string, partitionName string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return false, err
	}
	return m.client.HasPartition(ctx, collName, partitionName)
}

func (m *MilvusClient) CreateIndex(ctx context.Context, db, collName string, fieldName string, idx entity.Index, async bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return err
	}
	return m.client.CreateIndex(ctx, collName, fieldName, idx, async, gomilvus.WithIndexName(idx.Name()))
}

func (m *MilvusClient) DropIndex(ctx context.Context, db, collName string, indexName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.client.UsingDatabase(ctx, db)
	if err != nil {
		return err
	}
	return m.client.DropIndex(ctx, collName, "", gomilvus.WithIndexName(indexName))
}

func (m *MilvusClient) BackupRBAC(ctx context.Context) (*entity.RBACMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.client.BackupRBAC(ctx)
}

func (m *MilvusClient) RestoreRBAC(ctx context.Context, rbacMeta *entity.RBACMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.client.RestoreRBAC(ctx, rbacMeta)
}
