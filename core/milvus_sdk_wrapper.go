package core

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"

	entityV2 "github.com/milvus-io/milvus/client/v2/entity"
	milvusClientV2 "github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

// MilvusClient wrap db into milvus API to make it thread safe
type MilvusClient struct {
	mu     sync.Mutex
	client gomilvus.Client
	// sdk v2
	milvusClientV2 *milvusClientV2.Client
}

func newMilvusClient(ctx context.Context, params paramtable.BackupParams) (*MilvusClient, error) {
	milvusEndpoint := params.MilvusCfg.Address + ":" + params.MilvusCfg.Port
	log.Debug("Start Milvus client", zap.String("endpoint", milvusEndpoint))
	var c gomilvus.Client
	var err error
	if params.MilvusCfg.AuthorizationEnabled && params.MilvusCfg.User != "" && params.MilvusCfg.Password != "" {
		if params.MilvusCfg.TLSMode == 0 {
			c, err = gomilvus.NewDefaultGrpcClientWithAuth(ctx, milvusEndpoint, params.MilvusCfg.User, params.MilvusCfg.Password)
		} else if params.MilvusCfg.TLSMode == 1 || params.MilvusCfg.TLSMode == 2 {
			c, err = gomilvus.NewDefaultGrpcClientWithTLSAuth(ctx, milvusEndpoint, params.MilvusCfg.User, params.MilvusCfg.Password)
		} else {
			log.Error("milvus.TLSMode is not illegal, support value 0, 1, 2")
			return nil, errors.New("milvus.TLSMode is not illegal, support value 0, 1, 2")
		}
	} else {
		c, err = gomilvus.NewGrpcClient(ctx, milvusEndpoint)
	}
	if err != nil {
		log.Error("failed to connect to milvus", zap.Error(err))
		return nil, err
	}

	milvusClientV2, err := milvusClientV2.New(ctx, &milvusClientV2.ClientConfig{
		Address:       milvusEndpoint,
		Username:      params.MilvusCfg.User,
		Password:      params.MilvusCfg.Password,
		EnableTLSAuth: params.MilvusCfg.AuthorizationEnabled,
	})
	if err != nil {
		log.Error("failed to initial milvus client v2", zap.Error(err))
		return nil, err
	}

	return &MilvusClient{
		client:         c,
		milvusClientV2: milvusClientV2,
	}, nil
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

func (m *MilvusClient) ListCollectionsV2(ctx context.Context, db string) ([]*entityV2.Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.milvusClientV2.UsingDatabase(ctx, milvusClientV2.NewUsingDatabaseOption(db))
	if err != nil {
		return nil, err
	}

	collections, err := m.milvusClientV2.ListCollections(ctx, milvusClientV2.NewListCollectionOption())
	if err != nil {
		return nil, err
	}

	collectionEntities := make([]*entityV2.Collection, 0)
	for _, collection := range collections {
		coll, err := m.milvusClientV2.DescribeCollection(ctx, milvusClientV2.NewDescribeCollectionOption(collection))
		if err != nil {
			return nil, err
		}
		collectionEntities = append(collectionEntities, coll)
	}

	return collectionEntities, nil
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
