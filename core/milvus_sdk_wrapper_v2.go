package core

import (
	"context"
	"time"

	entityV2 "github.com/milvus-io/milvus/client/v2/entity"
	indexV2 "github.com/milvus-io/milvus/client/v2/index"
	milvusClientV2 "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

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

func (m *MilvusClient) DescribeCollectionV2(ctx context.Context, db, collName string) (*entityV2.Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.milvusClientV2.UsingDatabase(ctx, milvusClientV2.NewUsingDatabaseOption(db))
	if err != nil {
		return nil, err
	}
	return m.milvusClientV2.DescribeCollection(ctx, milvusClientV2.NewDescribeCollectionOption(collName))
}

func (m *MilvusClient) CreateCollectionV2(ctx context.Context, db string, schema *entityV2.Schema, shardsNum int32, cl entityV2.ConsistencyLevel, partitionNum int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.milvusClientV2.UsingDatabase(ctx, milvusClientV2.NewUsingDatabaseOption(db))
	if err != nil {
		return err
	}
	// add retry to make sure won't be block by rate control
	return retry.Do(ctx, func() error {
		option := milvusClientV2.NewCreateCollectionOption(schema.CollectionName, &entityV2.Schema{
			CollectionName:     schema.CollectionName,
			Description:        schema.Description,
			AutoID:             schema.AutoID,
			Fields:             schema.Fields,
			EnableDynamicField: schema.EnableDynamicField,
			Functions:          schema.Functions,
		}).WithShardNum(shardsNum).WithConsistencyLevel(cl)
		if partitionNum != 0 {
			option = option.WithPartitionNum(partitionNum)
		}
		return m.milvusClientV2.CreateCollection(ctx, option)
	}, retry.Sleep(2*time.Second), retry.Attempts(10))
}

func (m *MilvusClient) CreateIndexV2(ctx context.Context, db, collName string, fieldName string, idx indexV2.Index, async bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.milvusClientV2.UsingDatabase(ctx, milvusClientV2.NewUsingDatabaseOption(db))
	if err != nil {
		return err
	}
	_, err = m.milvusClientV2.CreateIndex(ctx, milvusClientV2.NewCreateIndexOption(collName, fieldName, idx))
	if err != nil {
		return err
	}
	return nil
}
