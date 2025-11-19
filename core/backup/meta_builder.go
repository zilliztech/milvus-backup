package backup

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type metaBuilder struct {
	mu sync.Mutex

	data *backuppb.BackupInfo
}

func newMetaBuilder(taskID, backupName string) *metaBuilder {
	info := &backuppb.BackupInfo{Id: taskID, Name: backupName}
	return &metaBuilder{data: info}
}

func (c *metaBuilder) setVersion(version string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.MilvusVersion = version
}

func (c *metaBuilder) addDatabase(databaseBackup *backuppb.DatabaseBackupInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.DatabaseBackups = append(c.data.DatabaseBackups, databaseBackup)
}

func (c *metaBuilder) addCollection(collectionBackup *backuppb.CollectionBackupInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.CollectionBackups = append(c.data.CollectionBackups, collectionBackup)
	c.data.Size += collectionBackup.GetSize()
}

func (c *metaBuilder) setRBACMeta(rbacMeta *backuppb.RBACMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.RbacMeta = rbacMeta
}

func (c *metaBuilder) setRPCChannelInfo(rpcChannelInfo *backuppb.RPCChannelInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.RpcChannelInfo = rpcChannelInfo
}

func (c *metaBuilder) buildBackupMeta() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := &backuppb.BackupInfo{
		Id:              c.data.GetId(),
		Name:            c.data.GetName(),
		BackupTimestamp: c.data.GetBackupTimestamp(),
		Size:            c.data.GetSize(),
		MilvusVersion:   c.data.GetMilvusVersion(),
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *metaBuilder) buildFullMeta() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.Marshal(c.data)
	if err != nil {
		return nil, fmt.Errorf("backup: build full meta: %w", err)
	}

	return data, nil
}

func (c *metaBuilder) buildCollectionMeta() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := &backuppb.CollectionLevelBackupInfo{Infos: c.data.CollectionBackups}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build collection meta: %w", err)
	}

	return data, nil
}

func (c *metaBuilder) buildPartitionMeta() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	partitions := make([]*backuppb.PartitionBackupInfo, 0)
	for _, collection := range c.data.CollectionBackups {
		partitions = append(partitions, collection.GetPartitionBackups()...)
	}

	info := &backuppb.PartitionLevelBackupInfo{Infos: partitions}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build partition meta: %w", err)
	}

	return data, nil
}

func (c *metaBuilder) buildSegmentMeta() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	segments := make([]*backuppb.SegmentBackupInfo, 0)
	for _, collection := range c.data.CollectionBackups {
		for _, partition := range collection.GetPartitionBackups() {
			segments = append(segments, partition.GetSegmentBackups()...)
		}
	}

	info := &backuppb.SegmentLevelBackupInfo{Infos: segments}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build segment meta: %w", err)
	}

	return data, nil
}
