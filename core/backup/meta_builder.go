package backup

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

type metaBuilder struct {
	mu sync.Mutex

	data *backuppb.BackupInfo

	// index
	nsToCollID        map[namespace.NS]int64
	collectionBackups map[int64]*backuppb.CollectionBackupInfo          // coll id - > collection backup info
	partitionBackups  map[int64]map[int64]*backuppb.PartitionBackupInfo // coll id -> part id - > partition backup info
}

func newMetaBuilder(taskID, backupName string) *metaBuilder {
	info := &backuppb.BackupInfo{Id: taskID, Name: backupName}
	return &metaBuilder{
		data: info,

		nsToCollID:        make(map[namespace.NS]int64),
		collectionBackups: make(map[int64]*backuppb.CollectionBackupInfo),
		partitionBackups:  make(map[int64]map[int64]*backuppb.PartitionBackupInfo),
	}
}

func (builder *metaBuilder) setVersion(version string) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.MilvusVersion = version
}

func (builder *metaBuilder) setClusterInfoAndTSS(cch string, pchs []string, flushAllTss map[string]string) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.FlushAllMsgsBase64 = flushAllTss
	builder.data.PhysicalChannelNames = pchs
	builder.data.ControlChannelName = cch
}

func (builder *metaBuilder) addDatabase(databaseBackup *backuppb.DatabaseBackupInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.DatabaseBackups = append(builder.data.DatabaseBackups, databaseBackup)
}

func (builder *metaBuilder) addCollection(ns namespace.NS, collectionBackup *backuppb.CollectionBackupInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.CollectionBackups = append(builder.data.CollectionBackups, collectionBackup)
	// add to index
	builder.nsToCollID[ns] = collectionBackup.GetCollectionId()
	builder.collectionBackups[collectionBackup.GetCollectionId()] = collectionBackup
	for _, partition := range collectionBackup.GetPartitionBackups() {
		if _, ok := builder.partitionBackups[collectionBackup.GetCollectionId()]; !ok {
			builder.partitionBackups[collectionBackup.GetCollectionId()] = make(map[int64]*backuppb.PartitionBackupInfo)
		}
		builder.partitionBackups[collectionBackup.GetCollectionId()][partition.GetPartitionId()] = partition
	}
}

func (builder *metaBuilder) addPOS(ns namespace.NS, channelCP map[string]string, maxChannelTS uint64, sealTime uint64) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	collID := builder.nsToCollID[ns]
	collBackup := builder.collectionBackups[collID]

	collBackup.ChannelCheckpoints = channelCP
	collBackup.BackupTimestamp = maxChannelTS
	collBackup.BackupPhysicalTimestamp = sealTime
}

func (builder *metaBuilder) addSegments(segments []*backuppb.SegmentBackupInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	for _, segment := range segments {
		builder.data.Size += segment.GetSize()

		collBackup := builder.collectionBackups[segment.GetCollectionId()]
		collBackup.Size += segment.GetSize()

		if segment.GetIsL0() && segment.GetPartitionId() == _allPartitionID {
			collBackup.L0Segments = append(collBackup.L0Segments, segment)
		} else {
			partBackup := builder.partitionBackups[segment.GetCollectionId()][segment.GetPartitionId()]
			partBackup.SegmentBackups = append(partBackup.SegmentBackups, segment)
			partBackup.Size += segment.GetSize()
		}
	}
}

func (builder *metaBuilder) addIndexExtraInfo(indexes []*indexpb.FieldIndex) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	// group index by collection id and index id
	indexMap := make(map[int64]map[int64]*backuppb.IndexInfo)
	for _, coll := range builder.data.GetCollectionBackups() {
		indexMap[coll.GetCollectionId()] = make(map[int64]*backuppb.IndexInfo)
		for _, index := range coll.GetIndexInfos() {
			indexMap[coll.GetCollectionId()][index.GetIndexId()] = index
		}
	}

	for _, index := range indexes {
		info := index.GetIndexInfo()
		indexInfo := indexMap[info.GetCollectionID()][info.GetIndexID()]
		indexInfo.FieldId = info.GetFieldID()
		indexInfo.TypeParams = pbconv.MilvusKVToBakKV(info.GetTypeParams())
		indexInfo.CreateTime = index.GetCreateTime()
		indexInfo.IndexParams = pbconv.MilvusKVToBakKV(info.GetIndexParams())
		indexInfo.UserIndexParams = pbconv.MilvusKVToBakKV(info.GetUserIndexParams())
		indexInfo.IsAutoIndex = info.GetIsAutoIndex()
		indexInfo.MinIndexVersion = info.GetMinIndexVersion()
		indexInfo.MaxIndexVersion = info.GetMaxIndexVersion()
	}
}

func (builder *metaBuilder) setRBACMeta(rbacMeta *backuppb.RBACMeta) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.RbacMeta = rbacMeta
}

func (builder *metaBuilder) setRPCChannelInfo(rpcChannelInfo *backuppb.RPCChannelInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.RpcChannelInfo = rpcChannelInfo
}

func (builder *metaBuilder) buildBackupMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	info := &backuppb.BackupInfo{
		Id:              builder.data.GetId(),
		Name:            builder.data.GetName(),
		BackupTimestamp: builder.data.GetBackupTimestamp(),
		Size:            builder.data.GetSize(),
		MilvusVersion:   builder.data.GetMilvusVersion(),
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (builder *metaBuilder) buildFullMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	data, err := json.Marshal(builder.data)
	if err != nil {
		return nil, fmt.Errorf("backup: build full meta: %w", err)
	}

	return data, nil
}

func (builder *metaBuilder) buildCollectionMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	// TODO: don't know why we need segment info in collection meta. maybe just a bug.
	// we should remove it in the future.
	info := &backuppb.CollectionLevelBackupInfo{Infos: builder.data.GetCollectionBackups()}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build collection meta: %w", err)
	}

	return data, nil
}

func (builder *metaBuilder) buildPartitionMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	partitions := make([]*backuppb.PartitionBackupInfo, 0)
	for _, collection := range builder.data.GetCollectionBackups() {
		partitions = append(partitions, collection.GetPartitionBackups()...)
	}

	info := &backuppb.PartitionLevelBackupInfo{Infos: partitions}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build partition meta: %w", err)
	}

	return data, nil
}

func (builder *metaBuilder) buildSegmentMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	segments := make([]*backuppb.SegmentBackupInfo, 0)
	for _, collection := range builder.data.GetCollectionBackups() {
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
