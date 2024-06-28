package core

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type MetaManager struct {
	backups                    map[string]*backuppb.BackupInfo                     // backupId -> BackupInfo
	collections                map[string]map[int64]*backuppb.CollectionBackupInfo // backupId -> collectionID -> collection
	partitions                 map[int64]map[int64]*backuppb.PartitionBackupInfo   // collectionID -> partitionID -> partition
	segments                   map[int64]map[int64]*backuppb.SegmentBackupInfo     // partitionID -> segmentID -> segment
	segmentPartitionReverse    map[int64]int64                                     // segmentID -> partitionID
	partitionCollectionReverse map[int64]int64                                     // partitionID -> collectionID
	collectionBackupReverse    map[int64]string                                    // collectionID -> backupId
	backupNameToIdDict         map[string]string
	restoreTasks               map[string]*backuppb.RestoreBackupTask
	mu                         sync.Mutex
}

func newMetaManager() *MetaManager {
	return &MetaManager{
		backups:                    make(map[string]*backuppb.BackupInfo, 0),
		collections:                make(map[string]map[int64]*backuppb.CollectionBackupInfo, 0),
		partitions:                 make(map[int64]map[int64]*backuppb.PartitionBackupInfo, 0),
		segments:                   make(map[int64]map[int64]*backuppb.SegmentBackupInfo, 0),
		segmentPartitionReverse:    make(map[int64]int64, 0),
		partitionCollectionReverse: make(map[int64]int64, 0),
		collectionBackupReverse:    make(map[int64]string, 0),
		backupNameToIdDict:         make(map[string]string, 0),
		restoreTasks:               make(map[string]*backuppb.RestoreBackupTask, 0),
		mu:                         sync.Mutex{},
	}
}

func (meta *MetaManager) GetBackup(id string) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup, exist := meta.backups[id]
	if !exist {
		return nil
	}
	return backup
}

func (meta *MetaManager) GetBackupByName(name string) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	id, exist := meta.backupNameToIdDict[name]
	if !exist {
		return nil
	}
	backup, exist := meta.backups[id]
	if !exist {
		return nil
	}
	return backup
}

func (meta *MetaManager) AddBackup(backup *backuppb.BackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	meta.backups[backup.Id] = backup
	meta.backupNameToIdDict[backup.Name] = backup.Id
}

func (meta *MetaManager) AddCollection(collection *backuppb.CollectionBackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if _, exist := meta.collections[collection.Id]; !exist {
		meta.collections[collection.Id] = make(map[int64]*backuppb.CollectionBackupInfo, 0)
	}
	meta.collections[collection.Id][collection.GetCollectionId()] = collection
	meta.collectionBackupReverse[collection.GetCollectionId()] = collection.Id
}

func (meta *MetaManager) AddPartition(partition *backuppb.PartitionBackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if _, exist := meta.partitions[partition.GetCollectionId()]; !exist {
		meta.partitions[partition.GetCollectionId()] = make(map[int64]*backuppb.PartitionBackupInfo, 0)
	}
	meta.partitions[partition.GetCollectionId()][partition.GetPartitionId()] = partition
	meta.partitionCollectionReverse[partition.GetPartitionId()] = partition.GetCollectionId()
}

func (meta *MetaManager) AddSegment(segment *backuppb.SegmentBackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if _, exist := meta.segments[segment.GetPartitionId()]; !exist {
		meta.segments[segment.GetPartitionId()] = make(map[int64]*backuppb.SegmentBackupInfo, 0)
	}
	meta.segments[segment.GetPartitionId()][segment.GetSegmentId()] = segment
	meta.segmentPartitionReverse[segment.GetSegmentId()] = segment.GetPartitionId()
}

type BackupOpt func(backup *backuppb.BackupInfo)

func setStateCode(stateCode backuppb.BackupTaskStateCode) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.StateCode = stateCode
	}
}

func setErrorMessage(errorMessage string) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.ErrorMessage = errorMessage
	}
}
func setStartTime(startTime int64) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.StartTime = startTime
	}
}

func setEndTime(endTime int64) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.EndTime = endTime
	}
}

func setProgress(progress int32) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.Progress = progress
	}
}

func setName(name string) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.Name = name
	}
}

// backup timestamp
func setBackupTimestamp(backupTimestamp uint64) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.BackupTimestamp = backupTimestamp
	}
}

// array of collection backup
//repeated CollectionBackupInfo collection_backups = 9;

func setSize(size int64) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.Size = size
	}
}

func incSize(size int64) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.Size = backup.Size + size
	}
}

func setMilvusVersion(milvusVersion string) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.MilvusVersion = milvusVersion
	}
}

func (meta *MetaManager) UpdateBackup(backupID string, opts ...BackupOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.backups[backupID]
	cBackup := proto.Clone(backup).(*backuppb.BackupInfo)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.backups[backup.Id] = cBackup
}

type CollectionOpt func(collection *backuppb.CollectionBackupInfo)

func setCollectionStateCode(stateCode backuppb.BackupTaskStateCode) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.StateCode = stateCode
	}
}

func setCollectionErrorMessage(errorMessage string) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.ErrorMessage = errorMessage
	}
}

func setCollectionStartTime(startTime int64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.StartTime = startTime
	}
}

func setCollectionEndTime(endTime int64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.EndTime = endTime
	}
}

func setCollectionProgress(progress int32) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.Progress = progress
	}
}

// backup timestamp
func setCollectionBackupTimestamp(backupTimestamp uint64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.BackupTimestamp = backupTimestamp
	}
}

func setCollectionSize(size int64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.Size = size
	}
}

func setL0Segments(segments []*backuppb.SegmentBackupInfo) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.L0Segments = segments
	}
}

func incCollectionSize(size int64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.Size = collection.Size + size
	}
}

func setCollectionDbName(dbName string) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.DbName = dbName
	}
}

func setCollectionName(collectionName string) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.CollectionName = collectionName
	}
}

func setCollectionSchema(schema *backuppb.CollectionSchema) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.Schema = schema
	}
}

func setCollectionShardNum(shardsNum int32) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.ShardsNum = shardsNum
	}
}

func setCollectionConsistencyLevel(consistencyLevel backuppb.ConsistencyLevel) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.ConsistencyLevel = consistencyLevel
	}
}

func setCollectionHasIndex(hasIndex bool) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.HasIndex = hasIndex
	}
}

func setCollectionIndexInfos(indexInfos []*backuppb.IndexInfo) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.IndexInfos = indexInfos
	}
}

func setCollectionLoadState(loadState string) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.LoadState = loadState
	}
}

func setCollectionBackupPhysicalTimestamp(backupPhysicalTimestamp uint64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.BackupPhysicalTimestamp = backupPhysicalTimestamp
	}
}

func setCollectionChannelCheckpoints(channelCheckpoints map[string]string) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.ChannelCheckpoints = channelCheckpoints
	}
}

func (meta *MetaManager) GetCollections(backupID string) map[int64]*backuppb.CollectionBackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	return meta.collections[backupID]
}

func (meta *MetaManager) UpdateCollection(backupID string, collectionID int64, opts ...CollectionOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.collections[backupID][collectionID]
	cBackup := proto.Clone(backup).(*backuppb.CollectionBackupInfo)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.collections[backupID][collectionID] = cBackup
}

type PartitionOpt func(partition *backuppb.PartitionBackupInfo)

func setPartitionSize(size int64) PartitionOpt {
	return func(partition *backuppb.PartitionBackupInfo) {
		partition.Size = size
	}
}

func (meta *MetaManager) GetPartitions(collectionID int64) map[int64]*backuppb.PartitionBackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()

	partitions, exist := meta.partitions[collectionID]
	if !exist {
		return make(map[int64]*backuppb.PartitionBackupInfo, 0)
	}
	return partitions
}

func (meta *MetaManager) UpdatePartition(collectionID int64, partitionID int64, opts ...PartitionOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.partitions[collectionID][partitionID]
	cBackup := proto.Clone(backup).(*backuppb.PartitionBackupInfo)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.partitions[collectionID][partitionID] = cBackup
}

type SegmentOpt func(segment *backuppb.SegmentBackupInfo)

func (meta *MetaManager) GetSegments(partitionID int64) map[int64]*backuppb.SegmentBackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	segments, exist := meta.segments[partitionID]
	if !exist {
		return make(map[int64]*backuppb.SegmentBackupInfo, 0)
	}
	return segments
}

func (meta *MetaManager) GetSegment(segmentID int64) *backuppb.SegmentBackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	partitionID := meta.segmentPartitionReverse[segmentID]
	return meta.segments[partitionID][segmentID]
}

func setSegmentNumOfRows(numOfRows int64) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.NumOfRows = numOfRows
	}
}

func setSegmentSize(size int64) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.Size = size
	}
}

func setSegmentL0(isL0 bool) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.IsL0 = isL0
	}
}

func setGroupID(groupID int64) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.GroupId = groupID
	}
}

func setSegmentBinlogs(binlogs []*backuppb.FieldBinlog) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.Binlogs = binlogs
	}
}

func setSegmentStatsBinlogs(binlogs []*backuppb.FieldBinlog) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.Statslogs = binlogs
	}
}

func setSegmentDeltaBinlogs(binlogs []*backuppb.FieldBinlog) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.Deltalogs = binlogs
	}
}

func setSegmentGroupId(groupId int64) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.GroupId = groupId
	}
}

func setSegmentBackuped(backuped bool) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.Backuped = backuped
	}
}

func (meta *MetaManager) UpdateSegment(partitionID int64, segmentID int64, opts ...SegmentOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.segments[partitionID][segmentID]
	cBackup := proto.Clone(backup).(*backuppb.SegmentBackupInfo)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.segments[partitionID][segmentID] = cBackup
}

func (meta *MetaManager) GetBackupBySegmentID(segmentID int64) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	partitionID, exist := meta.segmentPartitionReverse[segmentID]
	if !exist {
		return nil
	}
	collectionID, exist := meta.partitionCollectionReverse[partitionID]
	if !exist {
		return nil
	}
	backupID, exist := meta.collectionBackupReverse[collectionID]
	if !exist {
		return nil
	}
	return meta.backups[backupID]
}

func (meta *MetaManager) GetBackupByCollectionID(collectionID int64) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backupID, exist := meta.collectionBackupReverse[collectionID]
	if !exist {
		return nil
	}
	return meta.backups[backupID]
}

func (meta *MetaManager) GetFullMeta(id string) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup, exist := meta.backups[id]
	if !exist {
		return nil
	}
	collections := meta.collections[id]
	var backupedSize int64 = 0
	var totalSize int64 = 0
	cloneBackup := proto.Clone(backup).(*backuppb.BackupInfo)

	collectionBackups := make([]*backuppb.CollectionBackupInfo, 0)
	for collectionID, collection := range collections {
		collectionBackup := proto.Clone(collection).(*backuppb.CollectionBackupInfo)
		partitionBackups := make([]*backuppb.PartitionBackupInfo, 0)
		for partitionID, partition := range meta.partitions[collectionID] {
			segmentBackups := make([]*backuppb.SegmentBackupInfo, 0)
			partitionBackup := proto.Clone(partition).(*backuppb.PartitionBackupInfo)
			for _, segment := range meta.segments[partitionID] {
				segmentBackups = append(segmentBackups, proto.Clone(segment).(*backuppb.SegmentBackupInfo))
				if segment.Backuped {
					backupedSize += segment.GetSize()
				}
				totalSize += segment.GetSize()
				partitionBackup.Size = partitionBackup.Size + segment.GetSize()
			}
			partitionBackup.SegmentBackups = segmentBackups
			partitionBackups = append(partitionBackups, partitionBackup)
			collectionBackup.Size = collectionBackup.Size + partitionBackup.Size
		}
		collectionBackup.PartitionBackups = partitionBackups
		collectionBackups = append(collectionBackups, collectionBackup)
		cloneBackup.Size = cloneBackup.Size + collectionBackup.Size
	}
	cloneBackup.CollectionBackups = collectionBackups
	if totalSize != 0 {
		cloneBackup.Progress = int32(backupedSize * 100 / (totalSize))
	} else {
		cloneBackup.Progress = 100
	}
	log.Info("Get backup progress", zap.Int64("backupedSize", backupedSize), zap.Int64("totalSize", totalSize), zap.Int32("progress", cloneBackup.Progress))
	return cloneBackup
}

type RestoreTaskOpt func(task *backuppb.RestoreBackupTask)

func setRestoreStateCode(stateCode backuppb.RestoreTaskStateCode) RestoreTaskOpt {
	return func(task *backuppb.RestoreBackupTask) {
		task.StateCode = stateCode
	}
}

func setRestoreErrorMessage(errorMessage string) RestoreTaskOpt {
	return func(task *backuppb.RestoreBackupTask) {
		task.ErrorMessage = errorMessage
	}
}

func setRestoreStartTime(startTime int64) RestoreTaskOpt {
	return func(task *backuppb.RestoreBackupTask) {
		task.StartTime = startTime
	}
}

func setRestoreEndTime(endTime int64) RestoreTaskOpt {
	return func(task *backuppb.RestoreBackupTask) {
		task.EndTime = endTime
	}
}

func addRestoreRestoredSize(restoredSize int64) RestoreTaskOpt {
	return func(task *backuppb.RestoreBackupTask) {
		task.RestoredSize = task.RestoredSize + restoredSize
	}
}

func addCollectionRestoredSize(collectionID, restoredSize int64) RestoreTaskOpt {
	return func(task *backuppb.RestoreBackupTask) {
		task.RestoredSize = task.RestoredSize + restoredSize
		for _, coll := range task.GetCollectionRestoreTasks() {
			if coll.CollBackup.CollectionId == collectionID {
				coll.RestoredSize = coll.RestoredSize + restoredSize
			}
		}
	}
}

func (meta *MetaManager) UpdateRestoreTask(restoreID string, opts ...RestoreTaskOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.restoreTasks[restoreID]
	cBackup := proto.Clone(backup).(*backuppb.RestoreBackupTask)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.restoreTasks[backup.Id] = cBackup
}

//CollectionRestoreTasks []*RestoreCollectionTask `protobuf:"bytes,6,rep,name=collection_restore_tasks,json=collectionRestoreTasks,proto3" json:"collection_restore_tasks,omitempty"`

func (meta *MetaManager) AddRestoreTask(task *backuppb.RestoreBackupTask) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	meta.restoreTasks[task.Id] = task
}

func (meta *MetaManager) GetRestoreTask(taskID string) *backuppb.RestoreBackupTask {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	return meta.restoreTasks[taskID]
}
