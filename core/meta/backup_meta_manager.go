package meta

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type MetaManager struct {
	backups            map[string]*backuppb.BackupInfo                              // taskID -> BackupInfo
	collections        map[string]map[int64]*backuppb.CollectionBackupInfo          // taskID -> collectionID -> collection
	partitions         map[string]map[int64]map[int64]*backuppb.PartitionBackupInfo // taskID -> collectionID -> partitionID -> partition
	segments           map[string]map[int64]map[int64]*backuppb.SegmentBackupInfo   // taskID -> partitionID -> segmentID -> segment
	backupNameToTaskID map[string]string                                            // backupName -> taskID
	mu                 sync.Mutex
}

func NewMetaManager() *MetaManager {
	return &MetaManager{
		backups:            make(map[string]*backuppb.BackupInfo),
		collections:        make(map[string]map[int64]*backuppb.CollectionBackupInfo),
		partitions:         make(map[string]map[int64]map[int64]*backuppb.PartitionBackupInfo),
		segments:           make(map[string]map[int64]map[int64]*backuppb.SegmentBackupInfo),
		backupNameToTaskID: make(map[string]string),
	}
}

func (meta *MetaManager) GetBackup(taskID string) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup, exist := meta.backups[taskID]
	if !exist {
		return nil
	}
	return backup
}

func (meta *MetaManager) GetBackupByName(name string) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	id, exist := meta.backupNameToTaskID[name]
	if !exist {
		return nil
	}
	backup, exist := meta.backups[id]
	if !exist {
		return nil
	}
	return backup
}

func (meta *MetaManager) AddBackup(taskID string, backup *backuppb.BackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	meta.backups[taskID] = backup
	meta.backupNameToTaskID[backup.Name] = taskID
}

func (meta *MetaManager) AddCollection(taskID string, collection *backuppb.CollectionBackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if _, exist := meta.collections[taskID]; !exist {
		meta.collections[taskID] = make(map[int64]*backuppb.CollectionBackupInfo)
	}
	meta.collections[taskID][collection.GetCollectionId()] = collection
}

func (meta *MetaManager) AddPartition(taskID string, partition *backuppb.PartitionBackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if _, exist := meta.partitions[taskID]; !exist {
		meta.partitions[taskID] = make(map[int64]map[int64]*backuppb.PartitionBackupInfo)
	}
	if _, exist := meta.partitions[taskID][partition.GetCollectionId()]; !exist {
		meta.partitions[taskID][partition.GetCollectionId()] = make(map[int64]*backuppb.PartitionBackupInfo)
	}

	meta.partitions[taskID][partition.GetCollectionId()][partition.GetPartitionId()] = partition
}

func (meta *MetaManager) AddSegment(taskID string, segment *backuppb.SegmentBackupInfo) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if _, exist := meta.segments[taskID]; !exist {
		meta.segments[taskID] = make(map[int64]map[int64]*backuppb.SegmentBackupInfo)
	}
	if _, exist := meta.segments[taskID][segment.GetPartitionId()]; !exist {
		meta.segments[taskID][segment.GetPartitionId()] = make(map[int64]*backuppb.SegmentBackupInfo)
	}
	meta.segments[taskID][segment.GetPartitionId()][segment.GetSegmentId()] = segment
}

type BackupOpt func(backup *backuppb.BackupInfo)

func SetStateCode(stateCode backuppb.BackupTaskStateCode) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.StateCode = stateCode
	}
}

func SetErrorMessage(errorMessage string) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.ErrorMessage = errorMessage
	}
}

func SetEndTime(endTime int64) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.EndTime = endTime
	}
}

func SetRPCChannelPos(name, pos string) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.RpcChannelInfo = &backuppb.RPCChannelInfo{Name: name, Position: pos}
	}
}

func SetRBACMeta(rbacMeta *backuppb.RBACMeta) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.RbacMeta = rbacMeta
	}
}

func AddDatabase(database *backuppb.DatabaseBackupInfo) BackupOpt {
	return func(backup *backuppb.BackupInfo) {
		backup.DatabaseBackups = append(backup.DatabaseBackups, database)
	}
}

func (meta *MetaManager) UpdateBackup(taskID string, opts ...BackupOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.backups[taskID]
	cBackup := proto.Clone(backup).(*backuppb.BackupInfo)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.backups[taskID] = cBackup
}

type CollectionOpt func(collection *backuppb.CollectionBackupInfo)

func SetCollectionBackupTimestamp(backupTimestamp uint64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.BackupTimestamp = backupTimestamp
	}
}

func AddL0Segment(segment *backuppb.SegmentBackupInfo) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.L0Segments = append(collection.L0Segments, segment)
	}
}

func SetCollectionBackupPhysicalTimestamp(backupPhysicalTimestamp uint64) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.BackupPhysicalTimestamp = backupPhysicalTimestamp
	}
}

func SetCollectionChannelCheckpoints(channelCheckpoints map[string]string) CollectionOpt {
	return func(collection *backuppb.CollectionBackupInfo) {
		collection.ChannelCheckpoints = channelCheckpoints
	}
}

func (meta *MetaManager) UpdateCollection(taskID string, collectionID int64, opts ...CollectionOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.collections[taskID][collectionID]
	cBackup := proto.Clone(backup).(*backuppb.CollectionBackupInfo)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.collections[taskID][collectionID] = cBackup
}

type PartitionOpt func(partition *backuppb.PartitionBackupInfo)

type SegmentOpt func(segment *backuppb.SegmentBackupInfo)

func SetSegmentBackuped(backuped bool) SegmentOpt {
	return func(segment *backuppb.SegmentBackupInfo) {
		segment.Backuped = backuped
	}
}

func (meta *MetaManager) UpdateSegment(taskID string, partitionID int64, segmentID int64, opts ...SegmentOpt) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup := meta.segments[taskID][partitionID][segmentID]
	cBackup := proto.Clone(backup).(*backuppb.SegmentBackupInfo)
	for _, opt := range opts {
		opt(cBackup)
	}
	meta.segments[taskID][partitionID][segmentID] = cBackup
}

func (meta *MetaManager) GetFullMeta(taskID string) *backuppb.BackupInfo {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	backup, exist := meta.backups[taskID]
	if !exist {
		return nil
	}
	collections := meta.collections[taskID]
	var backupedSize int64
	var totalSize int64
	cloneBackup := proto.Clone(backup).(*backuppb.BackupInfo)

	collectionBackups := make([]*backuppb.CollectionBackupInfo, 0)
	for collectionID, collection := range collections {
		collectionBackup := proto.Clone(collection).(*backuppb.CollectionBackupInfo)
		partitionBackups := make([]*backuppb.PartitionBackupInfo, 0)
		for partitionID, partition := range meta.partitions[taskID][collectionID] {
			segmentBackups := make([]*backuppb.SegmentBackupInfo, 0)
			partitionBackup := proto.Clone(partition).(*backuppb.PartitionBackupInfo)
			for _, segment := range meta.segments[taskID][partitionID] {
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
		cloneBackup.Progress = int32(float64(backupedSize) / float64(totalSize) * 100)
	} else {
		cloneBackup.Progress = 0
	}
	log.Info("Get backup", zap.String("state", cloneBackup.StateCode.String()), zap.Int64("backupedSize", backupedSize), zap.Int64("totalSize", totalSize), zap.Int32("progress", cloneBackup.Progress))
	return cloneBackup
}
