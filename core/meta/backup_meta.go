package meta

import (
	"encoding/json"

	"github.com/golang/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

const (
	SEPERATOR = "/"

	LoadState_NotExist = "NotExist"
	LoadState_NotLoad  = "NotLoad"
	LoadState_Loading  = "Loading"
	LoadState_Loaded   = "Loaded"
)

type BackupMetaBytes struct {
	BackupMetaBytes     []byte
	CollectionMetaBytes []byte
	PartitionMetaBytes  []byte
	SegmentMetaBytes    []byte
	FullMetaBytes       []byte
}

type LeveledBackupInfo struct {
	backupLevel     *backuppb.BackupInfo
	collectionLevel *backuppb.CollectionLevelBackupInfo
	partitionLevel  *backuppb.PartitionLevelBackupInfo
	segmentLevel    *backuppb.SegmentLevelBackupInfo
}

// treeToLevel parse BackupInfo into backup-collection-partition-segment 4-level structure
func treeToLevel(backup *backuppb.BackupInfo) (LeveledBackupInfo, error) {
	collections := make([]*backuppb.CollectionBackupInfo, 0)
	partitions := make([]*backuppb.PartitionBackupInfo, 0)
	segments := make([]*backuppb.SegmentBackupInfo, 0)
	// recalculate backup size
	var backupSize int64 = 0
	for _, collectionBack := range backup.GetCollectionBackups() {
		// recalculate backup size
		var collectionSize int64 = 0
		for _, partitionBack := range collectionBack.GetPartitionBackups() {
			// recalculate backup size
			var partitionSize int64 = 0
			for _, segmentBack := range partitionBack.GetSegmentBackups() {
				segments = append(segments, segmentBack)
				partitionSize = partitionSize + segmentBack.GetSize()
			}
			partitionBack.Size = partitionSize
			clonePartitionBackupInfo := &backuppb.PartitionBackupInfo{
				PartitionId:   partitionBack.GetPartitionId(),
				PartitionName: partitionBack.GetPartitionName(),
				CollectionId:  partitionBack.GetCollectionId(),
				Size:          partitionBack.GetSize(),
				LoadState:     partitionBack.GetLoadState(),
			}
			partitions = append(partitions, clonePartitionBackupInfo)
			collectionSize = collectionSize + partitionSize
		}

		collectionBack.Size = collectionSize
		cloneCollectionBackup := proto.Clone(collectionBack).(*backuppb.CollectionBackupInfo)
		collections = append(collections, cloneCollectionBackup)
		backupSize = backupSize + collectionSize
	}

	collectionLevel := &backuppb.CollectionLevelBackupInfo{
		Infos: collections,
	}
	partitionLevel := &backuppb.PartitionLevelBackupInfo{
		Infos: partitions,
	}
	segmentLevel := &backuppb.SegmentLevelBackupInfo{
		Infos: segments,
	}
	backup.Size = backupSize
	backupLevel := &backuppb.BackupInfo{
		Id:              backup.GetId(),
		Name:            backup.GetName(),
		BackupTimestamp: backup.GetBackupTimestamp(),
		Size:            backup.GetSize(),
		MilvusVersion:   backup.GetMilvusVersion(),
	}

	return LeveledBackupInfo{
		backupLevel:     backupLevel,
		collectionLevel: collectionLevel,
		partitionLevel:  partitionLevel,
		segmentLevel:    segmentLevel,
	}, nil
}

func Serialize(backup *backuppb.BackupInfo) (*BackupMetaBytes, error) {
	level, err := treeToLevel(backup)
	if err != nil {
		return nil, err
	}
	collectionBackupMetaBytes, err := json.Marshal(level.collectionLevel)
	if err != nil {
		return nil, err
	}
	partitionBackupMetaBytes, err := json.Marshal(level.partitionLevel)
	if err != nil {
		return nil, err
	}
	segmentBackupMetaBytes, err := json.Marshal(level.segmentLevel)
	if err != nil {
		return nil, err
	}
	backupMetaBytes, err := json.Marshal(level.backupLevel)
	if err != nil {
		return nil, err
	}
	fullMetaBytes, err := json.Marshal(backup)
	if err != nil {
		return nil, err
	}

	return &BackupMetaBytes{
		BackupMetaBytes:     backupMetaBytes,
		CollectionMetaBytes: collectionBackupMetaBytes,
		PartitionMetaBytes:  partitionBackupMetaBytes,
		SegmentMetaBytes:    segmentBackupMetaBytes,
		FullMetaBytes:       fullMetaBytes,
	}, nil
}

type DbCollections = map[string][]string
