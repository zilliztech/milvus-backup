package core

import (
	"encoding/json"
	"fmt"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"strings"
)

const (
	BACKUP_PREFIX        = "backup"
	META_PREFIX          = "meta"
	BACKUP_META_FILE     = "backup_meta.json"
	COLLECTION_META_FILE = "collection_meta.json"
	PARTITION_META_FILE  = "partition_meta.json"
	SEGMENT_META_FILE    = "segment_meta.json"
	SEPERATOR            = "/"

	BINGLOG_DIR    = "binlogs"
	INSERT_LOG_DIR = "insert_log"
	DELTA_LOG_DIR  = "delta_log"
	STATS_LOG_DIR  = "stats_log"
)

type BackupMetaBytes struct {
	BackupMetaBytes     []byte
	CollectionMetaBytes []byte
	PartitionMetaBytes  []byte
	SegmentMetaBytes    []byte
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

	for _, collectionBack := range backup.GetCollectionBackups() {
		cloneCollectionBackup := &backuppb.CollectionBackupInfo{
			CollectionId:     collectionBack.GetCollectionId(),
			DbName:           collectionBack.GetDbName(),
			CollectionName:   collectionBack.GetCollectionName(),
			Schema:           collectionBack.GetSchema(),
			ShardsNum:        collectionBack.GetShardsNum(),
			ConsistencyLevel: collectionBack.GetConsistencyLevel(),
			BackupTimestamp:  collectionBack.GetBackupTimestamp(),
			BackupStatus:     collectionBack.GetBackupStatus(),
			BackupError:      collectionBack.GetBackupError(),
			Health:           collectionBack.GetHealth(),
		}
		collections = append(collections, cloneCollectionBackup)

		for _, partitionBack := range collectionBack.GetPartitionBackups() {
			clonePartitionBackupInfo := &backuppb.PartitionBackupInfo{
				PartitionId:   partitionBack.GetPartitionId(),
				PartitionName: partitionBack.GetPartitionName(),
				CollectionId:  partitionBack.GetCollectionId(),
			}
			partitions = append(partitions, clonePartitionBackupInfo)

			for _, segmentBack := range partitionBack.GetSegmentBackups() {
				segments = append(segments, segmentBack)
			}
		}
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
	backupLevel := &backuppb.BackupInfo{
		Id:              backup.GetId(),
		Name:            backup.GetName(),
		BackupTimestamp: backup.GetBackupTimestamp(),
		BackupStatus:    backup.GetBackupStatus(),
		BackupError:     backup.GetBackupError(),
		Health:          backup.GetHealth(),
	}

	return LeveledBackupInfo{
		backupLevel:     backupLevel,
		collectionLevel: collectionLevel,
		partitionLevel:  partitionLevel,
		segmentLevel:    segmentLevel,
	}, nil
}

func serialize(backup *backuppb.BackupInfo) (*BackupMetaBytes, error) {
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

	return &BackupMetaBytes{
		BackupMetaBytes:     backupMetaBytes,
		CollectionMetaBytes: collectionBackupMetaBytes,
		PartitionMetaBytes:  partitionBackupMetaBytes,
		SegmentMetaBytes:    segmentBackupMetaBytes,
	}, nil
}

// levelToTree rebuild complete tree structure BackupInfo from backup-collection-partition-segment 4-level structure
func levelToTree(level *LeveledBackupInfo) (*backuppb.BackupInfo, error) {
	backupInfo := &backuppb.BackupInfo{
		Id:              level.backupLevel.GetId(),
		Name:            level.backupLevel.GetName(),
		BackupTimestamp: level.backupLevel.GetBackupTimestamp(),
		BackupStatus:    level.backupLevel.GetBackupStatus(),
		BackupError:     level.backupLevel.GetBackupError(),
		Health:          level.backupLevel.GetHealth(),
	}
	segmentDict := make(map[string][]*backuppb.SegmentBackupInfo, len(level.segmentLevel.GetInfos()))
	for _, segment := range level.segmentLevel.GetInfos() {
		unqiueId := fmt.Sprintf("%d-%d", segment.GetCollectionId(), segment.GetPartitionId())
		segmentDict[unqiueId] = append(segmentDict[unqiueId], segment)
	}

	partitionDict := make(map[int64][]*backuppb.PartitionBackupInfo, len(level.partitionLevel.GetInfos()))
	for _, partition := range level.partitionLevel.GetInfos() {
		unqiueId := partition.GetCollectionId()
		partition.SegmentBackups = segmentDict[fmt.Sprintf("%d-%d", partition.GetCollectionId(), partition.GetPartitionId())]
		partitionDict[unqiueId] = append(partitionDict[unqiueId], partition)
	}

	for _, collection := range level.collectionLevel.GetInfos() {
		collection.PartitionBackups = partitionDict[collection.GetCollectionId()]
	}

	backupInfo.CollectionBackups = level.collectionLevel.GetInfos()
	return backupInfo, nil
}

func deserialize(backup *BackupMetaBytes) (*backuppb.BackupInfo, error) {
	backupInfo := &backuppb.BackupInfo{}
	err := json.Unmarshal(backup.BackupMetaBytes, backupInfo)
	if err != nil {
		return backupInfo, err
	}
	collectionLevel := &backuppb.CollectionLevelBackupInfo{}
	err = json.Unmarshal(backup.CollectionMetaBytes, collectionLevel)
	partitionLevel := &backuppb.PartitionLevelBackupInfo{}
	err = json.Unmarshal(backup.PartitionMetaBytes, partitionLevel)
	segmentLevel := &backuppb.SegmentLevelBackupInfo{}
	err = json.Unmarshal(backup.SegmentMetaBytes, segmentLevel)

	return levelToTree(&LeveledBackupInfo{
		collectionLevel: collectionLevel,
		partitionLevel:  partitionLevel,
		segmentLevel:    segmentLevel,
		backupLevel:     backupInfo,
	})
}

func BackupPathToName(backupRootPath, path string) string {
	return strings.Replace(strings.Replace(path, backupRootPath+SEPERATOR, "", 1), SEPERATOR, "", 1)
}

func BackupDirPath(backupRootPath, backupName string) string {
	return backupRootPath + SEPERATOR + backupName + SEPERATOR
}

func BackupMetaDirPath(backupRootPath, backupName string) string {
	return backupRootPath + SEPERATOR + backupName + SEPERATOR + META_PREFIX
}

func BackupMetaPath(backupRootPath, backupName string) string {
	return BackupMetaDirPath(backupRootPath, backupName) + SEPERATOR + BACKUP_META_FILE
}

func CollectionMetaPath(backupRootPath, backupName string) string {
	return BackupMetaDirPath(backupRootPath, backupName) + SEPERATOR + COLLECTION_META_FILE
}

func PartitionMetaPath(backupRootPath, backupName string) string {
	return BackupMetaDirPath(backupRootPath, backupName) + SEPERATOR + PARTITION_META_FILE
}

func SegmentMetaPath(backupRootPath, backupName string) string {
	return BackupMetaDirPath(backupRootPath, backupName) + SEPERATOR + SEGMENT_META_FILE
}

func BackupBinlogDirPath(backupRootPath, backupName string) string {
	return backupRootPath + SEPERATOR + backupName + SEPERATOR + BINGLOG_DIR
}
