package core

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

const (
	META_PREFIX          = "meta"
	BACKUP_META_FILE     = "backup_meta.json"
	COLLECTION_META_FILE = "collection_meta.json"
	PARTITION_META_FILE  = "partition_meta.json"
	SEGMENT_META_FILE    = "segment_meta.json"
	FULL_META_FILE       = "full_meta.json"
	SEPERATOR            = "/"

	BINGLOG_DIR    = "binlogs"
	INSERT_LOG_DIR = "insert_log"
	DELTA_LOG_DIR  = "delta_log"
	STATS_LOG_DIR  = "stats_log"

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

	for _, collectionBack := range backup.GetCollectionBackups() {
		cloneCollectionBackup := &backuppb.CollectionBackupInfo{
			CollectionId:            collectionBack.GetCollectionId(),
			DbName:                  collectionBack.GetDbName(),
			CollectionName:          collectionBack.GetCollectionName(),
			Schema:                  collectionBack.GetSchema(),
			ShardsNum:               collectionBack.GetShardsNum(),
			ConsistencyLevel:        collectionBack.GetConsistencyLevel(),
			BackupTimestamp:         collectionBack.GetBackupTimestamp(),
			Size:                    collectionBack.GetSize(),
			HasIndex:                collectionBack.GetHasIndex(),
			IndexInfos:              collectionBack.GetIndexInfos(),
			LoadState:               collectionBack.GetLoadState(),
			BackupPhysicalTimestamp: collectionBack.GetBackupPhysicalTimestamp(),
			ChannelCheckpoints:      collectionBack.GetChannelCheckpoints(),
		}
		collections = append(collections, cloneCollectionBackup)

		for _, partitionBack := range collectionBack.GetPartitionBackups() {
			clonePartitionBackupInfo := &backuppb.PartitionBackupInfo{
				PartitionId:   partitionBack.GetPartitionId(),
				PartitionName: partitionBack.GetPartitionName(),
				CollectionId:  partitionBack.GetCollectionId(),
				Size:          partitionBack.GetSize(),
				LoadState:     partitionBack.GetLoadState(),
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
		StateCode:       backup.GetStateCode(),
		ErrorMessage:    backup.GetErrorMessage(),
		StartTime:       backup.GetStartTime(),
		EndTime:         backup.GetEndTime(),
		Progress:        backup.GetProgress(),
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

// levelToTree rebuild complete tree structure BackupInfo from backup-collection-partition-segment 4-level structure
func levelToTree(level *LeveledBackupInfo) (*backuppb.BackupInfo, error) {
	backupInfo := &backuppb.BackupInfo{
		Id:              level.backupLevel.GetId(),
		StateCode:       level.backupLevel.GetStateCode(),
		ErrorMessage:    level.backupLevel.GetErrorMessage(),
		StartTime:       level.backupLevel.GetStartTime(),
		EndTime:         level.backupLevel.GetEndTime(),
		Progress:        level.backupLevel.GetProgress(),
		Name:            level.backupLevel.GetName(),
		BackupTimestamp: level.backupLevel.GetBackupTimestamp(),
		Size:            level.backupLevel.GetSize(),
		MilvusVersion:   level.backupLevel.GetMilvusVersion(),
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
		collPartitions := partitionDict[collection.GetCollectionId()]
		var size int64 = 0
		for _, part := range collPartitions {
			size += part.GetSize()
		}
		collection.PartitionBackups = partitionDict[collection.GetCollectionId()]
		collection.Size = size
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

func FullMetaPath(backupRootPath, backupName string) string {
	return BackupMetaDirPath(backupRootPath, backupName) + SEPERATOR + FULL_META_FILE
}

func BackupBinlogDirPath(backupRootPath, backupName string) string {
	return backupRootPath + SEPERATOR + backupName + SEPERATOR + BINGLOG_DIR
}

func SimpleListBackupsResponse(input *backuppb.ListBackupsResponse) *backuppb.ListBackupsResponse {
	simpleBackupInfos := make([]*backuppb.BackupInfo, 0)
	for _, backup := range input.GetData() {
		simpleBackupInfos = append(simpleBackupInfos, &backuppb.BackupInfo{
			Id:              backup.GetId(),
			Name:            backup.GetName(),
			StateCode:       backup.GetStateCode(),
			ErrorMessage:    backup.GetErrorMessage(),
			BackupTimestamp: backup.GetBackupTimestamp(),
		})
	}
	return &backuppb.ListBackupsResponse{
		RequestId: input.GetRequestId(),
		Code:      input.GetCode(),
		Msg:       input.GetMsg(),
		Data:      simpleBackupInfos,
	}
}

func SimpleBackupResponse(input *backuppb.BackupInfoResponse) *backuppb.BackupInfoResponse {
	backup := input.GetData()

	collections := make([]*backuppb.CollectionBackupInfo, 0)
	for _, coll := range backup.GetCollectionBackups() {
		collections = append(collections, &backuppb.CollectionBackupInfo{
			StateCode:               coll.GetStateCode(),
			ErrorMessage:            coll.GetErrorMessage(),
			DbName:                  coll.GetDbName(),
			CollectionName:          coll.GetCollectionName(),
			CollectionId:            coll.GetCollectionId(),
			BackupTimestamp:         coll.GetBackupTimestamp(),
			HasIndex:                coll.GetHasIndex(),
			IndexInfos:              coll.GetIndexInfos(),
			LoadState:               coll.GetLoadState(),
			Schema:                  coll.GetSchema(),
			Size:                    coll.GetSize(),
			Progress:                coll.GetProgress(),
			BackupPhysicalTimestamp: coll.GetBackupPhysicalTimestamp(),
		})
	}
	simpleBackupInfo := &backuppb.BackupInfo{
		Id:                backup.GetId(),
		Name:              backup.GetName(),
		StateCode:         backup.GetStateCode(),
		ErrorMessage:      backup.GetErrorMessage(),
		BackupTimestamp:   backup.GetBackupTimestamp(),
		CollectionBackups: collections,
		MilvusVersion:     backup.GetMilvusVersion(),
		StartTime:         backup.GetStartTime(),
		EndTime:           backup.GetEndTime(),
		Progress:          backup.GetProgress(),
		Size:              backup.GetSize(),
	}
	return &backuppb.BackupInfoResponse{
		RequestId: input.GetRequestId(),
		Code:      input.GetCode(),
		Msg:       input.GetMsg(),
		Data:      simpleBackupInfo,
	}
}

func SimpleRestoreResponse(input *backuppb.RestoreBackupResponse) *backuppb.RestoreBackupResponse {
	restore := input.GetData()

	collectionRestores := make([]*backuppb.RestoreCollectionTask, 0)
	for _, coll := range restore.GetCollectionRestoreTasks() {
		collectionRestores = append(collectionRestores, &backuppb.RestoreCollectionTask{
			Id:                   coll.GetId(),
			StateCode:            coll.GetStateCode(),
			ErrorMessage:         coll.GetErrorMessage(),
			StartTime:            coll.GetStartTime(),
			EndTime:              coll.GetEndTime(),
			Progress:             coll.GetProgress(),
			TargetCollectionName: coll.GetTargetCollectionName(),
			TargetDbName:         coll.GetTargetDbName(),
			ToRestoreSize:        coll.GetToRestoreSize(),
			RestoredSize:         coll.GetRestoredSize(),
		})
	}

	simpleRestore := &backuppb.RestoreBackupTask{
		Id:                     restore.GetId(),
		StateCode:              restore.GetStateCode(),
		ErrorMessage:           restore.GetErrorMessage(),
		StartTime:              restore.GetStartTime(),
		EndTime:                restore.GetEndTime(),
		CollectionRestoreTasks: collectionRestores,
		Progress:               restore.GetProgress(),
	}

	return &backuppb.RestoreBackupResponse{
		RequestId: input.GetRequestId(),
		Code:      input.GetCode(),
		Msg:       input.GetMsg(),
		Data:      simpleRestore,
	}
}

func UpdateRestoreBackupTask(input *backuppb.RestoreBackupTask) *backuppb.RestoreBackupTask {
	var storedSize int64 = 0
	for _, coll := range input.GetCollectionRestoreTasks() {
		storedSize += coll.GetRestoredSize()
	}
	if input.ToRestoreSize == 0 {
		input.Progress = 100
	} else {
		input.Progress = int32(storedSize * 100 / input.ToRestoreSize)
	}
	return input
}

type DbCollections = map[string][]string
