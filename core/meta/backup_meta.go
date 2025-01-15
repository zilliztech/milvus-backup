package meta

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

const (
	META_PREFIX          = "meta"
	BACKUP_META_FILE     = "backup_meta.json"
	COLLECTION_META_FILE = "collection_meta.json"
	PARTITION_META_FILE  = "partition_meta.json"
	SEGMENT_META_FILE    = "segment_meta.json"
	FULL_META_FILE       = "full_meta.json"
	CP_META_FILE         = "channel_cp_meta.json"
	SEPERATOR            = "/"

	BinglogDir = "binlogs"

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
		var size int64 = 0
		for _, seg := range partition.SegmentBackups {
			size += seg.Size
		}
		partition.Size = size
		partitionDict[unqiueId] = append(partitionDict[unqiueId], partition)
	}

	var backupSize int64 = 0
	for _, collection := range level.collectionLevel.GetInfos() {
		collPartitions := partitionDict[collection.GetCollectionId()]
		var size int64 = 0
		for _, part := range collPartitions {
			size += part.GetSize()
		}
		collection.PartitionBackups = partitionDict[collection.GetCollectionId()]
		collection.Size = size
		backupSize += size
	}

	backupInfo.Size = backupSize
	backupInfo.CollectionBackups = level.collectionLevel.GetInfos()
	return backupInfo, nil
}

func Deserialize(backup *BackupMetaBytes) (*backuppb.BackupInfo, error) {
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

func ChannelCPMetaPath(backupRootPath, backupName string) string {
	return BackupMetaDirPath(backupRootPath, backupName) + SEPERATOR + CP_META_FILE
}

func BackupBinlogDirPath(backupRootPath, backupName string) string {
	return backupRootPath + SEPERATOR + backupName + SEPERATOR + BinglogDir
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
			Size:            backup.GetSize(),
			StartTime:       backup.GetStartTime(),
			EndTime:         backup.GetEndTime(),
			MilvusVersion:   backup.GetMilvusVersion(),
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
	if backup == nil {
		return input
	}

	collections := make([]*backuppb.CollectionBackupInfo, 0)
	for _, coll := range backup.GetCollectionBackups() {
		// clone and remove PartitionBackups, avoid updating here every time we add a field in CollectionBackupInfo
		clonedCollectionBackup := proto.Clone(coll).(*backuppb.CollectionBackupInfo)
		clonedCollectionBackup.PartitionBackups = nil
		// clonedCollectionBackup.Schema = nil // schema is needed by cloud
		collections = append(collections, clonedCollectionBackup)
	}
	simpleBackupInfo := proto.Clone(backup).(*backuppb.BackupInfo)
	simpleBackupInfo.CollectionBackups = collections
	return &backuppb.BackupInfoResponse{
		RequestId: input.GetRequestId(),
		Code:      input.GetCode(),
		Msg:       input.GetMsg(),
		Data:      simpleBackupInfo,
	}
}

func SimpleRestoreResponse(input *backuppb.RestoreBackupResponse) *backuppb.RestoreBackupResponse {
	restore := input.GetData()
	if restore == nil {
		return input
	}

	simpleRestore := proto.Clone(restore).(*backuppb.RestoreBackupTask)

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

	simpleRestore.CollectionRestoreTasks = collectionRestores

	return &backuppb.RestoreBackupResponse{
		RequestId: input.GetRequestId(),
		Code:      input.GetCode(),
		Msg:       input.GetMsg(),
		Data:      simpleRestore,
	}
}

type DbCollections = map[string][]string
