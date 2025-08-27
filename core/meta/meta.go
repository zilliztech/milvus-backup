package meta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/samber/lo"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
)

func readFromFull(ctx context.Context, backupDir string, cli storage.Client) (*backuppb.BackupInfo, error) {
	bytes, err := storage.Read(ctx, cli, mpath.MetaKey(backupDir, mpath.FullMeta))
	if err != nil {
		return nil, fmt.Errorf("meta: read meta %w", err)
	}

	var backupInfo backuppb.BackupInfo
	if err := json.Unmarshal(bytes, &backupInfo); err != nil {
		return nil, fmt.Errorf("meta: unmarshal meta %w", err)
	}

	return &backupInfo, nil
}

type levelBackupInfo struct {
	backupInfo     *backuppb.BackupInfo
	collectionInfo *backuppb.CollectionLevelBackupInfo
	partitionInfo  *backuppb.PartitionLevelBackupInfo
	segmentInfo    *backuppb.SegmentLevelBackupInfo
}

// ToBackupInfo convert levelBackupInfo to backuppb.BackupInfo
// it will copy all the data, so it's safe to modify the returned backupInfo
func levelToTree(l *levelBackupInfo) *backuppb.BackupInfo {
	collIDPartIDSegs := make(map[string][]*backuppb.SegmentBackupInfo, len(l.segmentInfo.GetInfos()))
	for _, segment := range l.segmentInfo.GetInfos() {
		key := fmt.Sprintf("%d/%d", segment.GetCollectionId(), segment.GetPartitionId())
		collIDPartIDSegs[key] = append(collIDPartIDSegs[key], segment)
	}

	collIDPartitions := make(map[int64][]*backuppb.PartitionBackupInfo, len(l.partitionInfo.GetInfos()))
	for _, partition := range l.partitionInfo.GetInfos() {
		key := fmt.Sprintf("%d/%d", partition.GetCollectionId(), partition.GetPartitionId())
		segs := collIDPartIDSegs[key]

		partition.SegmentBackups = segs
		partition.Size = lo.SumBy(segs, func(seg *backuppb.SegmentBackupInfo) int64 { return seg.GetSize() })
		collIDPartitions[partition.GetCollectionId()] = append(collIDPartitions[partition.GetCollectionId()], partition)
	}

	collections := make([]*backuppb.CollectionBackupInfo, 0, len(l.collectionInfo.GetInfos()))
	for _, collection := range l.collectionInfo.GetInfos() {
		parts := collIDPartitions[collection.GetCollectionId()]

		collection.PartitionBackups = parts
		collection.Size = lo.SumBy(parts, func(part *backuppb.PartitionBackupInfo) int64 { return part.GetSize() })
		collections = append(collections, collection)
	}

	l.backupInfo.Size = lo.SumBy(collections, func(coll *backuppb.CollectionBackupInfo) int64 { return coll.GetSize() })
	l.backupInfo.CollectionBackups = collections
	return l.backupInfo

}

func readLevel[T any](ctx context.Context, backupDir string, cli storage.Client, metaType mpath.MetaType) (T, error) {
	var info T
	byts, err := storage.Read(ctx, cli, mpath.MetaKey(backupDir, metaType))
	if err != nil {
		return info, fmt.Errorf("meta: read level meta %w", err)
	}
	if err := json.Unmarshal(byts, &info); err != nil {
		return info, fmt.Errorf("meta: unmarshal level meta %w", err)
	}

	return info, nil
}

func readFromLevel(ctx context.Context, backupDir string, cli storage.Client) (*backuppb.BackupInfo, error) {
	backup, err := readLevel[backuppb.BackupInfo](ctx, backupDir, cli, mpath.BackupMeta)
	if err != nil {
		return nil, fmt.Errorf("meta: read backup meta %w", err)
	}

	collection, err := readLevel[backuppb.CollectionLevelBackupInfo](ctx, backupDir, cli, mpath.CollectionMeta)
	if err != nil {
		return nil, fmt.Errorf("meta: read collection meta %w", err)
	}

	partition, err := readLevel[backuppb.PartitionLevelBackupInfo](ctx, backupDir, cli, mpath.PartitionMeta)
	if err != nil {
		return nil, fmt.Errorf("meta: read partition meta %w", err)
	}

	segment, err := readLevel[backuppb.SegmentLevelBackupInfo](ctx, backupDir, cli, mpath.SegmentMeta)
	if err != nil {
		return nil, fmt.Errorf("meta: read segment meta %w", err)
	}

	level := &levelBackupInfo{
		backupInfo:     &backup,
		collectionInfo: &collection,
		partitionInfo:  &partition,
		segmentInfo:    &segment,
	}

	return levelToTree(level), nil
}

func Read(ctx context.Context, cli storage.Client, backupDir string) (*backuppb.BackupInfo, error) {
	exist, err := storage.Exist(ctx, cli, mpath.MetaKey(backupDir, mpath.FullMeta))
	if err != nil {
		return nil, fmt.Errorf("meta: check full meta exist %w", err)
	}
	if exist {
		return readFromFull(ctx, backupDir, cli)
	} else {
		return readFromLevel(ctx, backupDir, cli)
	}
}

func Exist(ctx context.Context, cli storage.Client, backupDir string) (bool, error) {
	key := mpath.MetaKey(backupDir, mpath.BackupMeta)
	return storage.Exist(ctx, cli, key)
}
