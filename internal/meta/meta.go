package meta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

const (
	LoadStateNotload = "NotLoad"
	LoadStateLoading = "Loading"
	LoadStateLoaded  = "Loaded"
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
	}
	return readFromLevel(ctx, backupDir, cli)
}

// Write persists a modified BackupInfo tree back to a backup directory, writing
// all five meta files (full + the four leveled files) so that both read paths in
// Read stay consistent. It MUTATES info while splitting it into leveled files
// (the full-meta bytes are captured first), so the caller must not reuse info
// afterwards. This is the inverse of levelToTree and mirrors the leveled layout
// produced by core/backup's meta builder.
func Write(ctx context.Context, cli storage.Client, backupDir string, info *backuppb.BackupInfo) error {
	// 1. full meta (authoritative: Read prefers it when present).
	fullBytes, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("meta: marshal full meta %w", err)
	}
	if err := storage.Write(ctx, cli, mpath.MetaKey(backupDir, mpath.FullMeta), fullBytes); err != nil {
		return fmt.Errorf("meta: write full meta %w", err)
	}

	// 2. split the tree into the four leveled files. Segment objects are shared
	// (only marshaled), so we mutate the collection/partition nesting in place.
	colls := info.GetCollectionBackups()
	var parts []*backuppb.PartitionBackupInfo
	for _, c := range colls {
		parts = append(parts, c.GetPartitionBackups()...)
	}
	var segs []*backuppb.SegmentBackupInfo
	for _, p := range parts {
		segs = append(segs, p.GetSegmentBackups()...)
		p.SegmentBackups = nil
	}
	for _, c := range colls {
		c.PartitionBackups = nil
		c.L0Segments = nil
	}
	info.CollectionBackups = nil // info is now the top-level backup summary

	levels := []struct {
		metaType mpath.MetaType
		value    any
	}{
		{mpath.BackupMeta, info},
		{mpath.CollectionMeta, &backuppb.CollectionLevelBackupInfo{Infos: colls}},
		{mpath.PartitionMeta, &backuppb.PartitionLevelBackupInfo{Infos: parts}},
		{mpath.SegmentMeta, &backuppb.SegmentLevelBackupInfo{Infos: segs}},
	}
	for _, lv := range levels {
		byts, err := json.Marshal(lv.value)
		if err != nil {
			return fmt.Errorf("meta: marshal %s %w", lv.metaType, err)
		}
		if err := storage.Write(ctx, cli, mpath.MetaKey(backupDir, lv.metaType), byts); err != nil {
			return fmt.Errorf("meta: write %s %w", lv.metaType, err)
		}
	}
	return nil
}

func Exist(ctx context.Context, cli storage.Client, backupDir string) (bool, error) {
	key := mpath.MetaKey(backupDir, mpath.BackupMeta)
	exist, err := storage.Exist(ctx, cli, key)
	if err != nil {
		return false, fmt.Errorf("meta: check backup exist %w", err)
	}

	return exist, nil
}

func List(ctx context.Context, cli storage.Client, backupRoot string) ([]*backuppb.BackupSummary, error) {
	backupDirs, _, err := storage.ListPrefixFlat(ctx, cli, mpath.BackupRootDir(backupRoot), false)
	if err != nil {
		return nil, fmt.Errorf("meta: list backup root %s: %w", backupRoot, err)
	}
	log.Info("list backup dirs", zap.Strings("dirs", backupDirs))

	summaries := make([]*backuppb.BackupSummary, 0, len(backupDirs))
	for _, backupDir := range backupDirs {
		backupInfo, err := Read(ctx, cli, backupDir)
		if err != nil {
			log.Warn("can not read backup info, skip it", zap.String("backup_dir", backupDir))
			continue
		}

		summary := &backuppb.BackupSummary{
			Id:            backupInfo.GetId(),
			Name:          backupInfo.GetName(),
			Size:          backupInfo.GetSize(),
			MilvusVersion: backupInfo.GetMilvusVersion(),
		}
		summaries = append(summaries, summary)
	}

	return summaries, nil
}
