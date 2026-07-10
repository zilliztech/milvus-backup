package meta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// Write persists a BackupInfo tree as full_meta.json plus the four leveled
// files, inverse of levelToTree. Read() prefers full_meta; Exist()/List() rely
// on backup_meta.json — so all five are written.
func Write(ctx context.Context, cli storage.Client, backupDir string, info *backuppb.BackupInfo) error {
	if err := writeJSON(ctx, cli, backupDir, mpath.FullMeta, info); err != nil {
		return err
	}

	// backup_meta.json = the tree WITHOUT nested collections (top-level scalars).
	backupLevel := &backuppb.BackupInfo{
		Id:                   info.GetId(),
		StateCode:            info.GetStateCode(),
		Name:                 info.GetName(),
		BackupTimestamp:      info.GetBackupTimestamp(),
		Size:                 info.GetSize(),
		MilvusVersion:        info.GetMilvusVersion(),
		RbacMeta:             info.GetRbacMeta(),
		RpcChannelInfo:       info.GetRpcChannelInfo(),
		DatabaseBackups:      info.GetDatabaseBackups(),
		FlushAllMsgsBase64:   info.GetFlushAllMsgsBase64(),
		ControlChannelName:   info.GetControlChannelName(),
		PhysicalChannelNames: info.GetPhysicalChannelNames(),
	}
	if err := writeJSON(ctx, cli, backupDir, mpath.BackupMeta, backupLevel); err != nil {
		return err
	}

	var colls []*backuppb.CollectionBackupInfo
	var parts []*backuppb.PartitionBackupInfo
	var segs []*backuppb.SegmentBackupInfo
	for _, coll := range info.GetCollectionBackups() {
		c := cloneCollShallow(coll) // copy, clear PartitionBackups
		c.PartitionBackups = nil
		colls = append(colls, c)
		for _, part := range coll.GetPartitionBackups() {
			p := clonePartShallow(part) // copy, clear SegmentBackups
			p.SegmentBackups = nil
			parts = append(parts, p)
			segs = append(segs, part.GetSegmentBackups()...)
		}
	}
	if err := writeJSON(ctx, cli, backupDir, mpath.CollectionMeta, &backuppb.CollectionLevelBackupInfo{Infos: colls}); err != nil {
		return err
	}
	if err := writeJSON(ctx, cli, backupDir, mpath.PartitionMeta, &backuppb.PartitionLevelBackupInfo{Infos: parts}); err != nil {
		return err
	}
	if err := writeJSON(ctx, cli, backupDir, mpath.SegmentMeta, &backuppb.SegmentLevelBackupInfo{Infos: segs}); err != nil {
		return err
	}
	return nil
}

func writeJSON(ctx context.Context, cli storage.Client, dir string, mt mpath.MetaType, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("meta: marshal %s: %w", mt, err)
	}
	if err := storage.Write(ctx, cli, mpath.MetaKey(dir, mt), b); err != nil {
		return fmt.Errorf("meta: write %s: %w", mt, err)
	}
	return nil
}

func cloneCollShallow(c *backuppb.CollectionBackupInfo) *backuppb.CollectionBackupInfo {
	cp := *c
	return &cp
}

func clonePartShallow(p *backuppb.PartitionBackupInfo) *backuppb.PartitionBackupInfo {
	cp := *p
	return &cp
}
