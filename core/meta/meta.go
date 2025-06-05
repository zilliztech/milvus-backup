package meta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
)

func Read(ctx context.Context, backupDir string, cli storage.Client) (*backuppb.BackupInfo, error) {
	key := mpath.MetaKey(backupDir, mpath.FullMeta)
	bytes, err := storage.Read(ctx, cli, key)
	if err != nil {
		return nil, fmt.Errorf("meta: read meta %w", err)
	}

	var backupInfo backuppb.BackupInfo
	if err := json.Unmarshal(bytes, &backupInfo); err != nil {
		return nil, fmt.Errorf("meta: unmarshal meta %w", err)
	}
	return &backupInfo, nil
}
