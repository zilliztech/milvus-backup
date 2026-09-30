package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func TestGetBackupByNameReadsCopiedArtifact(t *testing.T) {
	root := t.TempDir()
	params, err := v2.Load("", map[string]string{
		"backup.storage.provider": v2.ProviderLocal,
		"backup.storage.rootPath": t.TempDir(),
	})
	require.NoError(t, err)

	backupName := "copied-backup"
	info := &backuppb.BackupInfo{Name: backupName, Format: "snapshot"}
	require.NoError(t, meta.Write(context.Background(), &storage.LocalClient{},
		mpath.BackupDir(root, backupName), info))

	h := newGetBackupHandler(&backuppb.GetBackupRequest{BackupName: backupName, Path: root}, params)
	h.taskMgr = taskmgr.NewMgr()
	resp := h.get(context.Background())
	require.Equal(t, backuppb.ResponseCode_Success, resp.GetCode(), resp.GetMsg())
	require.Equal(t, backupName, resp.GetData().GetName())
	require.Equal(t, "snapshot", resp.GetData().GetFormat())
}
