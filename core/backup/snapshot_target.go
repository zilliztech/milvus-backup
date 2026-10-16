package backup

import (
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// snapshotTarget says where a collection's bundle is written. Backup copies
// the snapshot files itself, so the target is just the bundle root: one key
// prefix in the backup storage, plus the same prefix as a URI on the storage
// service the server can reach — the identity the recorded metadata path is
// resolved against on restore.
type snapshotTarget struct {
	// dir is the bundle root as an object key prefix in the backup storage.
	dir string

	// rootURI is dir as a complete storage URI, reachable by the server.
	rootURI string
}

func newSnapshotTarget(milvusCfg, backupCfg storage.Config, backupDir string) (snapshotTarget, error) {
	dir := mpath.BackupBundleDir(backupDir)
	rootURI, err := storage.SnapshotStoreURI(milvusCfg, backupCfg, dir)
	if err != nil {
		return snapshotTarget{}, err
	}
	return snapshotTarget{dir: dir, rootURI: rootURI}, nil
}
