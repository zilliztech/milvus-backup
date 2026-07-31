package backup

import (
	"fmt"
	"path"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// snapshotTarget says where a collection's bundle is exported to, in the terms
// ExportSnapshot takes them.
type snapshotTarget struct {
	// Path is the bundle root as a complete URI. ExportSnapshot also accepts a bare
	// object key, but that names a key in Milvus's own bucket, which is not where a
	// backup goes.
	Path string

	// Dir is that same root relative to the backup directory.
	Dir string

	// ExternalSpec carries the credentials for the target, and is empty when Milvus
	// can reach it with its own.
	ExternalSpec string
}

func newSnapshotTarget(milvusCfg, backupCfg storage.Config, backupDir string) (snapshotTarget, error) {
	uri, err := storage.SnapshotURI(backupCfg, mpath.BackupBundleDir(backupDir))
	if err != nil {
		return snapshotTarget{}, err
	}
	target := snapshotTarget{Path: uri, Dir: mpath.BundleDirName}

	// With no spec Milvus writes with its own credential and lets bucket policy
	// authorize the target, which is exactly the case when both sides are the same
	// backend — and it keeps the access key off the wire.
	if storage.SameBackend(milvusCfg, backupCfg) {
		return target, nil
	}

	spec, err := storage.SnapshotExternalSpec(backupCfg)
	if err != nil {
		return snapshotTarget{}, err
	}
	target.ExternalSpec = spec

	return target, nil
}

// metadataPath turns the absolute uri a finished export reports into a path relative to
// the backup directory, which is what the backup meta records: an absolute uri would pin
// the backup to the bucket and prefix it was written to.
func (t snapshotTarget) metadataPath(metadataURI string) (string, error) {
	root := strings.TrimSuffix(t.Path, "/") + "/"
	if !strings.HasPrefix(metadataURI, root) {
		return "", fmt.Errorf("backup: exported metadata %s is not under the target %s", metadataURI, t.Path)
	}

	return path.Join(t.Dir, strings.TrimPrefix(metadataURI, root)), nil
}
