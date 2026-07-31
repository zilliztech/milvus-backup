package restore

import (
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/storage"
)

// snapshotSource says where the bundles of a snapshot-format backup are read from, in the
// terms RestoreExternalSnapshot takes them.
type snapshotSource struct {
	// dirURI is the backup directory as a complete URI, which the paths in the backup meta
	// are relative to.
	dirURI string

	// externalSpec carries the credentials for the source, and is empty when Milvus can
	// reach it with its own.
	externalSpec string
}

func newSnapshotSource(milvusCfg, backupCfg storage.Config, backupDir string) (snapshotSource, error) {
	uri, err := storage.SnapshotURI(backupCfg, backupDir)
	if err != nil {
		return snapshotSource{}, err
	}
	source := snapshotSource{dirURI: uri}

	// With no spec Milvus reads with its own credential, which is what to rely on when both
	// sides are the same backend — it keeps the access key off the wire.
	if storage.SameBackend(milvusCfg, backupCfg) {
		return source, nil
	}

	spec, err := storage.SnapshotExternalSpec(backupCfg)
	if err != nil {
		return snapshotSource{}, err
	}
	source.externalSpec = spec

	return source, nil
}

// metadataURI rebuilds the uri of one bundle's metadata. The backup meta records that path
// relative to the backup directory, so a backup that was moved to another bucket or prefix
// resolves against wherever it is being read from now.
func (s snapshotSource) metadataURI(metadataPath string) (string, error) {
	p := strings.Trim(metadataPath, "/")
	if p == "" {
		return "", fmt.Errorf("restore: backup meta records no snapshot metadata path")
	}
	// Joining an absolute uri onto the backup directory resolves to neither of them.
	if strings.Contains(p, "://") {
		return "", fmt.Errorf("restore: snapshot metadata path %s is not relative to the backup directory", metadataPath)
	}

	return s.dirURI + "/" + p, nil
}
