package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/storage"
)

// snapshotSource says where the bundles of a snapshot-format backup are read from, in the
// terms RestoreExternalSnapshot and a snapshot-sourced import take them.
type snapshotSource struct {
	// dirURI is the backup directory as a complete URI, which the paths in the backup meta
	// are relative to.
	dirURI string

	// externalSpec carries the credentials for the source, and is empty when Milvus can
	// reach it with its own.
	externalSpec string

	// sourceSASSet reports whether the spec carries a source SAS, for logging: the
	// token itself is a secret and never appears there.
	sourceSASSet bool

	// sameService reports whether the backup storage and the milvus storage are one
	// storage service. The restore api path needs it to be; the import path reads the
	// source through the external spec and does not.
	sameService bool
}

// newSnapshotSource resolves where Milvus reads the backup's bundles from. The restore api
// copies data inside one storage service, so crossServiceOK=false refuses a backup held on
// another service; the import path reads the source itself, and crossServiceOK=true lets
// that through on the external spec.
func newSnapshotSource(ctx context.Context, milvusCfg, backupCfg storage.Config, backupDir string, crossServiceOK bool) (snapshotSource, error) {
	sameService := storage.SnapshotSameService(milvusCfg, backupCfg)
	if !sameService && !crossServiceOK {
		return snapshotSource{}, fmt.Errorf("restore: the restore api copies data inside one storage service, but the milvus storage (%s) and the backup storage (%s) are not the same one; "+
			"on a Milvus 3.0.4 or newer target the import_snap strategy reads the source directly, otherwise move the backup onto the milvus storage's service first",
			storage.SnapshotStoreDescription(milvusCfg), storage.SnapshotStoreDescription(backupCfg))
	}

	uri, err := storage.SnapshotStoreURI(milvusCfg, backupCfg, backupDir)
	if err != nil {
		return snapshotSource{}, err
	}
	source := snapshotSource{dirURI: uri, sameService: sameService}

	// The copy source is the backup bucket. On Azure it lives in the backup
	// storage account, and no single principal can authorize reading a blob of
	// another account, so a cross-account restore needs a read-scoped SAS on the
	// source — minted from (or provided with) the backup account.
	if storage.CrossAccountAzure(backupCfg, milvusCfg) {
		backupCfg, err = storage.ResolveSourceSAS(ctx, backupCfg)
		if err != nil {
			return snapshotSource{}, fmt.Errorf("restore: source sas for the backup storage: %w", err)
		}
	} else if backupCfg.SourceSAS != "" {
		// A SAS anywhere else is a misconfiguration the server would reject, so
		// fail here where the config key can be pointed at.
		return snapshotSource{}, fmt.Errorf("restore: a source sas token applies only to a snapshot copy that crosses azure storage accounts")
	}

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
	source.sourceSASSet = backupCfg.SourceSAS != ""

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
