package snapshotsource

import (
	"context"
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/storage"
)

// Source says where the bundles of a snapshot-format backup are read from, in the
// terms RestoreExternalSnapshot and a snapshot-sourced import take them.
type Source struct {
	// DirURI is the backup directory as a complete URI, which the paths in the backup meta
	// are relative to. Empty while staging: then the uri is known only once the bundle
	// has been copied into the milvus storage.
	DirURI string

	// ExternalSpec carries the credentials for the source, and is empty when Milvus can
	// reach it with its own.
	ExternalSpec string

	// SourceSASSet reports whether the spec carries a source SAS, for logging: the
	// token itself is a secret and never appears there.
	SourceSASSet bool

	// SameService reports whether the backup storage and the milvus storage are one
	// storage service. The restore api path needs it to be; the import path reads the
	// source through the external spec and does not.
	SameService bool

	// Staging reports that the bundles must be copied into the milvus storage before the
	// restore: the restore api copies data inside one storage service, and a backup held
	// on another service has no such primitive, so milvus-backup streams the bundle over
	// itself. The import path never stages — it reads the source through the spec.
	Staging bool
}

// New resolves where Milvus reads the backup's bundles from. A backup on the target's
// storage service is read in place. One on another service is read through the external
// spec when crossServiceOK selects the import path, and staged into the milvus storage
// first otherwise, for the restore api path.
func New(ctx context.Context, milvusCfg, backupCfg storage.Config, backupDir string, crossServiceOK bool) (Source, error) {
	sameService := storage.SnapshotSameService(milvusCfg, backupCfg)
	if !sameService && !crossServiceOK {
		// Staging still needs the milvus storage to be a kind a snapshot uri can name —
		// probe it before any copying starts.
		if _, err := storage.SnapshotStoreURI(milvusCfg, milvusCfg, backupDir); err != nil {
			return Source{}, err
		}
		return Source{Staging: true}, nil
	}

	uri, err := storage.SnapshotStoreURI(milvusCfg, backupCfg, backupDir)
	if err != nil {
		return Source{}, err
	}
	source := Source{DirURI: uri, SameService: sameService}

	// The copy source is the backup bucket. On Azure it lives in the backup
	// storage account, and no single principal can authorize reading a blob of
	// another account, so a cross-account restore needs a read-scoped SAS on the
	// source — minted from (or provided with) the backup account.
	if storage.CrossAccountAzure(backupCfg, milvusCfg) {
		backupCfg, err = storage.ResolveSourceSAS(ctx, backupCfg)
		if err != nil {
			return Source{}, fmt.Errorf("snapshot source: source sas for the backup storage: %w", err)
		}
	} else if backupCfg.SourceSAS != "" {
		// A SAS anywhere else is a misconfiguration the server would reject, so
		// fail here where the config key can be pointed at.
		return Source{}, fmt.Errorf("snapshot source: a source sas token applies only to a snapshot copy that crosses azure storage accounts")
	}

	// With no spec Milvus reads with its own credential, which is what to rely on when both
	// sides are the same backend — it keeps the access key off the wire.
	if storage.SameBackend(milvusCfg, backupCfg) {
		return source, nil
	}

	spec, err := storage.SnapshotExternalSpec(backupCfg)
	if err != nil {
		return Source{}, err
	}
	source.ExternalSpec = spec
	source.SourceSASSet = backupCfg.SourceSAS != ""

	return source, nil
}

// MetadataURI rebuilds the uri of one bundle's metadata. The backup meta records that path
// relative to the backup directory, so a backup that was moved to another bucket or prefix
// resolves against wherever it is being read from now.
func (s Source) MetadataURI(metadataPath string) (string, error) {
	if s.DirURI == "" {
		return "", fmt.Errorf("snapshot source: a staging source has no uri until the bundle is staged")
	}

	p := strings.Trim(metadataPath, "/")
	if p == "" {
		return "", fmt.Errorf("snapshot source: backup meta records no snapshot metadata path")
	}
	// Joining an absolute uri onto the backup directory resolves to neither of them.
	if strings.Contains(p, "://") {
		return "", fmt.Errorf("snapshot source: snapshot metadata path %s is not relative to the backup directory", metadataPath)
	}

	return s.DirURI + "/" + p, nil
}
