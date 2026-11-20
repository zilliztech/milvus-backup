package snapshotsource

import (
	"context"
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/storage"
)

// SourceKey picks the shape of the source location a snapshot import takes:
// SourceKeyURI names one bucket with a complete URI, SourceKeyRelative names a
// bucket-relative object key that every cluster resolves against its own bucket.
type SourceKey string

const (
	// SourceKeyURI is the default: a complete URI in one of the shapes Milvus
	// parses, admitted against one bucket and endpoint.
	SourceKeyURI SourceKey = ""
	// SourceKeyRelative makes each cluster read the bundle from its own
	// configured bucket at the same object key, with its own credential. Only
	// the import api takes a bare key, and only with no external spec — the
	// bundle has to be staged in every cluster's bucket beforehand.
	SourceKeyRelative SourceKey = "relative"
)

// ParseSourceKey validates the user-facing spelling of a source mode: shared
// (the default, like empty) is today's complete uri pointed at one bucket,
// own_bucket is the bucket-relative key every cluster resolves in its own.
func ParseSourceKey(s string) (SourceKey, error) {
	switch s {
	case "", "shared":
		return SourceKeyURI, nil
	case "own_bucket":
		return SourceKeyRelative, nil
	default:
		return SourceKeyURI, fmt.Errorf("invalid snapshot source %q, only support [shared, own_bucket]", s)
	}
}

// Source says where the bundles of a snapshot-format backup are read from, in the
// terms RestoreExternalSnapshot and a snapshot-sourced import take them.
type Source struct {
	// DirURI is the backup directory as a complete URI, which the paths in the backup meta
	// are relative to. In the bucket-relative mode it is the bare key of the backup
	// directory instead, which every cluster resolves against its own bucket.
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
}

// New resolves where Milvus reads the backup's bundles from. The restore api
// copies data inside one storage service, so crossServiceOK=false refuses a backup held on
// another service; the import path reads the source itself, and crossServiceOK=true lets
// that through on the external spec. SourceKeyRelative hands out the bare key of the
// backup directory instead of any URI — see relativeSource.
func New(ctx context.Context, milvusCfg, backupCfg storage.Config, backupDir string, crossServiceOK bool, sourceKey SourceKey) (Source, error) {
	if sourceKey == SourceKeyRelative {
		return relativeSource(milvusCfg, backupCfg, backupDir, crossServiceOK)
	}

	sameService := storage.SnapshotSameService(milvusCfg, backupCfg)
	if !sameService && !crossServiceOK {
		return Source{}, fmt.Errorf("snapshot source: the restore api copies data inside one storage service, but the milvus storage (%s) and the backup storage (%s) are not the same one; "+
			"on a Milvus 3.0.4 or newer target the import_snap strategy reads the source directly, otherwise move the backup onto the milvus storage's service first",
			storage.SnapshotStoreDescription(milvusCfg), storage.SnapshotStoreDescription(backupCfg))
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

// relativeSource reports the backup directory as a bare object key: every cluster in the
// restore — the target and each secondary — then reads the bundle from its own configured
// bucket at that key, with its own credential, so no spec is sent and neither endpoint nor
// bucket is ever named. The shape the import api takes since it accepted bucket-relative
// keys; the restore api copies data within one storage service and still needs a complete
// URI, so it is refused here. A source sas is refused too: the import it was minted for
// never happens, and a spec next to a bare key is rejected by the server anyway.
func relativeSource(milvusCfg, backupCfg storage.Config, backupDir string, crossServiceOK bool) (Source, error) {
	if !crossServiceOK {
		return Source{}, fmt.Errorf("snapshot source: a bucket-relative source key is only taken by the import api; restore with --secondaries or --strategy=import_snap, or keep snapshot_source shared for a complete-uri source")
	}
	if backupCfg.SourceSAS != "" {
		return Source{}, fmt.Errorf("snapshot source: a source sas token applies only to a snapshot copy that crosses azure storage accounts")
	}

	key := strings.Trim(backupDir, "/")
	if key == "" {
		return Source{}, fmt.Errorf("snapshot source: backup meta records no backup directory")
	}
	if strings.Contains(key, "://") {
		return Source{}, fmt.Errorf("snapshot source: backup dir %s is a uri; a bucket-relative source key names the object key inside every cluster's own bucket", key)
	}

	return Source{DirURI: key, SameService: storage.SnapshotSameService(milvusCfg, backupCfg)}, nil
}

// MetadataURI rebuilds the uri of one bundle's metadata — its bucket-relative object
// key in the bucket-relative mode. The backup meta records that path
// relative to the backup directory, so a backup that was moved to another bucket or prefix
// resolves against wherever it is being read from now.
func (s Source) MetadataURI(metadataPath string) (string, error) {
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
