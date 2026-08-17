package backup

import (
	"fmt"
	"net"
	"net/url"
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
	uri, err := storage.SnapshotStoreURI(milvusCfg, backupCfg, mpath.BackupBundleDir(backupDir))
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
	target, err := parseSnapshotURI(t.Path)
	if err != nil {
		return "", fmt.Errorf("backup: parse snapshot target %s: %w", t.Path, err)
	}
	metadata, err := parseSnapshotURI(metadataURI)
	if err != nil {
		return "", fmt.Errorf("backup: parse exported metadata %s: %w", metadataURI, err)
	}

	if !target.sameStorage(metadata) {
		return "", fmt.Errorf("backup: exported metadata %s is not under the target %s", metadataURI, t.Path)
	}

	root := strings.TrimSuffix(target.key, "/") + "/"
	if !strings.HasPrefix(metadata.key, root) {
		return "", fmt.Errorf("backup: exported metadata %s is not under the target %s", metadataURI, t.Path)
	}

	return path.Join(t.Dir, strings.TrimPrefix(metadata.key, root)), nil
}

// snapshotURI is the storage identity and object key encoded by one of the URI
// forms Milvus accepts. Endpoint-style forms put the bucket in the first path
// segment, while provider-style forms put it in the host.
type snapshotURI struct {
	scheme        string
	endpoint      string
	bucket        string
	key           string
	endpointStyle bool
}

func parseSnapshotURI(raw string) (snapshotURI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return snapshotURI{}, err
	}
	if u.Scheme == "" || u.Host == "" {
		return snapshotURI{}, fmt.Errorf("uri needs a scheme and host")
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return snapshotURI{}, fmt.Errorf("uri must not contain credentials, query parameters, or a fragment")
	}

	objectPath, err := url.PathUnescape(u.EscapedPath())
	if err != nil {
		return snapshotURI{}, fmt.Errorf("unescape object path: %w", err)
	}
	objectPath, err = cleanSnapshotObjectKey(objectPath)
	if err != nil {
		return snapshotURI{}, err
	}

	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "minio", "http", "https", "az", "azure":
		parts := strings.SplitN(objectPath, "/", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return snapshotURI{}, fmt.Errorf("endpoint-style uri needs a bucket and object key")
		}
		endpoint, err := snapshotEndpoint(u, scheme)
		if err != nil {
			return snapshotURI{}, err
		}
		return snapshotURI{
			scheme:        scheme,
			endpoint:      endpoint,
			bucket:        parts[0],
			key:           parts[1],
			endpointStyle: true,
		}, nil
	case "s3", "gs", "gcs":
		if u.Port() != "" {
			return snapshotURI{}, fmt.Errorf("provider-style uri bucket must not contain a port")
		}
		return snapshotURI{
			scheme: scheme,
			bucket: u.Host,
			key:    objectPath,
		}, nil
	default:
		return snapshotURI{}, fmt.Errorf("unsupported snapshot uri scheme %q", scheme)
	}
}

func cleanSnapshotObjectKey(objectPath string) (string, error) {
	objectPath = strings.Trim(objectPath, "/")
	if objectPath == "" {
		return "", fmt.Errorf("uri needs an object key")
	}
	for _, part := range strings.Split(objectPath, "/") {
		if part == "." || part == ".." {
			return "", fmt.Errorf("object key must not contain path traversal")
		}
	}
	return path.Clean(objectPath), nil
}

func snapshotEndpoint(u *url.URL, scheme string) (string, error) {
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "" {
		return "", fmt.Errorf("uri needs an endpoint host")
	}
	port := u.Port()
	if port == "" {
		switch scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	if port == "" {
		return host, nil
	}
	return net.JoinHostPort(host, port), nil
}

func (u snapshotURI) sameStorage(other snapshotURI) bool {
	if u.bucket != other.bucket {
		return false
	}
	if u.endpointStyle == other.endpointStyle {
		if u.endpointStyle {
			return u.endpoint == other.endpoint
		}
		return canonicalSnapshotScheme(u.scheme) == canonicalSnapshotScheme(other.scheme)
	}
	// One side names only a bucket: the endpoint was deliberately omitted, and the
	// server spelled the same bucket back with the endpoint it resolved from its own
	// storage config. That endpoint is the server's to pick, so only the bucket
	// (compared above) and the provider family can be matched.
	provider, endpoint := u, other
	if provider.endpointStyle {
		provider, endpoint = other, u
	}
	switch provider.scheme {
	case "s3":
		return endpoint.scheme == "minio" || endpoint.scheme == "http" || endpoint.scheme == "https"
	case "gs", "gcs":
		return endpoint.scheme == "http" || endpoint.scheme == "https"
	default:
		return false
	}
}

func canonicalSnapshotScheme(scheme string) string {
	if scheme == "gcs" {
		return "gs"
	}
	return scheme
}
