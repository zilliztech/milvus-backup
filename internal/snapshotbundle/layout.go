package snapshotbundle

import (
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// SnapshotRootPath is the directory under a bundle root that holds all snapshots.
	SnapshotRootPath = "snapshots"
	// SnapshotMetadataSubPath holds each snapshot's metadata JSON.
	SnapshotMetadataSubPath = "metadata"
	// SnapshotManifestsSubPath holds each snapshot's per-segment avro manifests.
	SnapshotManifestsSubPath = "manifests"
	// SnapshotStagingSubPath holds the private metadata object used to make
	// publication recoverable before the final metadata JSON is committed.
	SnapshotStagingSubPath = "_staging"
	// ExportedSnapshotFilesPath is the directory under a bundle root that holds
	// the copied data files, relocated out of the metadata subtree.
	ExportedSnapshotFilesPath = "files"
)

// GetSnapshotPaths returns the manifest directory and metadata path for one
// snapshot under a bundle root.
func GetSnapshotPaths(rootPath string, collectionID int64, snapshotID int64) (manifestDir, metadataPath string) {
	basePath := path.Join(rootPath, SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	snapshotIDStr := strconv.FormatInt(snapshotID, 10)
	manifestDir = path.Join(basePath, SnapshotManifestsSubPath, snapshotIDStr)
	metadataPath = path.Join(basePath, SnapshotMetadataSubPath, snapshotIDStr+".json")
	return manifestDir, metadataPath
}

// GetSegmentManifestPath returns the avro manifest path for one segment.
func GetSegmentManifestPath(manifestDir string, segmentID int64) string {
	return path.Join(manifestDir, strconv.FormatInt(segmentID, 10)+".avro")
}

// GetSnapshotStagingMetadataPath returns the private staging object a bundle
// writes before committing the final metadata JSON.
func GetSnapshotStagingMetadataPath(rootPath string) string {
	return path.Join(rootPath, SnapshotStagingSubPath, "metadata.json")
}

// GetBundleDataRoot returns the directory under a bundle root where exported
// data files are placed.
func GetBundleDataRoot(rootPath string) string {
	return path.Join(rootPath, ExportedSnapshotFilesPath)
}

// ExportedSnapshotPath maps a source object key to its location inside a
// bundle: the source root is stripped and the remainder lands under
// targetRoot/files. A source key outside the source root is kept whole.
func ExportedSnapshotPath(sourceRoot, src, targetRoot string) string {
	root := strings.TrimSuffix(NormalizeObjectPath(sourceRoot), "/")
	relative := src
	if root != "" {
		if src == root {
			relative = ""
		} else if strings.HasPrefix(src, root+"/") {
			relative = strings.TrimPrefix(src, root+"/")
		}
	}
	return path.Join(targetRoot, ExportedSnapshotFilesPath, relative)
}

// DeriveBundleRoot recovers the bundle root from a metadata path or URI by
// locating the canonical snapshots/<collectionID>/metadata/<snapshotID>.json
// anchor, which restore relies on to find a relocated bundle.
func DeriveBundleRoot(metadataLocation string) (root string, collectionID, snapshotID int64, ok bool) {
	key := NormalizeObjectPath(metadataLocation)
	hasLeadingSlash := strings.HasPrefix(key, "/")
	segments := strings.Split(strings.Trim(key, "/"), "/")
	if len(segments) < 4 {
		return "", 0, 0, false
	}
	anchor := len(segments) - 4
	if segments[anchor] != SnapshotRootPath || segments[anchor+2] != SnapshotMetadataSubPath {
		return "", 0, 0, false
	}
	collectionID, err := strconv.ParseInt(segments[anchor+1], 10, 64)
	if err != nil || collectionID <= 0 {
		return "", 0, 0, false
	}
	metadataFile := segments[anchor+3]
	if !strings.HasSuffix(metadataFile, ".json") {
		return "", 0, 0, false
	}
	snapshotID, err = strconv.ParseInt(strings.TrimSuffix(metadataFile, ".json"), 10, 64)
	if err != nil || snapshotID <= 0 {
		return "", 0, 0, false
	}
	root = joinURIPathSegments(segments[:anchor])
	if hasLeadingSlash && root != "" {
		root = "/" + root
	}
	return root, collectionID, snapshotID, true
}

// NormalizeObjectPath converts a snapshot path reference to a bare object key
// so URI-form and key-form references to the same object compare equal. A
// reference that is not a valid supported URI is returned unchanged.
func NormalizeObjectPath(objectPath string) string {
	if !hasURITransportScheme(objectPath) {
		return objectPath
	}
	_, key, _, err := ParseForeignURI(objectPath)
	if err != nil {
		return objectPath
	}
	return key
}

// ParseForeignURI accepts object keys, bucket-hosted URIs, and endpoint-style
// URIs, mirroring the server's path parsing. Endpoint-style forms such as
// minio://host/bucket/key or https://host/bucket/key keep the endpoint in the
// host result and take the bucket from the first path segment.
func ParseForeignURI(raw string) (bucket, objectKey, endpointHost string, err error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", "", merr.WrapErrParameterInvalidErr(err, "foreign_uri is invalid")
	}
	if parsed.User != nil {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must not embed credentials")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must not include query parameters or fragments")
	}

	scheme := strings.ToLower(parsed.Scheme)
	if !isSupportedForeignScheme(scheme) {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri scheme %q is not supported", scheme)
	}
	if parsed.Host == "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include a bucket or endpoint host")
	}

	parts, err := cleanURIPathSegments(parsed.EscapedPath())
	if err != nil {
		return "", "", "", err
	}

	switch scheme {
	case "minio", "http", "https", "az", "azure":
		if len(parts) < 2 {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include bucket and object key")
		}
		return parts[0], joinURIPathSegments(parts[1:]), parsed.Host, nil
	default:
		if externalspec.IsCloudEndpointHost(parsed.Host) {
			if len(parts) < 2 {
				return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include bucket and object key")
			}
			return parts[0], joinURIPathSegments(parts[1:]), parsed.Host, nil
		}
		if len(parts) == 0 {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include object key")
		}
		return parsed.Host, joinURIPathSegments(parts), "", nil
	}
}

func isSupportedForeignScheme(scheme string) bool {
	switch scheme {
	case "s3", "minio", "gs", "gcs", "az", "azure", "http", "https":
		return true
	default:
		return false
	}
}

func hasURITransportScheme(raw string) bool {
	return strings.Contains(raw, "://")
}

func cleanURIPathSegments(escapedPath string) ([]string, error) {
	unescaped, err := url.PathUnescape(escapedPath)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid foreign_uri path escape: %s", err.Error())
	}
	// Reject traversal on raw URI path segments before path.Join can normalize
	// them away and silently point the reader at a different object key.
	rawParts := strings.Split(strings.Trim(unescaped, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		if part == "" {
			continue
		}
		if part == "." || part == ".." {
			return nil, merr.WrapErrParameterInvalidMsg("foreign_uri object key must not contain path traversal")
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func joinURIPathSegments(segments []string) string {
	if len(segments) == 0 {
		return ""
	}
	return path.Join(segments...)
}
