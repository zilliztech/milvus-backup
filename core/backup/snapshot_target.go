package backup

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
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
	uri, err := snapshotTargetPath(backupCfg, mpath.BackupBundleDir(backupDir))
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

	spec, err := snapshotExternalSpec(backupCfg)
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

// snapshotCloudProvider maps this tool's provider name onto the value Milvus accepts in
// extfs.cloud_provider, and doubles as the check for whether a provider is supported at
// all. The value is always sent: Milvus infers one from the URI scheme when it is
// missing, and its own comment calls that inference a source of silent misconfiguration,
// since s3:// covers both AWS and a self-hosted store.
func snapshotCloudProvider(provider string) (string, error) {
	switch provider {
	case v2.ProviderS3, v2.ProviderAWS:
		return "aws", nil
	case v2.ProviderMinio:
		return "minio", nil
	case v2.ProviderTencent:
		return "tencent", nil
	case v2.ProviderAliyun:
		return "aliyun", nil
	case v2.ProviderHwc:
		return "huawei", nil
	default:
		return "", fmt.Errorf("backup: snapshot export does not support %s backup storage", provider)
	}
}

// snapshotTargetPath builds the bundle root URI, in one of the two shapes Milvus parses:
// minio://<endpoint>/<bucket>/<key> names the endpoint in the host, s3://<bucket>/<key>
// leaves Milvus to derive it from cloud_provider and region.
//
// The configured endpoint wins whenever there is one. Derivation only produces the
// canonical public endpoint for a provider and region, which is wrong for a deployment
// reached over an internal or private-link address — and wrong in a way that surfaces as
// a copy failure rather than a configuration error.
func snapshotTargetPath(cfg storage.Config, backupDir string) (string, error) {
	dir := strings.Trim(backupDir, "/")
	if dir == "" {
		return "", fmt.Errorf("backup: snapshot export target needs a backup directory")
	}
	if _, err := snapshotCloudProvider(cfg.Provider); err != nil {
		return "", err
	}

	if host := endpointHost(cfg.Endpoint); host != "" {
		return fmt.Sprintf("minio://%s/%s/%s", host, cfg.Bucket, dir), nil
	}

	// No endpoint to name, so Milvus has to derive one, and every provider it can derive
	// for needs the region to do it.
	if cfg.Region == "" {
		return "", fmt.Errorf("backup: snapshot export to %s needs an endpoint or a region", cfg.Provider)
	}

	return fmt.Sprintf("s3://%s/%s", cfg.Bucket, dir), nil
}

func endpointHost(endpoint string) string {
	host := strings.TrimSpace(endpoint)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")

	return strings.Trim(host, "/")
}

// snapshotExternalSpec renders the storage config as the extfs json Milvus expects.
// It only overrides what it names: the server starts from its own storage config and
// applies these on top.
func snapshotExternalSpec(cfg storage.Config) (string, error) {
	cloudProvider, err := snapshotCloudProvider(cfg.Provider)
	if err != nil {
		return "", err
	}

	extfs := map[string]string{
		"cloud_provider": cloudProvider,
		"use_ssl":        strconv.FormatBool(cfg.UseSSL),
	}
	if cfg.Region != "" {
		extfs["region"] = cfg.Region
	}

	switch cfg.Credential.Type {
	case storage.Static:
		// extfs has no session token field, so a temporary credential would be sent
		// as a permanent one and fail to authorize with nothing pointing at why.
		if cfg.Credential.Token != "" {
			return "", fmt.Errorf("backup: snapshot export cannot pass a session token to milvus")
		}
		extfs["access_key_id"] = cfg.Credential.AK
		extfs["access_key_value"] = cfg.Credential.SK
	case storage.IAM:
		extfs["use_iam"] = "true"
		if cfg.Credential.IAMEndpoint != "" {
			extfs["iam_endpoint"] = cfg.Credential.IAMEndpoint
		}
	default:
		return "", fmt.Errorf("backup: snapshot export cannot pass %s credentials to milvus", cfg.Credential.Type)
	}

	byts, err := json.Marshal(map[string]any{"extfs": extfs})
	if err != nil {
		return "", fmt.Errorf("backup: marshal snapshot external spec: %w", err)
	}

	return string(byts), nil
}
