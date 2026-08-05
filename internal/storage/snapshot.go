package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// Milvus moves snapshot bundles in and out of object storage itself, and takes two things to
// do it: a uri naming the object, and an extfs spec authorizing access to it. Export and
// restore describe the same store in opposite directions, so both build them from here.

// SnapshotURI names key in cfg's bucket, in one of the shapes Milvus parses:
// minio://<endpoint>/<bucket>/<key> puts the endpoint in the host, s3://<bucket>/<key> leaves
// Milvus to derive it from cloud_provider and region, and gcs://<bucket>/<key> names a native
// GCS store, whose provider the scheme alone decides.
//
// The configured endpoint wins whenever there is one, because derivation only ever produces
// the canonical public endpoint — wrong for a deployment reached over an internal or
// private-link address, and wrong as a copy failure rather than a configuration error.
func SnapshotURI(cfg Config, key string) (string, error) {
	key = strings.Trim(key, "/")
	if key == "" {
		return "", fmt.Errorf("storage: snapshot uri needs a key")
	}
	if _, err := snapshotCloudProvider(cfg.Provider); err != nil {
		return "", err
	}

	// Native GCS is reached through its own client, not an S3-compatible endpoint, so the
	// scheme names it and neither endpoint nor region is needed. The bucket is global.
	if cfg.Provider == v2.ProviderGCPNative {
		return fmt.Sprintf("gcs://%s/%s", cfg.Bucket, key), nil
	}

	if host := endpointHost(cfg.Endpoint); host != "" {
		return fmt.Sprintf("minio://%s/%s/%s", host, cfg.Bucket, key), nil
	}

	// No endpoint to name, so Milvus has to derive one, and every provider it can derive for
	// needs the region to do it.
	if cfg.Region == "" {
		return "", fmt.Errorf("storage: snapshot uri for %s needs an endpoint or a region", cfg.Provider)
	}

	return fmt.Sprintf("s3://%s/%s", cfg.Bucket, key), nil
}

// SnapshotExternalSpec renders cfg as the extfs json Milvus expects. It only overrides what it
// names: the server starts from its own storage config and applies these on top.
func SnapshotExternalSpec(cfg Config) (string, error) {
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
	case Static:
		// extfs has no session token field, so a temporary credential would be sent as a
		// permanent one and fail to authorize with nothing pointing at why.
		if cfg.Credential.Token != "" {
			return "", fmt.Errorf("storage: snapshot external spec cannot carry a session token")
		}
		extfs["access_key_id"] = cfg.Credential.AK
		extfs["access_key_value"] = cfg.Credential.SK
	case IAM:
		extfs["use_iam"] = "true"
		if cfg.Credential.IAMEndpoint != "" {
			extfs["iam_endpoint"] = cfg.Credential.IAMEndpoint
		}
	case GCPCredJSON:
		data, err := os.ReadFile(cfg.Credential.GCPCredJSON)
		if err != nil {
			return "", fmt.Errorf("storage: read gcp credential file: %w", err)
		}
		extfs["credential_json"] = string(data)
	default:
		return "", fmt.Errorf("storage: snapshot external spec cannot carry %s credentials", cfg.Credential.Type)
	}

	byts, err := json.Marshal(map[string]any{"extfs": extfs})
	if err != nil {
		return "", fmt.Errorf("storage: marshal snapshot external spec: %w", err)
	}

	return string(byts), nil
}

// snapshotCloudProvider maps this tool's provider name onto the value Milvus accepts in
// extfs.cloud_provider, and doubles as the check for whether a provider is supported at all.
// The value is always sent: Milvus infers one from the uri scheme when it is missing, and its
// own comment calls that inference a source of silent misconfiguration, since s3:// covers
// both AWS and a self-hosted store.
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
	case v2.ProviderGCP:
		return "gcp", nil
	case v2.ProviderGCPNative:
		return "gcpnative", nil
	default:
		return "", fmt.Errorf("storage: milvus snapshots do not support %s storage", provider)
	}
}

func endpointHost(endpoint string) string {
	host := strings.TrimSpace(endpoint)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")

	return strings.Trim(host, "/")
}
