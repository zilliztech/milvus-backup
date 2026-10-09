package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

// Milvus moves snapshot bundles in and out of object storage itself, and takes two things to
// do it: a uri naming the object, and an extfs spec authorizing access to it. Export and
// restore describe the same store in opposite directions, so both build them from here.

// SnapshotURI names key in storeCfg's bucket, in one of the shapes Milvus parses:
// minio://<endpoint>/<bucket>/<key> puts the endpoint in the host, s3://<bucket>/<key> leaves
// Milvus to derive it from cloud_provider and region, and gcs://<bucket>/<key> names a native
// GCS store, whose provider the scheme alone decides.
//
// The endpoint written into the URI is the one Milvus connects to, so the Milvus-view
// override wins when there is one, then the configured endpoint: derivation only ever
// produces the canonical public endpoint — wrong for a deployment reached over an internal
// or private-link address, and wrong as a copy failure rather than a configuration error.
func SnapshotURI(storeCfg Config, key string) (string, error) {
	key = strings.Trim(key, "/")
	if key == "" {
		return "", fmt.Errorf("storage: snapshot uri needs a key")
	}
	if _, err := snapshotCloudProvider(storeCfg.Provider); err != nil {
		return "", err
	}

	// Milvus's snapshot validator recognizes an Azure URI host only as the
	// account-qualified endpoint (<account>.blob.<service-endpoint>) or the bare
	// service endpoint, and rejects anything else as not matching the configured
	// endpoint — so the URI names the account-qualified host. The account name is
	// always set, since the Azure client itself needs it to build the blob
	// service URL.
	if storeCfg.Provider == cfg.ProviderAzure {
		if storeCfg.MilvusEndpoint != "" {
			// The override names the exact host Milvus resolves, account included.
			host := endpointHost(storeCfg.MilvusEndpoint)
			if host == "" {
				return "", fmt.Errorf("storage: snapshot uri for azure needs an endpoint")
			}
			return fmt.Sprintf("azure://%s/%s/%s", host, storeCfg.Bucket, key), nil
		}
		if storeCfg.Credential.AzureAccountName == "" {
			return "", fmt.Errorf("storage: snapshot uri for azure needs an account name")
		}
		host := endpointHost(storeCfg.Endpoint)
		if host == "" {
			return "", fmt.Errorf("storage: snapshot uri for azure needs an endpoint")
		}
		// Milvus reports the completed export's metadata URI with the default
		// https port dropped; drop it here too, or the backup meta's cross-check
		// of the two spellings fails on the port alone.
		host = strings.TrimSuffix(host, ":443")
		return fmt.Sprintf("azure://%s.blob.%s/%s/%s", storeCfg.Credential.AzureAccountName, host, storeCfg.Bucket, key), nil
	}

	// Native GCS is reached through its own client, not an S3-compatible endpoint, so the
	// scheme names it and neither endpoint nor region is needed. The bucket is global.
	if storeCfg.Provider == cfg.ProviderGCPNative {
		return fmt.Sprintf("gcs://%s/%s", storeCfg.Bucket, key), nil
	}

	endpoint := storeCfg.Endpoint
	if storeCfg.MilvusEndpoint != "" {
		endpoint = storeCfg.MilvusEndpoint
	}
	if host := endpointHost(endpoint); host != "" {
		return fmt.Sprintf("minio://%s/%s/%s", host, storeCfg.Bucket, key), nil
	}

	// No endpoint to name, so Milvus has to derive one, and every provider it can derive for
	// needs the region to do it.
	if storeCfg.Region == "" {
		return "", fmt.Errorf("storage: snapshot uri for %s needs an endpoint or a region", storeCfg.Provider)
	}

	return fmt.Sprintf("s3://%s/%s", storeCfg.Bucket, key), nil
}

// SnapshotStoreURI names key in the backup bucket, in the form Milvus should resolve it
// in. A snapshot export or restore is executed by Milvus, so the endpoint in the URI must
// be one Milvus can reach — not the one milvus-backup reaches. When both configs describe
// the same backend and no Milvus-view endpoint is pinned, the endpoint is omitted
// entirely and Milvus resolves the bucket through its own storage config: the endpoint
// milvus-backup would name is only its own view of that backend, and may be an alias — a
// container port mapping, a private link — that Milvus cannot connect to, or rejects as a
// foreign cross-bucket target.
func SnapshotStoreURI(milvusCfg, backupCfg Config, key string) (string, error) {
	// Azure has no endpoint-less URI form: its service endpoint is part of every URI.
	if backupCfg.Provider != cfg.ProviderAzure && backupCfg.MilvusEndpoint == "" &&
		SameBackend(milvusCfg, backupCfg) {
		return snapshotURIOnInstance(backupCfg, key)
	}
	return SnapshotURI(backupCfg, key)
}

// snapshotURIOnInstance builds the endpoint-less form of a snapshot URI: bucket and key
// only, leaving the endpoint for Milvus to fill in from its own storage config. Only the
// S3 family has such a form: Azure names its service endpoint in every URI, and native
// GCS never carries one.
func snapshotURIOnInstance(storeCfg Config, key string) (string, error) {
	key = strings.Trim(key, "/")
	if key == "" {
		return "", fmt.Errorf("storage: snapshot uri needs a key")
	}
	if _, err := snapshotCloudProvider(storeCfg.Provider); err != nil {
		return "", err
	}

	if storeCfg.Provider == cfg.ProviderAzure {
		return "", fmt.Errorf("storage: snapshot uri for azure cannot omit the endpoint")
	}
	if storeCfg.Provider == cfg.ProviderGCPNative {
		return fmt.Sprintf("gcs://%s/%s", storeCfg.Bucket, key), nil
	}

	return fmt.Sprintf("s3://%s/%s", storeCfg.Bucket, key), nil
}

// SnapshotExternalSpec renders storeCfg as the extfs json Milvus expects. It only overrides what it
// names: the server starts from its own storage config and applies these on top.
func SnapshotExternalSpec(storeCfg Config) (string, error) {
	cloudProvider, err := snapshotCloudProvider(storeCfg.Provider)
	if err != nil {
		return "", err
	}

	extfs := map[string]string{
		"cloud_provider": cloudProvider,
		"use_ssl":        strconv.FormatBool(storeCfg.UseSSL),
	}
	if storeCfg.Region != "" {
		extfs["region"] = storeCfg.Region
	}

	switch storeCfg.Credential.Type {
	case Static:
		// extfs has no session token field, so a temporary credential would be sent as a
		// permanent one and fail to authorize with nothing pointing at why.
		if storeCfg.Credential.Token != "" {
			return "", fmt.Errorf("storage: snapshot external spec cannot carry a session token")
		}
		extfs["access_key_id"] = storeCfg.Credential.AK
		extfs["access_key_value"] = storeCfg.Credential.SK
	case IAM:
		extfs["use_iam"] = "true"
		if storeCfg.Credential.IAMEndpoint != "" {
			extfs["iam_endpoint"] = storeCfg.Credential.IAMEndpoint
		}
	case GCPCredJSON:
		data, err := os.ReadFile(storeCfg.Credential.GCPCredJSON)
		if err != nil {
			return "", fmt.Errorf("storage: read gcp credential file: %w", err)
		}
		extfs["credential_json"] = string(data)
	default:
		return "", fmt.Errorf("storage: snapshot external spec cannot carry %s credentials", storeCfg.Credential.Type)
	}

	// A cross-account Azure copy reads its source under this SAS: neither the
	// destination credential above nor the destination account's own identity
	// can authorize reading another account's blobs, so the source read rides
	// on the token instead. Azure is the only provider with such a grant.
	if storeCfg.SourceSAS != "" {
		if storeCfg.Provider != cfg.ProviderAzure {
			return "", fmt.Errorf("storage: snapshot external spec cannot carry a source sas for %s storage", storeCfg.Provider)
		}
		extfs["source_sas_token"] = storeCfg.SourceSAS
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
	case cfg.ProviderS3, cfg.ProviderAWS:
		return "aws", nil
	case cfg.ProviderMinio:
		return "minio", nil
	case cfg.ProviderTencent:
		return "tencent", nil
	case cfg.ProviderAliyun:
		return "aliyun", nil
	case cfg.ProviderHwc:
		return "huawei", nil
	case cfg.ProviderAzure:
		return "azure", nil
	case cfg.ProviderGCP:
		return "gcp", nil
	case cfg.ProviderGCPNative:
		return "gcpnative", nil
	default:
		return "", fmt.Errorf("storage: milvus snapshots do not support %s storage", provider)
	}
}

// SnapshotSameService reports whether a snapshot copy between a and b stays
// inside one storage service. Milvus moves snapshot bundles with a server-side
// copy addressed to a single service, so the copy can span that service's
// buckets, accounts and regions, but never a second provider — and never a
// second endpoint of a self-hosted store, which is a service of its own. The
// binlog format has no such limit: it streams the bytes through milvus-backup.
func SnapshotSameService(a, b Config) bool {
	if SameBackend(a, b) {
		return true
	}

	aProvider, err := snapshotCloudProvider(a.Provider)
	if err != nil {
		// An unsupported provider is rejected where the snapshot uri is built;
		// here it only means the copy cannot be one service's work.
		return false
	}
	bProvider, err := snapshotCloudProvider(b.Provider)
	if err != nil {
		return false
	}
	if aProvider != bProvider {
		return false
	}

	if a.Provider == cfg.ProviderMinio {
		// A self-hosted store is its endpoint, as Milvus reaches it: two
		// endpoints are two services, whatever the provider string says.
		return milvusViewEndpoint(a) == milvusViewEndpoint(b)
	}

	// A cloud provider's service spans its accounts and regions, so a copy
	// between two of its endpoints is still one service's work.
	return true
}

// SnapshotStoreDescription renders the identity a snapshot copy is confined to,
// for errors and logs: the provider, and the endpoint when there is one.
func SnapshotStoreDescription(storeCfg Config) string {
	if storeCfg.Endpoint != "" {
		return storeCfg.Provider + " at " + storeCfg.Endpoint
	}
	return storeCfg.Provider
}

// milvusViewEndpoint is the endpoint Milvus connects to, which is the one a
// snapshot copy is addressed to.
func milvusViewEndpoint(storeCfg Config) string {
	if storeCfg.MilvusEndpoint != "" {
		return storeCfg.MilvusEndpoint
	}
	return storeCfg.Endpoint
}

func endpointHost(endpoint string) string {
	host := strings.TrimSpace(endpoint)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")

	return strings.Trim(host, "/")
}
