package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

func TestSnapshotURI(t *testing.T) {
	// The configured endpoint goes in the host, whatever the provider: Milvus uses it
	// verbatim there, where derivation would only ever produce the canonical public one.
	t.Run("EndpointGoesInTheURI", func(t *testing.T) {
		tests := []struct {
			name     string
			provider string
			endpoint string
			want     string
		}{
			{"Minio", v2.ProviderMinio, "minio:9000", "minio://minio:9000/backup-bucket/backup/mybackup"},
			{"MinioWithScheme", v2.ProviderMinio, "http://minio:9000", "minio://minio:9000/backup-bucket/backup/mybackup"},
			{"Tencent", v2.ProviderTencent, "cos.ap-guangzhou.myqcloud.com", "minio://cos.ap-guangzhou.myqcloud.com/backup-bucket/backup/mybackup"},
			{"Aliyun", v2.ProviderAliyun, "oss-cn-hangzhou.aliyuncs.com", "minio://oss-cn-hangzhou.aliyuncs.com/backup-bucket/backup/mybackup"},
			{"Huawei", v2.ProviderHwc, "obs.cn-north-4.myhuaweicloud.com", "minio://obs.cn-north-4.myhuaweicloud.com/backup-bucket/backup/mybackup"},
			{"AWS", v2.ProviderAWS, "s3.us-west-2.amazonaws.com", "minio://s3.us-west-2.amazonaws.com/backup-bucket/backup/mybackup"},
			{"GCP", v2.ProviderGCP, "storage.googleapis.com", "minio://storage.googleapis.com/backup-bucket/backup/mybackup"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := Config{Provider: tt.provider, Bucket: "backup-bucket", Endpoint: tt.endpoint}
				got, err := SnapshotURI(cfg, "backup/mybackup")
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	// Without one, Milvus derives the endpoint from cloud_provider and region.
	t.Run("NoEndpointLeavesItToMilvus", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderS3, Bucket: "backup-bucket", Region: "us-west-2"}
		got, err := SnapshotURI(cfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", got)
	})

	// GCP without an endpoint is an S3-family store, so Milvus derives the GCS endpoint
	// from cloud_provider and region the same way it does for AWS.
	t.Run("GCPNoEndpointUsesS3SchemeWithRegion", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderGCP, Bucket: "backup-bucket", Region: "us-west1"}
		got, err := SnapshotURI(cfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", got)
	})

	// Native GCS is not an S3-family store: the gcs:// scheme alone tells Milvus which
	// client to build, so neither endpoint nor region is needed.
	t.Run("GCPNativeUsesGcsScheme", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderGCPNative, Bucket: "backup-bucket"}
		got, err := SnapshotURI(cfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "gcs://backup-bucket/backup/mybackup", got)
	})

	// Every provider Milvus can derive an endpoint for needs the region to do it, so
	// neither being set is refused here rather than as a copy failure later.
	t.Run("NeedsAnEndpointOrARegion", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderS3, Bucket: "backup-bucket"}
		_, err := SnapshotURI(cfg, "backup/mybackup")
		assert.ErrorContains(t, err, "region")
	})

	// Azure reaches its containers through the blob. service endpoint, which the tool
	// config holds without the blob. prefix (its own client prepends it), so the snapshot
	// uri restores the full service host and leaves the account name to the extfs.
	t.Run("AzureUsesAzureScheme", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderAzure, Bucket: "backup-bucket", Endpoint: "core.windows.net:443"}
		got, err := SnapshotURI(cfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "azure://blob.core.windows.net:443/backup-bucket/backup/mybackup", got)
	})

	t.Run("AzureNeedsAnEndpoint", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderAzure, Bucket: "backup-bucket"}
		_, err := SnapshotURI(cfg, "backup/mybackup")
		assert.ErrorContains(t, err, "endpoint")
	})

	t.Run("UnsupportedProvider", func(t *testing.T) {
		cfg := Config{Provider: "madeup", Bucket: "backup-bucket", Endpoint: "acct.blob.core.windows.net"}
		_, err := SnapshotURI(cfg, "backup/mybackup")
		assert.ErrorContains(t, err, "madeup")
	})
}

func TestSnapshotExternalSpec(t *testing.T) {
	t.Run("Static", func(t *testing.T) {
		cfg := Config{
			Provider:   v2.ProviderTencent,
			Region:     "ap-guangzhou",
			UseSSL:     true,
			Credential: Credential{Type: Static, AK: "ak", SK: "sk"},
		}

		spec, err := SnapshotExternalSpec(cfg)
		require.NoError(t, err)
		assert.JSONEq(t, `{"extfs":{"cloud_provider":"tencent","access_key_id":"ak","access_key_value":"sk","region":"ap-guangzhou","use_ssl":"true"}}`, spec)
	})

	t.Run("IAM", func(t *testing.T) {
		cfg := Config{
			Provider:   v2.ProviderAWS,
			Credential: Credential{Type: IAM, IAMEndpoint: "http://169.254.169.254"},
		}

		spec, err := SnapshotExternalSpec(cfg)
		require.NoError(t, err)
		assert.JSONEq(t, `{"extfs":{"cloud_provider":"aws","iam_endpoint":"http://169.254.169.254","use_iam":"true","use_ssl":"false"}}`, spec)
	})

	// For azure the static key pair is the account name and the account key: Milvus
	// reads access_key_id as the storage account, matching what the tool signs with.
	t.Run("AzureAccountKey", func(t *testing.T) {
		cfg := Config{
			Provider:   v2.ProviderAzure,
			Credential: Credential{Type: Static, AK: "azure-account", SK: "azure-key"},
		}

		spec, err := SnapshotExternalSpec(cfg)
		require.NoError(t, err)
		assert.JSONEq(t, `{"extfs":{"cloud_provider":"azure","access_key_id":"azure-account","access_key_value":"azure-key","use_ssl":"false"}}`, spec)
	})

	// The GCP backup pod reaches GCS through workload identity, so the spec asks Milvus
	// to do the same: cloud_provider gcp puts it in the S3 family, use_iam leaves it to
	// the node's own credentials.
	t.Run("GCPIAM", func(t *testing.T) {
		cfg := Config{
			Provider:   v2.ProviderGCP,
			Region:     "us-west1",
			UseSSL:     true,
			Credential: Credential{Type: IAM},
		}

		spec, err := SnapshotExternalSpec(cfg)
		require.NoError(t, err)
		assert.JSONEq(t, `{"extfs":{"cloud_provider":"gcp","region":"us-west1","use_iam":"true","use_ssl":"true"}}`, spec)
	})

	// Native GCS is authorized with the service-account json itself, the same file the
	// gcpnative client reads to talk to the bucket.
	t.Run("GCPNativeServiceAccount", func(t *testing.T) {
		saPath := filepath.Join(t.TempDir(), "service-account.json")
		require.NoError(t, os.WriteFile(saPath, []byte(`{"type":"service_account","project_id":"snapshot-project"}`), 0o600))

		cfg := Config{
			Provider:   v2.ProviderGCPNative,
			Credential: Credential{Type: GCPCredJSON, GCPCredJSON: saPath},
		}

		spec, err := SnapshotExternalSpec(cfg)
		require.NoError(t, err)
		assert.JSONEq(t, `{"extfs":{"cloud_provider":"gcpnative","credential_json":"{\"type\":\"service_account\",\"project_id\":\"snapshot-project\"}","use_ssl":"false"}}`, spec)
	})

	// extfs has no session token field, so sending the key pair alone would fail to
	// authorize with nothing pointing at the cause.
	t.Run("RejectsSessionToken", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderAWS, Credential: Credential{Type: Static, AK: "ak", SK: "sk", Token: "token"}}
		_, err := SnapshotExternalSpec(cfg)
		assert.Error(t, err)
	})

	t.Run("RejectsUnsupportedCredential", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderAWS, Credential: Credential{Type: MinioCredProvider}}
		_, err := SnapshotExternalSpec(cfg)
		assert.Error(t, err)
	})
}
