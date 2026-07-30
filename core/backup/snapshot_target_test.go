package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestSnapshotTargetPath(t *testing.T) {
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
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := storage.Config{Provider: tt.provider, Bucket: "backup-bucket", Endpoint: tt.endpoint}
				got, err := snapshotTargetPath(cfg, "backup/mybackup")
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	// Without one, Milvus derives the endpoint from cloud_provider and region.
	t.Run("NoEndpointLeavesItToMilvus", func(t *testing.T) {
		cfg := storage.Config{Provider: v2.ProviderS3, Bucket: "backup-bucket", Region: "us-west-2"}
		got, err := snapshotTargetPath(cfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup", got)
	})

	// Every provider Milvus can derive an endpoint for needs the region to do it, so
	// neither being set is refused here rather than as a copy failure later.
	t.Run("NeedsAnEndpointOrARegion", func(t *testing.T) {
		cfg := storage.Config{Provider: v2.ProviderS3, Bucket: "backup-bucket"}
		_, err := snapshotTargetPath(cfg, "backup/mybackup")
		assert.ErrorContains(t, err, "region")
	})

	t.Run("UnsupportedProvider", func(t *testing.T) {
		cfg := storage.Config{Provider: v2.ProviderAzure, Bucket: "backup-bucket", Endpoint: "acct.blob.core.windows.net"}
		_, err := snapshotTargetPath(cfg, "backup/mybackup")
		assert.ErrorContains(t, err, v2.ProviderAzure)
	})
}

func TestSnapshotExternalSpec(t *testing.T) {
	t.Run("Static", func(t *testing.T) {
		cfg := storage.Config{
			Provider:   v2.ProviderTencent,
			Region:     "ap-guangzhou",
			UseSSL:     true,
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		spec, err := snapshotExternalSpec(cfg)
		require.NoError(t, err)
		assert.JSONEq(t, `{"extfs":{"cloud_provider":"tencent","access_key_id":"ak","access_key_value":"sk","region":"ap-guangzhou","use_ssl":"true"}}`, spec)
	})

	t.Run("IAM", func(t *testing.T) {
		cfg := storage.Config{
			Provider:   v2.ProviderAWS,
			Credential: storage.Credential{Type: storage.IAM, IAMEndpoint: "http://169.254.169.254"},
		}

		spec, err := snapshotExternalSpec(cfg)
		require.NoError(t, err)
		assert.JSONEq(t, `{"extfs":{"cloud_provider":"aws","iam_endpoint":"http://169.254.169.254","use_iam":"true","use_ssl":"false"}}`, spec)
	})

	// extfs has no session token field, so sending the key pair alone would fail to
	// authorize with nothing pointing at the cause.
	t.Run("RejectsSessionToken", func(t *testing.T) {
		cfg := storage.Config{Provider: v2.ProviderAWS, Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk", Token: "token"}}
		_, err := snapshotExternalSpec(cfg)
		assert.Error(t, err)
	})

	t.Run("RejectsUnsupportedCredential", func(t *testing.T) {
		cfg := storage.Config{Provider: v2.ProviderAWS, Credential: storage.Credential{Type: storage.GCPCredJSON}}
		_, err := snapshotExternalSpec(cfg)
		assert.Error(t, err)
	})
}

func TestNewSnapshotTarget(t *testing.T) {
	// Milvus falls back to its own credential when no spec is given, which is what to
	// rely on when both sides are the same backend — it keeps the key off the wire.
	t.Run("SameBackendSendsNoSpec", func(t *testing.T) {
		milvusCfg := storage.Config{
			Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}
		backupCfg := milvusCfg
		backupCfg.Bucket = "backup-bucket"

		target, err := newSnapshotTarget(milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "minio://minio:9000/backup-bucket/backup/mybackup/bundle", target.Path)
		assert.Equal(t, "bundle", target.Dir)
		assert.Empty(t, target.ExternalSpec)
	})

	t.Run("OtherBackendSendsSpec", func(t *testing.T) {
		milvusCfg := storage.Config{Provider: v2.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"}
		backupCfg := storage.Config{
			Provider: v2.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket",
			Credential: storage.Credential{Type: storage.Static, AK: "ak", SK: "sk"},
		}

		target, err := newSnapshotTarget(milvusCfg, backupCfg, "backup/mybackup")
		require.NoError(t, err)
		assert.Equal(t, "s3://backup-bucket/backup/mybackup/bundle", target.Path)
		assert.Contains(t, target.ExternalSpec, `"access_key_id":"ak"`)
	})
}

func TestSnapshotTarget_MetadataPath(t *testing.T) {
	target := snapshotTarget{Path: "s3://backup-bucket/backup/mybackup/bundle", Dir: "bundle"}

	// What lands in the meta is relative to the backup directory, bundle prefix and
	// all, so restore can rebuild the uri without knowing this layout.
	t.Run("RelativeToTheBackupDir", func(t *testing.T) {
		got, err := target.metadataPath("s3://backup-bucket/backup/mybackup/bundle/snapshots/449577/metadata/449580.json")
		require.NoError(t, err)
		assert.Equal(t, "bundle/snapshots/449577/metadata/449580.json", got)
	})

	// Anything outside the target is not ours to record, and a path relative to the
	// backup directory could not describe it anyway.
	t.Run("OutsideTarget", func(t *testing.T) {
		_, err := target.metadataPath("s3://other-bucket/snapshots/449577/metadata/449580.json")
		assert.Error(t, err)
	})
}
