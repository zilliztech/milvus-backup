package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// The loader's v1 compatibility path is pinned by goldens rather than by a
// second implementation: the retired pipeline (v1 schema resolve plus struct
// translate) produced these files from exactly the cases below, and the
// source-level translation must keep reproducing them value for value.
// Provenance is compared nowhere: the retired pipeline stamped every
// translated parameter as defaulted, and recording the v1 origin a value
// actually came from is the point of the translation — the migrate and param
// tests pin that instead.
//
// Regenerating the goldens requires the pre-translation tree: run this test
// with GOLDEN_WRITE=1 there, then copy testdata/golden back.
type goldenCase struct {
	Name      string            `yaml:"name"`
	Config    string            `yaml:"config"`
	Env       map[string]string `yaml:"env"`
	Overrides map[string]string `yaml:"overrides"`
}

const goldenCasesYAML = `cases:
  - name: empty-file
    config: ""

  - name: declared-v1
    config: |
      configVersion: v1
      milvus:
        address: milvus-proxy

  - name: declared-v2-runs-direct
    config: |
      configVersion: v2
      milvus:
        grpc:
          address: milvus-proxy

  - name: full-file
    config: |
      log:
        level: warn
        console: false
        file:
          filename: /var/log/backup.log
          maxSize: 100
          maxDays: 3
          maxBackups: 2
      http:
        debugMode: true
        swaggerBasePath: /backup
      cloud:
        address: https://api.example.com
        apiKey: cloud-key
      milvus:
        address: milvus-proxy
        port: 19531
        user: root
        password: milvus-pass
        tlsMode: 1
        caCertPath: /ca.pem
        serverName: milvus.local
        rpcChannelName: my-replicate
        etcd:
          endpoints: etcd-0:2379,etcd-1:2379
          rootPath: my-root
      minio:
        address: minio.local
        port: 9001
        region: us-west-2
        useSSL: true
        bucketName: milvus-bucket
        rootPath: milvus-root
        storageType: s3
        accessKeyID: ak
        secretAccessKey: sk
        token: tok
      backup:
        parallelism:
          copydata: 32
          backupCollection: 8
          backupSegment: 512
          restoreCollection: 3
          importJob: 64
        keepTempFiles: true
        gcPause:
          enable: false
          address: http://datacoord:9091

  - name: env-only-is-v2
    config: ""
    env:
      MILVUS_ADDRESS: from-v1-env

  - name: env-v1-names
    config: |
      milvus:
        port: 19531
    env:
      MILVUS_ADDRESS: from-v1-env
      MINIO_BUCKET_NAME: v1-bucket
      MINIO_REGION: v1-region

  - name: env-v1-secrets
    config: |
      minio:
        storageType: s3
        accessKeyID: ak
    env:
      MILVUS_PASSWORD: env-pass
      MINIO_SECRET_KEY: env-sk
      MINIO_TOKEN: env-tok
      MINIO_ACCESS_KEY: env-ak

  - name: env-endpoints
    config: |
      milvus:
        address: m
    env:
      MILVUS_ETCD_ENDPOINTS: " etcd-a:2379 , etcd-b:2379 ,"

  - name: override-v1-names
    config: |
      milvus:
        address: from-file
      minio:
        bucketName: file-bucket
    env:
      MILVUS_ADDRESS: from-env
    overrides:
      milvus.address: from-override
      minio.bucketname: override-bucket

  - name: override-v1-env-spelling
    config: |
      milvus:
        address: from-file
    env:
      MILVUS_ADDRESS: from-env
    overrides:
      MILVUS_ADDRESS: from-override

  - name: provider-alias-ali
    config: |
      minio:
        cloudProvider: ali
  - name: provider-alias-alibaba
    config: |
      minio:
        cloudProvider: alibaba
  - name: provider-alias-alicloud
    config: |
      minio:
        cloudProvider: alicloud
  - name: provider-alias-aliyun
    config: |
      minio:
        cloudProvider: aliyun
  - name: provider-alias-tc
    config: |
      minio:
        cloudProvider: tc
  - name: provider-alias-tencent
    config: |
      minio:
        cloudProvider: tencent

  - name: storage-key-declaration-order
    config: |
      storage:
        storageType: s3
      minio:
        storageType: azure
        cloudProvider: aws

  - name: azure-shared-key
    config: |
      minio:
        storageType: azure
        accessKeyID: myaccount
        secretAccessKey: mykey
        bucketName: mycontainer

  - name: azure-useiam
    config: |
      minio:
        storageType: azure
        useIAM: true
        accessKeyID: myaccount

  - name: gcpnative
    config: |
      minio:
        storageType: gcpnative
        gcpCredentialJSON: /creds.json
        backupGcpCredentialJSON: /backup-creds.json

  - name: iam
    config: |
      minio:
        storageType: aws
        useIAM: true
        iamEndpoint: http://iam.local

  - name: local-provider
    config: |
      minio:
        storageType: local

  - name: tls-disabled
    config: |
      milvus:
        tlsMode: 0
  - name: tls-server
    config: |
      milvus:
        tlsMode: 1
  - name: tls-mutual
    config: |
      milvus:
        tlsMode: 2
        mtlsCertPath: /c.pem
        mtlsKeyPath: /k.pem
  - name: tls-mutual-downgraded
    config: |
      milvus:
        tlsMode: 2
  - name: tls-invalid
    config: |
      milvus:
        tlsMode: 7

  - name: etcd-endpoints-comma-string
    config: |
      milvus:
        etcd:
          endpoints: etcd-a:2379, etcd-b:2379,

  - name: etcd-endpoints-override-comma
    config: |
      milvus:
        etcd:
          endpoints: etcd-file:2379
    overrides:
      milvus.etcd.endpoints: etcd-override-a:2379,etcd-override-b:2379

  - name: backup-inherits-milvus-side
    config: |
      minio:
        storageType: s3
        address: s3.amazonaws.com
        port: 443
        useSSL: true
        accessKeyID: ak
        secretAccessKey: sk
        backupBucketName: backup-bucket

  - name: backup-rootpath-inherits-custom
    config: |
      minio:
        rootPath: custom-root

  - name: backup-rootpath-explicit
    config: |
      minio:
        rootPath: custom-root
        backupRootPath: backup-root

  # v1's cmp.Or made an explicitly empty Milvus root path fall through to
  # "backup" for the backup side; the inheritance is settled, not carried.
  - name: backup-rootpath-milvus-empty
    config: |
      minio:
        rootPath: ""

  - name: backup-side-opts-out-of-iam
    config: |
      minio:
        storageType: aws
        useIAM: true
        backupStorageType: s3
        backupUseIAM: false
        backupAccessKeyID: bak-ak
        backupSecretAccessKey: bak-sk

  - name: backup-inherits-env-secret
    config: |
      minio:
        storageType: s3
        accessKeyID: ak
        backupBucketName: backups
    env:
      MINIO_SECRET_KEY: env-sk

  - name: crossstorage-true
    config: |
      minio:
        crossStorage: true

  - name: crossstorage-false-different-backends
    config: |
      minio:
        crossStorage: false
        address: milvus-minio
        backupAddress: backup-s3

  - name: http-disabled
    config: |
      http:
        enabled: false

  - name: minioadmin-defaults
    config: |
      minio:
        storageType: minio

  - name: unknown-keys-dropped
    config: |
      milvus:
        address: m
        notAKey: true
      totallyUnknown: 1

  - name: multipart-threshold
    config: |
      minio:
        multipartCopyThresholdMiB: 64
      backup:
        parallelism:
          copydata: 16
`

func TestLoadV1Golden(t *testing.T) {
	var cases struct {
		Cases []goldenCase `yaml:"cases"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(goldenCasesYAML), &cases))

	for _, c := range cases.Cases {
		t.Run(c.Name, func(t *testing.T) {
			for k, v := range c.Env {
				t.Setenv(k, v)
			}

			p := filepath.Join(t.TempDir(), "backup.yaml")
			require.NoError(t, os.WriteFile(p, []byte(c.Config), 0o600))

			out, err := Load(p, c.Overrides)
			require.NoError(t, err)

			var b strings.Builder
			for _, e := range param.Entries(out) {
				fmt.Fprintf(&b, "%s\t%s\n", e.Name, e.Value)
			}

			golden := filepath.Join("testdata", "golden", c.Name+".txt")
			if os.Getenv("GOLDEN_WRITE") == "1" {
				require.NoError(t, os.MkdirAll(filepath.Dir(golden), 0o755))
				require.NoError(t, os.WriteFile(golden, []byte(b.String()), 0o644))
				return
			}

			want, err := os.ReadFile(golden)
			require.NoError(t, err)
			assert.Equal(t, string(want), b.String())
		})
	}
}
