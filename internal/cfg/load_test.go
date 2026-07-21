package cfg

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "cfg_test.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return p
}

func TestLoad_Precedence(t *testing.T) {
	p := writeTempYAML(t, `
milvus:
  port: 12345
`)

	t.Run("config_file", func(t *testing.T) {
		c, err := Load(p, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Milvus.Port.Val; got != 12345 {
			t.Fatalf("milvus.port = %d, want %d", got, 12345)
		}
	})

	t.Run("env_overrides_config_file", func(t *testing.T) {
		t.Setenv("MILVUS_PORT", "23456")
		c, err := Load(p, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Milvus.Port.Val; got != 23456 {
			t.Fatalf("milvus.port = %d, want %d", got, 23456)
		}
	})

	t.Run("override_overrides_env", func(t *testing.T) {
		t.Setenv("MILVUS_PORT", "23456")
		c, err := Load(p, map[string]string{"MILVUS_PORT": "34567"})
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Milvus.Port.Val; got != 34567 {
			t.Fatalf("milvus.port = %d, want %d", got, 34567)
		}
	})

	t.Run("override_dot_key_works", func(t *testing.T) {
		c, err := Load(p, map[string]string{"milvus.port": "45678"})
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Milvus.Port.Val; got != 45678 {
			t.Fatalf("milvus.port = %d, want %d", got, 45678)
		}
	})
}

func TestLoad_LegacyConcurrencyKeys(t *testing.T) {
	p := writeTempYAML(t, `
backup:
  parallelism:
    copydata: 99
    backupCollection: 7
    backupSegment: 55
    restoreCollection: 6
    importJob: 44
  keepTempFiles: true
`)

	c, err := Load(p, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.Transfer.Concurrency.Val; got != 99 {
		t.Fatalf("transfer.concurrency = %d, want %d", got, 99)
	}
	if got := c.Backup.Concurrency.Collections.Val; got != 7 {
		t.Fatalf("backup.concurrency.collections = %d, want %d", got, 7)
	}
	if got := c.Backup.Concurrency.Segments.Val; got != 55 {
		t.Fatalf("backup.concurrency.segments = %d, want %d", got, 55)
	}
	if got := c.Restore.Concurrency.Collections.Val; got != 6 {
		t.Fatalf("restore.concurrency.collections = %d, want %d", got, 6)
	}
	if got := c.Restore.Concurrency.ImportJobs.Val; got != 44 {
		t.Fatalf("restore.concurrency.importJobs = %d, want %d", got, 44)
	}
	if !c.Restore.KeepTempFiles.Val {
		t.Fatal("restore.keepTempFiles did not load legacy backup.keepTempFiles")
	}
}

func TestLoad_ConcurrencyStructure(t *testing.T) {
	p := writeTempYAML(t, `
transfer:
  concurrency: 32
backup:
  concurrency:
    collections: 3
    segments: 64
restore:
  concurrency:
    collections: 5
    importJobs: 20
`)

	c, err := Load(p, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.Transfer.Concurrency.Val; got != 32 {
		t.Fatalf("transfer.concurrency = %d", got)
	}
	if got := c.Backup.Concurrency.Collections.Val; got != 3 {
		t.Fatalf("backup.concurrency.collections = %d", got)
	}
	if got := c.Backup.Concurrency.Segments.Val; got != 64 {
		t.Fatalf("backup.concurrency.segments = %d", got)
	}
	if got := c.Restore.Concurrency.Collections.Val; got != 5 {
		t.Fatalf("restore.concurrency.collections = %d", got)
	}
	if got := c.Restore.Concurrency.ImportJobs.Val; got != 20 {
		t.Fatalf("restore.concurrency.importJobs = %d", got)
	}
}

func TestLoad_RejectsNonPositiveConcurrency(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{name: "transfer", yaml: "transfer:\n  concurrency: 0\n"},
		{name: "backup collections", yaml: "backup:\n  concurrency:\n    collections: 0\n"},
		{name: "restore import jobs", yaml: "restore:\n  concurrency:\n    importJobs: -1\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := writeTempYAML(t, tt.yaml)
			if _, err := Load(p, nil); err == nil {
				t.Fatal("expected concurrency validation error")
			}
		})
	}
}

func TestLoad_StorageConfig(t *testing.T) {
	t.Run("canonical_structure", func(t *testing.T) {
		p := writeTempYAML(t, `
milvus:
  storage:
    provider: minio
    address: source.example.com
    port: 9000
    bucketName: source-bucket
    rootPath: files
backup:
  storage:
    provider: s3
    address: dest.example.com
    port: 443
    bucketName: backup-bucket
    rootPath: backups
transfer:
  mode: streaming
`)

		c, err := Load(p, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Milvus.Storage.Address.Val; got != "source.example.com" {
			t.Fatalf("milvus.storage.address = %q", got)
		}
		if got := c.Backup.Storage.Provider.Val; got != S3 {
			t.Fatalf("backup.storage.provider = %q", got)
		}
		if got := c.Transfer.Mode.Val; got != TransferModeStreaming {
			t.Fatalf("transfer.mode = %q", got)
		}
	})

	t.Run("legacy_structure", func(t *testing.T) {
		p := writeTempYAML(t, `
minio:
  storageType: minio
  address: source.example.com
  bucketName: source-bucket
  backupStorageType: s3
  backupAddress: dest.example.com
  backupBucketName: backup-bucket
  crossStorage: true
`)

		c, err := Load(p, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Milvus.Storage.Address.Val; got != "source.example.com" {
			t.Fatalf("milvus.storage.address = %q", got)
		}
		if got := c.Backup.Storage.Provider.Val; got != S3 {
			t.Fatalf("backup.storage.provider = %q", got)
		}
		if got := c.Transfer.Mode.Val; got != TransferModeStreaming {
			t.Fatalf("legacy crossStorage mapped to %q", got)
		}
		if got := c.Transfer.Mode.Used.Key; got != "minio.crossstorage" {
			t.Fatalf("transfer mode source key = %q", got)
		}
	})

	t.Run("canonical_keys_win", func(t *testing.T) {
		p := writeTempYAML(t, `
milvus:
  storage:
    address: canonical.example.com
minio:
  address: legacy.example.com
backup:
  transfer:
    mode: direct
`)

		c, err := Load(p, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Milvus.Storage.Address.Val; got != "canonical.example.com" {
			t.Fatalf("milvus.storage.address = %q", got)
		}
		if got := c.Milvus.Storage.Address.Used.Key; got != "milvus.storage.address" {
			t.Fatalf("storage address source key = %q", got)
		}
	})

	t.Run("backup_storage_inherits_milvus_storage", func(t *testing.T) {
		p := writeTempYAML(t, `
milvus:
  storage:
    provider: gcpnative
    address: storage.example.com
    useSSL: true
    useIAM: true
    gcpCredentialJSON: credentials.json
`)

		c, err := Load(p, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Backup.Storage.Provider.Val; got != CloudProviderGCPNative {
			t.Fatalf("backup.storage.provider = %q", got)
		}
		if !c.Backup.Storage.UseSSL.Val || !c.Backup.Storage.UseIAM.Val {
			t.Fatal("backup storage did not inherit boolean settings")
		}
		if got := c.Backup.Storage.GcpCredentialJSON.Val; got != "credentials.json" {
			t.Fatalf("backup GCP credential = %q", got)
		}
	})
}

func TestLoad_TransferMode(t *testing.T) {
	t.Run("default_auto", func(t *testing.T) {
		c, err := Load("", nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Transfer.Mode.Val; got != TransferModeAuto {
			t.Fatalf("transfer.mode = %q", got)
		}
	})

	t.Run("legacy_false_maps_to_auto", func(t *testing.T) {
		p := writeTempYAML(t, "minio:\n  crossStorage: false\n")
		c, err := Load(p, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := c.Transfer.Mode.Val; got != TransferModeAuto {
			t.Fatalf("transfer.mode = %q", got)
		}
	})

	t.Run("reject_invalid_mode", func(t *testing.T) {
		p := writeTempYAML(t, "transfer:\n  mode: magic\n")
		_, err := Load(p, nil)
		if err == nil {
			t.Fatal("expected invalid transfer mode error")
		}
	})
}
