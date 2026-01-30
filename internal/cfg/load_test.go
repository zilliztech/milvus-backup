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

func TestLoad_FlattenNestedKeys(t *testing.T) {
	p := writeTempYAML(t, `
backup:
  parallelism:
    copydata: 99
    backupCollection: 7
`)

	c, err := Load(p, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.Backup.Parallelism.CopyData.Val; got != 99 {
		t.Fatalf("backup.parallelism.copydata = %d, want %d", got, 99)
	}
	if got := c.Backup.Parallelism.BackupCollection.Val; got != 7 {
		t.Fatalf("backup.parallelism.backupCollection = %d, want %d", got, 7)
	}
}
