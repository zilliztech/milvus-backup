package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/cmd/root"
)

// runMigrate wires the migrate command against configPath and captures stdout
// and stderr separately.
func runMigrate(t *testing.T, configPath string, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	opt := &root.Options{Config: configPath}
	cmd := newMigrateCmd(opt)

	var out, errBuf bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errBuf)
	cmd.SetArgs(args)

	err = cmd.Execute()

	return out.String(), errBuf.String(), err
}

func writeV1(t *testing.T, content string) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "backup.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))

	return p
}

// stdout is a clean v2 document: the report and diagnostics stay on stderr.
func TestMigrate_StdoutIsCleanDocument(t *testing.T) {
	stdout, _, err := runMigrate(t, writeV1(t, "milvus:\n  address: milvus-proxy\n"))
	require.NoError(t, err)

	assert.True(t, len(stdout) > 0)
	assert.Equal(t, "configVersion: v2", stdout[:len("configVersion: v2")])
	assert.Contains(t, stdout, "address: milvus-proxy")
}

func TestMigrate_Output(t *testing.T) {
	src := writeV1(t, "milvus:\n  address: milvus-proxy\n")
	out := filepath.Join(t.TempDir(), "v2.yaml")

	t.Run("WritesFile", func(t *testing.T) {
		stdout, stderr, err := runMigrate(t, src, "--output", out)
		require.NoError(t, err)

		assert.Empty(t, stdout)
		assert.Contains(t, stderr, "wrote v2 config to")

		data, err := os.ReadFile(out)
		require.NoError(t, err)
		assert.Contains(t, string(data), "configVersion: v2")
	})

	t.Run("RefusesToOverwriteWithoutForce", func(t *testing.T) {
		_, _, err := runMigrate(t, src, "--output", out)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("ForceOverwrites", func(t *testing.T) {
		_, _, err := runMigrate(t, src, "--output", out, "--force")
		require.NoError(t, err)
	})
}

// Migrating a file that is already v2 is a mistake, not a no-op reload.
func TestMigrate_RejectsV2Input(t *testing.T) {
	src := writeV1(t, "configVersion: v2\nmilvus:\n  grpc:\n    address: milvus-proxy\n")

	_, _, err := runMigrate(t, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already a v2 config")
}

// --strict turns a config that cannot be expressed as valid v2 into a failure
// and writes nothing.
func TestMigrate_Strict(t *testing.T) {
	// Azure with explicitly empty credentials (as the shipped azure sample has)
	// resolves to an invalid v2 config: no account name or key.
	src := writeV1(t, "minio:\n  storageType: azure\n  accessKeyID: \"\"\n  secretAccessKey: \"\"\n")
	out := filepath.Join(t.TempDir(), "v2.yaml")

	_, _, err := runMigrate(t, src, "--strict", "--output", out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--strict")

	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr), "no file should be written on a strict failure")
}
