package param

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceKindStrings(t *testing.T) {
	assert.Equal(t, "v1 env", SourceV1Env.String())
	assert.Equal(t, "v1 config", SourceV1ConfigFile.String())
}

// The environment is snapshotted when the source is built, so a variable set
// afterwards is invisible to resolution: a source resolves the same way every
// time it is consulted.
func TestSourceEnvSnapshot(t *testing.T) {
	t.Setenv("SNAPSHOT_TEST_VAR", "at-construction")

	p := filepath.Join(t.TempDir(), "backup.yaml")
	require.NoError(t, os.WriteFile(p, []byte("log:\n  level: info\n"), 0o600))

	src, err := NewSource(p, nil)
	require.NoError(t, err)

	require.NoError(t, os.Setenv("SNAPSHOT_TEST_VAR", "after-construction"))
	t.Cleanup(func() { require.NoError(t, os.Unsetenv("SNAPSHOT_TEST_VAR")) })

	v, ok := src.EnvValue("SNAPSHOT_TEST_VAR")
	assert.True(t, ok)
	assert.Equal(t, "at-construction", v)
}

func TestNewTranslatedSource(t *testing.T) {
	t.Run("OverrideIsMatchedCaseInsensitively", func(t *testing.T) {
		src := NewTranslatedSource("",
			map[string]Input{},
			map[string]string{"MILVUS.GRPC.PORT": "19531"})

		v, ok := src.OverrideValue("milvus.grpc.port")
		assert.True(t, ok)
		assert.Equal(t, "19531", v)
		assert.Equal(t, []string{"MILVUS.GRPC.PORT"}, src.OverrideKeys())
	})

	t.Run("FileEntriesKeepProvenance", func(t *testing.T) {
		src := NewTranslatedSource("/some/backup.yaml",
			map[string]Input{
				"milvus.grpc.address": {Value: "from-v1-file", Kind: SourceV1ConfigFile, SourceKey: "milvus.address"},
				"log.level":           {Value: "debug", Kind: SourceV1Env, SourceKey: "LOG_LEVEL"},
			},
			map[string]string{})

		raw, ok := src.ConfigFileValue("milvus.grpc.address")
		assert.True(t, ok)
		assert.Equal(t, "from-v1-file", raw)

		v := Value[string]{Keys: []string{"milvus.grpc.address"}}
		require.NoError(t, v.Resolve(src))
		assert.Equal(t, "from-v1-file", v.Val)
		assert.Equal(t, SourceV1ConfigFile, v.Used.Kind)
		assert.Equal(t, "milvus.address", v.Used.Key)

		lvl := Value[string]{Default: "info", Keys: []string{"log.level"}}
		require.NoError(t, lvl.Resolve(src))
		assert.Equal(t, SourceV1Env, lvl.Used.Kind)
		assert.Equal(t, "LOG_LEVEL", lvl.Used.Key)
	})

	t.Run("EnvIsAlwaysEmpty", func(t *testing.T) {
		t.Setenv("MILVUS_ADDRESS", "from-process")

		src := NewTranslatedSource("",
			map[string]Input{"milvus.grpc.address": {Value: "from-v1", Kind: SourceV1ConfigFile, SourceKey: "milvus.address"}},
			map[string]string{})

		_, ok := src.EnvValue("MILVUS_ADDRESS")
		assert.False(t, ok, "a translated source carries no environment: the translation already carried its values over")
	})
}
