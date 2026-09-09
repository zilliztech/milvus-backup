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

// WithOverrides is the merge Fork builds on, so it has to be exact: a repeated
// key takes the new value under the new spelling, the receiver keeps its own
// layer untouched, and the layers not being merged pass through.
func TestWithOverrides(t *testing.T) {
	t.Run("MergeLaterWins", func(t *testing.T) {
		src, err := NewSource("", map[string]string{"milvus.grpc.port": "19531", "log.level": "info"})
		require.NoError(t, err)

		out := src.WithOverrides(map[string]string{"MILVUS.GRPC.PORT": "19532"})

		v, ok := out.OverrideValue("milvus.grpc.port")
		assert.True(t, ok)
		assert.Equal(t, "19532", v)
		// The latest spelling is the one errors quote back.
		assert.Equal(t, []string{"MILVUS.GRPC.PORT", "log.level"}, out.OverrideKeys())
	})

	t.Run("ReceiverIsNeverMutated", func(t *testing.T) {
		src, err := NewSource("", map[string]string{"milvus.grpc.port": "19531"})
		require.NoError(t, err)

		src.WithOverrides(map[string]string{"milvus.grpc.port": "19532", "log.level": "debug"})

		v, ok := src.OverrideValue("milvus.grpc.port")
		assert.True(t, ok)
		assert.Equal(t, "19531", v)
		_, ok = src.OverrideValue("log.level")
		assert.False(t, ok)
		assert.Equal(t, []string{"milvus.grpc.port"}, src.OverrideKeys())
	})

	t.Run("NilIsAPlainCopy", func(t *testing.T) {
		src, err := NewSource("", map[string]string{"milvus.grpc.port": "19531"})
		require.NoError(t, err)

		out := src.WithOverrides(nil)

		assert.Equal(t, src.OverrideKeys(), out.OverrideKeys())
		v, ok := out.OverrideValue("milvus.grpc.port")
		assert.True(t, ok)
		assert.Equal(t, "19531", v)
	})

	t.Run("FileAndEnvLayersPassThrough", func(t *testing.T) {
		t.Setenv("WITHOVERRIDES_ENV", "from-env")

		p := filepath.Join(t.TempDir(), "backup.yaml")
		require.NoError(t, os.WriteFile(p, []byte("log:\n  level: debug\n"), 0o600))

		src, err := NewSource(p, nil)
		require.NoError(t, err)

		out := src.WithOverrides(map[string]string{"log.console": "false"})

		assert.Equal(t, p, out.ConfigFilePath())
		raw, ok := out.ConfigFileValue("log.level")
		assert.True(t, ok)
		assert.Equal(t, "debug", raw)
		env, ok := out.EnvValue("WITHOVERRIDES_ENV")
		assert.True(t, ok)
		assert.Equal(t, "from-env", env)
	})
}
