package param

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newList() *List {
	return &List{Default: []string{"localhost:2379"}, Keys: []string{"milvus.etcd.endpoints"}, EnvKeys: []string{"MILVUS_ETCD_ENDPOINTS"}}
}

func sourceFromYAML(t *testing.T, content string) *Source {
	t.Helper()

	p := filepath.Join(t.TempDir(), "backup.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))

	src, err := NewSource(p, nil)
	require.NoError(t, err)

	return src
}

func TestListResolve(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		l := newList()
		require.NoError(t, l.Resolve(&Source{}))

		assert.Equal(t, []string{"localhost:2379"}, l.Val)
		assert.Equal(t, SourceDefault, l.Used.Kind)
	})

	t.Run("ConfigFileList", func(t *testing.T) {
		src := sourceFromYAML(t, "milvus:\n  etcd:\n    endpoints:\n      - a:2379\n      - b:2379\n")

		l := newList()
		require.NoError(t, l.Resolve(src))

		assert.Equal(t, []string{"a:2379", "b:2379"}, l.Val)
		assert.Equal(t, SourceConfigFile, l.Used.Kind)
	})

	t.Run("ConfigFileScalarIsRejected", func(t *testing.T) {
		src := sourceFromYAML(t, "milvus:\n  etcd:\n    endpoints: a:2379,b:2379\n")

		err := newList().Resolve(src)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be a YAML list")
	})

	t.Run("EnvIsCommaSeparated", func(t *testing.T) {
		t.Setenv("MILVUS_ETCD_ENDPOINTS", " a:2379 , b:2379 ,")

		l := newList()
		require.NoError(t, l.Resolve(&Source{}))

		assert.Equal(t, []string{"a:2379", "b:2379"}, l.Val)
		assert.Equal(t, SourceEnv, l.Used.Kind)
	})

	t.Run("OverrideWins", func(t *testing.T) {
		t.Setenv("MILVUS_ETCD_ENDPOINTS", "a:2379")
		src, err := NewSource("", map[string]string{"milvus.etcd.endpoints": "b:2379"})
		require.NoError(t, err)

		l := newList()
		require.NoError(t, l.Resolve(src))

		assert.Equal(t, []string{"b:2379"}, l.Val)
		assert.Equal(t, SourceOverride, l.Used.Kind)
	})
}

func TestListDisplay(t *testing.T) {
	l := newList()
	require.NoError(t, l.Resolve(&Source{}))

	entry := l.Display("Milvus.Etcd.Endpoints")
	assert.Equal(t, "Milvus.Etcd.Endpoints", entry.Name)
	assert.Equal(t, "localhost:2379", entry.Value)
}
