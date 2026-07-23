package v2

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// values maps each parameter to its displayed value, ignoring the source. A
// round trip changes the source (an emitted default reloads as coming from the
// config file), but the value must be preserved.
func values(c *Config) map[string]string {
	m := make(map[string]string)
	for _, e := range c.Entries() {
		m[e.Name] = e.Value
	}

	return m
}

// roundTrip renders c, loads the result back, and returns the reloaded config.
func roundTrip(t *testing.T, c *Config) *Config {
	t.Helper()

	data, err := Render(c, nil)
	require.NoError(t, err)

	p := filepath.Join(t.TempDir(), "rendered.yaml")
	require.NoError(t, os.WriteFile(p, data, 0o600))

	out, err := Load(p, nil)
	require.NoError(t, err)

	return out
}

// Rendering a resolved config and loading the result back must reproduce every
// value: Render is the inverse of Load over resolved values.
func TestRender_RoundTrip(t *testing.T) {
	in, err := Load(filepath.Join("testdata", "complete.yaml"), nil)
	require.NoError(t, err)

	data, err := Render(in, nil)
	require.NoError(t, err)
	assert.Contains(t, string(data), "configVersion: v2")

	p := filepath.Join(t.TempDir(), "rendered.yaml")
	require.NoError(t, os.WriteFile(p, data, 0o600))
	out, err := Load(p, nil)
	require.NoError(t, err)

	assert.Equal(t, values(in), values(out))
}

// Defaults round-trip too: an env-only config renders and loads back to the
// same values.
func TestRender_Defaults(t *testing.T) {
	in, err := Load("", nil)
	require.NoError(t, err)

	assert.Equal(t, values(in), values(roundTrip(t, in)))
}

func TestRender_Comment(t *testing.T) {
	c, err := Load("", nil)
	require.NoError(t, err)

	data, err := Render(c, map[string]string{"transfer.mode": "note about the mode"})
	require.NoError(t, err)

	assert.Contains(t, string(data), "# note about the mode")
}

// The etcd endpoints render as a YAML list, not the v1 comma-separated string.
func TestRender_EtcdList(t *testing.T) {
	c, err := Load(filepath.Join("testdata", "complete.yaml"), nil)
	require.NoError(t, err)

	data, err := Render(c, nil)
	require.NoError(t, err)

	assert.Contains(t, string(data), "- etcd-0:2379")
	assert.Contains(t, string(data), "- etcd-1:2379")
}
