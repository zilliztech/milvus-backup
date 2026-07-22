package cfg

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigWriteTable(t *testing.T) {
	t.Run("Header", func(t *testing.T) {
		c, err := Load("", nil)
		assert.NoError(t, err)

		var buf bytes.Buffer
		assert.NoError(t, c.WriteTable(&buf))

		output := buf.String()
		assert.Contains(t, output, "PARAMETER")
		assert.Contains(t, output, "VALUE")
		assert.Contains(t, output, "SOURCE")
		assert.Contains(t, output, "SOURCE_KEY")
		assert.Contains(t, output, "Log.Level")
		assert.Contains(t, output, "Milvus.Address")
		assert.Contains(t, output, "default")
	})

	t.Run("Override", func(t *testing.T) {
		c, err := Load("", map[string]string{"milvus.address": "192.168.1.100"})
		assert.NoError(t, err)

		var buf bytes.Buffer
		assert.NoError(t, c.WriteTable(&buf))

		output := buf.String()
		assert.Contains(t, output, "192.168.1.100")
		assert.Contains(t, output, "override")
	})

	t.Run("SecretIsMasked", func(t *testing.T) {
		c, err := Load("", map[string]string{"milvus.password": "supersecret123"})
		assert.NoError(t, err)

		var buf bytes.Buffer
		assert.NoError(t, c.WriteTable(&buf))

		output := buf.String()
		// Password should be masked with a fixed-length mask, not exposing actual length.
		assert.NotContains(t, output, "supersecret123")
		assert.Contains(t, output, "su****23")
	})
}

func TestConfigEntries(t *testing.T) {
	c, err := Load("", nil)
	assert.NoError(t, err)

	entries := c.Entries()
	assert.NotEmpty(t, entries)

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
	}

	expectedFields := []string{
		"Log.Level",
		"Log.Console",
		"Milvus.Address",
		"Milvus.Port",
		"Minio.BucketName",
		"Backup.KeepTempFiles",
	}

	for _, field := range expectedFields {
		assert.True(t, names[field], "expected field %s not found", field)
	}
}
