package cfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaskSecret(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Empty", "", ""},
		{"Short1Char", "a", "****"},
		{"Short2Chars", "ab", "****"},
		{"Short3Chars", "abc", "****"},
		{"Short4Chars", "abcd", "****"},
		{"5Chars", "abcde", "ab****de"},
		{"6Chars", "abcdef", "ab****ef"},
		{"10Chars", "0123456789", "01****89"},
		{"Password", "minioadmin", "mi****in"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := maskSecret(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueDisplay(t *testing.T) {
	t.Run("NonSecretValue", func(t *testing.T) {
		v := Value[string]{Val: "localhost", Used: Used{Kind: SourceDefault}}
		entry := v.Display("Milvus.Address")

		assert.Equal(t, "Milvus.Address", entry.Name)
		assert.Equal(t, "localhost", entry.Value)
	})

	t.Run("SecretValueIsMasked", func(t *testing.T) {
		v := Value[string]{Val: "supersecret", Opts: SecretValue, Used: Used{Kind: SourceConfigFile, Key: "password"}}
		entry := v.Display("Milvus.Password")

		assert.NotEqual(t, "supersecret", entry.Value, "secret value should be masked")
		// fixed-length masking: "su" + "****" + "et"
		assert.Equal(t, "su****et", entry.Value)
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
