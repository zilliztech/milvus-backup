package check

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func TestPrintConfig(t *testing.T) {
	c, err := cfg.Load("", nil)
	assert.NoError(t, err)

	var buf bytes.Buffer
	printConfig(&buf, c)

	output := buf.String()

	assert.Contains(t, output, "PARAMETER")
	assert.Contains(t, output, "VALUE")
	assert.Contains(t, output, "SOURCE")
	assert.Contains(t, output, "SOURCE_KEY")

	assert.Contains(t, output, "Log.Level")
	assert.Contains(t, output, "Milvus.Address")
	assert.Contains(t, output, "SourceDefault")
}

func TestPrintConfigWithOverride(t *testing.T) {
	c, err := cfg.Load("", map[string]string{
		"milvus.address": "192.168.1.100",
	})
	assert.NoError(t, err)

	var buf bytes.Buffer
	printConfig(&buf, c)

	output := buf.String()

	assert.Contains(t, output, "192.168.1.100")
	assert.Contains(t, output, "SourceOverride")
}

func TestSecretMasking(t *testing.T) {
	c, err := cfg.Load("", map[string]string{
		"milvus.password": "supersecret123",
	})
	assert.NoError(t, err)

	var buf bytes.Buffer
	printConfig(&buf, c)

	output := buf.String()

	// Password should be masked with fixed-length, not exposing actual length
	assert.NotContains(t, output, "supersecret123")
	assert.Contains(t, output, "su****23")
}
