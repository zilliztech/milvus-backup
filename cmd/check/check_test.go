package check

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/cfg/loader"
)

func TestWriteConfig(t *testing.T) {
	c, err := loader.Load("", nil)
	assert.NoError(t, err)

	var buf bytes.Buffer
	assert.NoError(t, writeConfig(&buf, c))

	out := buf.String()
	assert.Contains(t, out, "Configuration:")
	assert.Contains(t, out, "PARAMETER")
	assert.Contains(t, out, "Milvus.Grpc.Address")
}
