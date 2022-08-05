package core

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"testing"
)

func TestBackupService(t *testing.T) {
	var params paramtable.ComponentParam
	milvusYamlFile := "milvus.yaml"
	params.GlobalInitWithYaml(milvusYamlFile)
	params.InitOnce()

	context := context.Background()
	server, err := NewServer(context, params)
	assert.NoError(t, err)
	server.registerHTTPServer()

}
