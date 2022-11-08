package core

import (
	"context"
	"net/http"
	"net/http/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func TestBackupService(t *testing.T) {
	var params paramtable.BackupParams
	milvusYamlFile := "backup.yaml"
	params.GlobalInitWithYaml(milvusYamlFile)
	params.Init()

	context := context.Background()
	server, err := NewServer(context, params)
	assert.NoError(t, err)
	server.Init()
	server.Start()
	time.Sleep(1000 * time.Second)

}

func TestProfileService(t *testing.T) {
	go func() {
		http.HandleFunc("/debug/pprof/heap", pprof.Index)
		http.ListenAndServe("localhost:8089", nil)
	}()
	time.Sleep(1000 * time.Second)

}
