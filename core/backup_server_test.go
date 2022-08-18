package core

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"net/http"
	"net/http/pprof"
	"testing"
	"time"
)

func TestBackupService(t *testing.T) {
	var params paramtable.ComponentParam
	milvusYamlFile := "milvus.yaml"
	params.GlobalInitWithYaml(milvusYamlFile)
	params.InitOnce()

	context := context.Background()
	server, err := NewServer(context, params)
	assert.NoError(t, err)
	server.Init()
	server.Start()

}

func TestProfileService(t *testing.T) {
	//var params paramtable.ComponentParam
	//milvusYamlFile := "milvus.yaml"
	//params.GlobalInitWithYaml(milvusYamlFile)
	//params.InitOnce()
	//
	//context := context.Background()
	//server, err := NewServer(context, params)
	//assert.NoError(t, err)
	//server.Init()
	//server.registerProfilePort()
	go func() {
		http.HandleFunc("/debug/pprof/heap", pprof.Index)
		http.ListenAndServe("localhost:8089", nil)
	}()
	time.Sleep(1000 * time.Second)

}
