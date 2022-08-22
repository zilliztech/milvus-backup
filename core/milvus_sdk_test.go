package core

import (
	"context"
	gomilvus "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"github.com/zilliztech/milvus-backup/internal/util/typeutil"
	"testing"
)

func TestProxyClient(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	ctx := context.Background()
	var Params paramtable.GrpcServerConfig
	Params.InitOnce(typeutil.ProxyRole)
	milvusAddr := Params.GetAddress()
	//c, err := proxy.NewClient(context, milvusAddr)
	//assert.NoError(t, err)

	c2, err := gomilvus.NewGrpcClient(ctx, milvusAddr)
	assert.NoError(t, err)
	collections, err := c2.ListCollections(ctx)
	assert.NotEmpty(t, collections)
}
