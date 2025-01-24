package client

import (
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func TestGrpcAuth(t *testing.T) {
	got := grpcAuth("username", "password")
	assert.Equal(t, "dXNlcm5hbWU6cGFzc3dvcmQ=", got)

	got = grpcAuth("", "")
	assert.Equal(t, "", got)
}

func TestTransCred(t *testing.T) {
	cred, err := transCred(&paramtable.MilvusConfig{TLSMode: 3})
	assert.Error(t, err)
	assert.Nil(t, cred)

	cred, err = transCred(&paramtable.MilvusConfig{TLSMode: 0})
	assert.NoError(t, err)
	assert.Equal(t, insecure.NewCredentials(), cred)

	cred, err = transCred(&paramtable.MilvusConfig{TLSMode: 1})
	assert.NoError(t, err)

	cred, err = transCred(&paramtable.MilvusConfig{TLSMode: 2})
	assert.NoError(t, err)
}

func TestStatusOk(t *testing.T) {
	assert.True(t, statusOk(&commonpb.Status{Code: 0}))

	assert.False(t, statusOk(&commonpb.Status{Code: 1}))
}

func TestCheckResponse(t *testing.T) {
	// err is not nil
	assert.Nil(t, checkResponse(&commonpb.Status{Code: 0}, nil))
	assert.Error(t, checkResponse(&commonpb.Status{Code: 0}, errors.New("some error")))

	// status is not ok
	assert.Error(t, checkResponse(&commonpb.Status{Code: 1}, nil))
	assert.Error(t, checkResponse(&milvuspb.ShowCollectionsResponse{Status: &commonpb.Status{Code: 1}}, nil))

	// status is ok
	assert.Nil(t, checkResponse(&commonpb.Status{Code: 0}, nil))
	assert.Nil(t, checkResponse(&milvuspb.ShowCollectionsResponse{Status: &commonpb.Status{Code: 0}}, nil))
}
