package milvus

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func TestGrpcAuth(t *testing.T) {
	got := grpcAuth("username", "password")
	assert.Equal(t, "dXNlcm5hbWU6cGFzc3dvcmQ=", got)

	got = grpcAuth("", "")
	assert.Equal(t, "", got)
}

func TestTransCred(t *testing.T) {
	conf := &cfg.MilvusConfig{}
	conf.TLSMode.Set(3)
	cred, err := transCred(conf)
	assert.Error(t, err)
	assert.Nil(t, cred)

	conf.TLSMode.Set(0)
	cred, err = transCred(conf)
	assert.NoError(t, err)
	assert.Equal(t, insecure.NewCredentials(), cred)

	conf.TLSMode.Set(1)
	cred, err = transCred(conf)
	assert.NoError(t, err)
	assert.NotNil(t, cred)

	conf.TLSMode.Set(2)
	cred, err = transCred(conf)
	assert.NoError(t, err)
	assert.NotNil(t, cred)
}

func TestIsUnimplemented(t *testing.T) {
	assert.False(t, isUnimplemented(nil))
	assert.False(t, isUnimplemented(errors.New("some error")))
	assert.True(t, isUnimplemented(status.Error(codes.Unimplemented, "some error")))
}

func TestStatusOk(t *testing.T) {
	// Both Code and ErrorCode are 0 (ErrorCode defaults to 0)
	assert.True(t, statusOk(&commonpb.Status{Code: 0}))

	// Code is 0 but ErrorCode is not 0
	assert.False(t, statusOk(&commonpb.Status{Code: 0, ErrorCode: commonpb.ErrorCode_UnexpectedError}))

	// Code is not 0 but ErrorCode is 0
	assert.False(t, statusOk(&commonpb.Status{Code: 1, ErrorCode: commonpb.ErrorCode_Success}))

	// Both Code and ErrorCode are not 0
	assert.False(t, statusOk(&commonpb.Status{Code: 1, ErrorCode: commonpb.ErrorCode_UnexpectedError}))
}

func TestCheckResponse(t *testing.T) {
	// err is not nil
	assert.Nil(t, checkResponse(&commonpb.Status{Code: 0}, nil))
	assert.Error(t, checkResponse(&commonpb.Status{Code: 0}, errors.New("some error")))

	// status is not ok - Code is not 0
	assert.Error(t, checkResponse(&commonpb.Status{Code: 1}, nil))
	assert.Error(t, checkResponse(&milvuspb.ShowCollectionsResponse{Status: &commonpb.Status{Code: 1}}, nil))

	// status is not ok - ErrorCode is not 0 (legacy check)
	assert.Error(t, checkResponse(&commonpb.Status{Code: 0, ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil))
	assert.Error(t, checkResponse(&milvuspb.ShowCollectionsResponse{Status: &commonpb.Status{Code: 0, ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil))

	// status is ok
	assert.Nil(t, checkResponse(&commonpb.Status{Code: 0}, nil))
	assert.Nil(t, checkResponse(&milvuspb.ShowCollectionsResponse{Status: &commonpb.Status{Code: 0}}, nil))
}

func TestIsRateLimitError(t *testing.T) {
	assert.False(t, isRateLimitError(errors.New("some error")))
	assert.False(t, isRateLimitError(nil))
	assert.False(t, isRateLimitError(errors.New("rate limit")))
	assert.True(t, isRateLimitError(errors.New("rate limit exceeded[rate=1]")))
}

func TestGrpcClient_newCtx(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		cli := &GrpcClient{auth: "auth", identifier: "identifier"}
		ctx := cli.newCtx(context.Background())
		md, ok := metadata.FromOutgoingContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "auth", md.Get(_authorizationHeader)[0])
		assert.Len(t, md.Get(_authorizationHeader), 1)
		assert.Equal(t, "identifier", md.Get(_identifierHeader)[0])
		assert.Len(t, md.Get(_identifierHeader), 1)
	})

	t.Run("SetMultipleTimes", func(t *testing.T) {
		cli := &GrpcClient{auth: "auth", identifier: "identifier"}
		ctx := cli.newCtx(context.Background())
		ctx = cli.newCtx(ctx)
		md, ok := metadata.FromOutgoingContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "auth", md.Get(_authorizationHeader)[0])
		assert.Len(t, md.Get(_authorizationHeader), 1)
		assert.Equal(t, "identifier", md.Get(_identifierHeader)[0])
		assert.Len(t, md.Get(_identifierHeader), 1)
	})

}

func TestGrpcClient_newCtxWithDB(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		cli := &GrpcClient{}
		ctx := cli.newCtxWithDB(context.Background(), "db")
		md, ok := metadata.FromOutgoingContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "db", md.Get(_databaseHeader)[0])
	})

	t.Run("SetMultipleTimes", func(t *testing.T) {
		cli := &GrpcClient{}
		ctx := cli.newCtxWithDB(context.Background(), "db")
		ctx = cli.newCtxWithDB(ctx, "db2")
		md, ok := metadata.FromOutgoingContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "db2", md.Get(_databaseHeader)[0])
		assert.Len(t, md.Get(_databaseHeader), 1)
	})
}

func TestGrpcClient_HasFeature(t *testing.T) {
	cli := &GrpcClient{flags: 0}
	assert.False(t, cli.HasFeature(MultiDatabase))
	assert.False(t, cli.HasFeature(DescribeDatabase))

	cli = &GrpcClient{flags: MultiDatabase}
	assert.True(t, cli.HasFeature(MultiDatabase))
	assert.False(t, cli.HasFeature(DescribeDatabase))

	cli = &GrpcClient{flags: DescribeDatabase}
	assert.True(t, cli.HasFeature(DescribeDatabase))
	assert.False(t, cli.HasFeature(MultiDatabase))

	cli = &GrpcClient{flags: MultiDatabase | DescribeDatabase}
	assert.True(t, cli.HasFeature(MultiDatabase))
	assert.True(t, cli.HasFeature(DescribeDatabase))
}

func TestGrpcClient_ListIndex(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockSrv := NewMockMilvusServiceClient(t)
		cli := &GrpcClient{srv: mockSrv}

		expectedIndexes := []*milvuspb.IndexDescription{{IndexName: "test_index"}}
		mockSrv.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&milvuspb.DescribeIndexResponse{
			Status:            &commonpb.Status{Code: 0},
			IndexDescriptions: expectedIndexes,
		}, nil)

		indexes, err := cli.ListIndex(context.Background(), "db", "coll")
		assert.NoError(t, err)
		assert.Equal(t, expectedIndexes, indexes)
	})

	t.Run("IndexNotExist", func(t *testing.T) {
		mockSrv := NewMockMilvusServiceClient(t)
		cli := &GrpcClient{srv: mockSrv}

		// Some Milvus versions return IndexNotExist error code when collection has no index
		mockSrv.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_IndexNotExist},
		}, nil)

		indexes, err := cli.ListIndex(context.Background(), "db", "coll")
		assert.NoError(t, err)
		assert.Nil(t, indexes)
	})

	t.Run("GrpcError", func(t *testing.T) {
		mockSrv := NewMockMilvusServiceClient(t)
		cli := &GrpcClient{srv: mockSrv}

		mockSrv.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(nil, errors.New("grpc error"))

		indexes, err := cli.ListIndex(context.Background(), "db", "coll")
		assert.Error(t, err)
		assert.Nil(t, indexes)
	})

	t.Run("StatusError", func(t *testing.T) {
		mockSrv := NewMockMilvusServiceClient(t)
		cli := &GrpcClient{srv: mockSrv}

		mockSrv.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{Code: 1, Reason: "some error"},
		}, nil)

		indexes, err := cli.ListIndex(context.Background(), "db", "coll")
		assert.Error(t, err)
		assert.Nil(t, indexes)
	})
}
