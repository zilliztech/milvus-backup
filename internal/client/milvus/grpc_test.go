package milvus

import (
	"context"
	"errors"
	"testing"

	semver "github.com/Masterminds/semver/v3"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func TestGrpcAuth(t *testing.T) {
	got := grpcAuth("username", "password")
	assert.Equal(t, "dXNlcm5hbWU6cGFzc3dvcmQ=", got)

	got = grpcAuth("", "")
	assert.Equal(t, "", got)
}

func TestTransCred(t *testing.T) {
	t.Run("Disabled", func(t *testing.T) {
		cred, err := transCred(&v2.MilvusGrpcConfig{TLSMode: param.Value[string]{Val: v2.TLSDisabled}})
		assert.NoError(t, err)
		assert.Equal(t, insecure.NewCredentials(), cred)
	})

	t.Run("Server", func(t *testing.T) {
		cred, err := transCred(&v2.MilvusGrpcConfig{TLSMode: param.Value[string]{Val: v2.TLSServer}})
		assert.NoError(t, err)
		assert.NotNil(t, cred)
	})

	// v1 quietly fell back to server TLS when the mutual key pair was missing
	// or unreadable. Configuration validation rules out a missing pair, and an
	// unreadable one is an error rather than a weaker connection.
	t.Run("MutualWithUnreadableKeyPair", func(t *testing.T) {
		cred, err := transCred(&v2.MilvusGrpcConfig{
			TLSMode:      param.Value[string]{Val: v2.TLSMutual},
			MTLSCertPath: param.Value[string]{Val: "/no/such/cert.pem"},
			MTLSKeyPath:  param.Value[string]{Val: "/no/such/key.pem"},
		})
		assert.Error(t, err)
		assert.Nil(t, cred)
	})
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

func TestReplicateMessageConstraint(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    bool
	}{
		{"Milvus2.5.0", "2.5.0", true},
		{"Milvus2.5.6", "2.5.6", true},
		{"Milvus2.5.99", "2.5.99", true},
		{"Milvus2.4.9", "2.4.9", false},
		{"Milvus2.6.0", "2.6.0", false},
		{"Milvus2.6.9", "2.6.9", false},
		{"Milvus2.7.0", "2.7.0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tuple := range _featureTuples {
				if tuple.Flag == ReplicateMessage {
					ver, err := semver.NewVersion(tt.version)
					assert.NoError(t, err)
					assert.Equal(t, tt.want, tuple.Constraints.Check(ver))
					return
				}
			}
			t.Fatal("ReplicateMessage not found in _featureTuples")
		})
	}
}

// Real strings observed from a Zilliz Cloud instance: Connect reports the product
// description, GetVersion reports an internal build tag whose branch could not be resolved
// at build time and therefore reads "unknown".
const (
	_cloudDesc     = "Zilliz Cloud Vector Database(Compatible with Milvus 2.6)"
	_cloudBuildTag = "unknown-20260608-7ea6e3526"
)

func TestGrpcClient_parseVersionForFeature(t *testing.T) {
	tests := []struct {
		name           string
		versions       []string
		wantConstraint string
		wantPass       bool
	}{
		// Strict semver: real release versions match constraints based on actual values.
		{"Release2.6.0_NoMultiL0", []string{"2.6.0"}, ">= 2.6.5-0", false},
		{"Release2.6.5_HasMultiL0", []string{"2.6.5"}, ">= 2.6.5-0", true},
		{"Release2.6.11_HasFlushAll", []string{"2.6.11"}, ">= 2.6.11-0", true},
		{"Release2.6.10_NoFlushAll", []string{"2.6.10"}, ">= 2.6.11-0", false},

		// Milvus releases report build tags with a leading "v" (e.g. "v2.2.16").
		// Without v-prefix stripping these would fall back to _latestDevVersion and
		// incorrectly enable features the old release does not implement (e.g.
		// DescribeDatabase on v2.2.16, which then crashes with Unimplemented).
		{"VPrefixV2.2.16_NoDescribeDatabase", []string{"v2.2.16"}, ">= 2.4.3-0", false},
		{"VPrefixV2.3.22_NoDescribeDatabase", []string{"v2.3.22"}, ">= 2.4.3-0", false},
		{"VPrefixV2.4.23_HasDescribeDatabase", []string{"v2.4.23"}, ">= 2.4.3-0", true},
		{"VPrefixV2.5.20_NoMultiL0", []string{"v2.5.20"}, ">= 2.6.5-0", false},
		{"VPrefixV2.6.5_HasMultiL0", []string{"v2.6.5"}, ">= 2.6.5-0", true},
		{"VPrefixV2.5.20_HasReplicateMessage", []string{"v2.5.20"}, ">= 2.5.0-0, < 2.6.0-0", true},

		// Four-part versions collapse to their release base. Without the collapse
		// these fail StrictNewVersion, fall back to _latestDevVersion, and wrongly
		// enable features the underlying release does not implement.
		// "v2.4.0.1-gpu-beta" and "v2.4.0.2-gpu-beta" are real milvusdb/milvus tags;
		// the trailing component (with its prerelease suffix) is dropped to "2.4.0".
		{"FourPartV2.4.0.1GpuBeta_NoMultiL0", []string{"v2.4.0.1-gpu-beta"}, ">= 2.6.5-0", false},
		{"FourPartV2.4.0.1GpuBeta_NoDescribeDatabase", []string{"v2.4.0.1-gpu-beta"}, ">= 2.4.3-0", false},
		{"FourPartV2.3.22.6_NoMultiL0", []string{"v2.3.22.6"}, ">= 2.6.5-0", false},
		{"FourPartV2.6.5.3_HasMultiL0", []string{"v2.6.5.3"}, ">= 2.6.5-0", true},

		// Dev build tag: the branch names the line, so it resolves to the head of that line
		// and passes every constraint within it. Without StrictNewVersion this regresses:
		// the lenient parser turns it into 2.6.0-20260404-31fb3fc, which is LESS than
		// 2.6.5-0 and disables features.
		{"DevTag2.6_HasMultiL0", []string{"2.6-20260404-31fb3fc"}, ">= 2.6.5-0", true},
		{"DevTag2.6_HasFlushAll", []string{"2.6-20260404-31fb3fc"}, ">= 2.6.11-0", true},
		{"DevTag2.6_HasGC", []string{"2.6-20260404-31fb3fc"}, ">= 2.6.8-0", true},
		{"DevTag2.6_NoReplicateMessage", []string{"2.6-20260404-31fb3fc"}, ">= 2.5.0-0, < 2.6.0-0", false},

		// A dev build of the 2.5 line must stay inside it. Resolving it to _latestDevVersion
		// instead wrongly disables ReplicateMessage, the one feature with an upper bound.
		{"DevTag2.5_HasReplicateMessage", []string{"2.5-20260404-31fb3fc"}, ">= 2.5.0-0, < 2.6.0-0", true},
		{"DevTag2.5_NoFlushAll", []string{"2.5-20260404-31fb3fc"}, ">= 2.6.11-0", false},

		// Zilliz Cloud answers Connect with a product description naming the Milvus line and
		// GetVersion with a build tag that has no version in it at all. The description is
		// the only source that knows anything, so it decides.
		{"CloudDesc_HasFlushAll", []string{_cloudDesc, _cloudBuildTag}, ">= 2.6.11-0", true},
		{"CloudDesc_HasDescribeDatabase", []string{_cloudDesc, _cloudBuildTag}, ">= 2.4.3-0", true},
		{"CloudDesc_NoReplicateMessage", []string{_cloudDesc, _cloudBuildTag}, ">= 2.5.0-0, < 2.6.0-0", false},
		{"CloudDesc_DoesNotLeakPastTheLine", []string{_cloudDesc, _cloudBuildTag}, ">= 2.7.0-0", false},

		// The version must be read from the Milvus line, not from a number that happens to
		// appear earlier in the product name.
		{"CloudDescWithProductVersion_NoReplicateMessage", []string{"Zilliz Cloud 3.0 Vector Database(Compatible with Milvus 2.5)"}, ">= 2.5.0-0, < 2.6.0-0", true},

		// A release version wins over an embedded one whichever call reported it.
		{"ReleaseBeatsDescription", []string{_cloudDesc, "2.6.10"}, ">= 2.6.11-0", false},
		{"ReleaseBeatsDescriptionReversed", []string{"2.6.10", _cloudDesc}, ">= 2.6.11-0", false},

		// Build tags that carry only a branch, a date and a commit: nothing to parse.
		{"UnknownBranchTag_HasFlushAll", []string{_cloudBuildTag}, ">= 2.6.11-0", true},
		{"MasterTag_HasFlushAll", []string{"master-20260226-abcdef"}, ">= 2.6.11-0", true},

		// Empty strings: also fall back to dev.
		{"Empty_HasFlushAll", []string{""}, ">= 2.6.11-0", true},
		{"AllEmpty_HasFlushAll", []string{"", ""}, ">= 2.6.11-0", true},
	}

	cli := &GrpcClient{logger: zap.NewNop()}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sem := cli.parseVersionForFeature(tt.versions...)
			constraint, err := semver.NewConstraint(tt.wantConstraint)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantPass, constraint.Check(sem))
		})
	}
}

func TestEmbeddedVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"MinorOnlyResolvesToHeadOfLine", "2.6-20260404-31fb3fc", "2.6.9999"},
		{"DescriptionMinorOnly", _cloudDesc, "2.6.9999"},
		{"DescriptionWithPatchKeepsPatch", "Zilliz Cloud Vector Database(Compatible with Milvus 2.5.8)", "2.5.8"},
		{"NoSeparatorBeforeVersion", "milvus2.6", "2.6.9999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sem, ok := embeddedVersion(tt.version)
			require.True(t, ok)
			assert.Equal(t, tt.want, sem.String())
		})
	}

	t.Run("NoVersionAtAll", func(t *testing.T) {
		for _, ver := range []string{_cloudBuildTag, "master-20260226-abcdef", ""} {
			_, ok := embeddedVersion(ver)
			assert.False(t, ok, ver)
		}
	})
}

func TestGrpcClient_GetVersion(t *testing.T) {
	t.Run("ConnectVersionValid", func(t *testing.T) {
		mockSrv := NewMockMilvusServiceClient(t)
		cli := &GrpcClient{srv: mockSrv, serverVersion: "2.5.0", logger: log.L()}

		ver, err := cli.GetVersion(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "2.5.0", ver)
	})

	t.Run("ConnectVersionInvalidFallsBackToRPC", func(t *testing.T) {
		mockSrv := NewMockMilvusServiceClient(t)
		cli := &GrpcClient{srv: mockSrv, serverVersion: "", logger: log.L()}

		mockSrv.EXPECT().GetVersion(mock.Anything, mock.Anything).Return(&milvuspb.GetVersionResponse{
			Status:  &commonpb.Status{Code: 0},
			Version: "2.6.0",
		}, nil)

		ver, err := cli.GetVersion(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "2.6.0", ver)
	})

	t.Run("BothInvalid", func(t *testing.T) {
		mockSrv := NewMockMilvusServiceClient(t)
		cli := &GrpcClient{srv: mockSrv, serverVersion: "", logger: log.L()}

		mockSrv.EXPECT().GetVersion(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

		_, err := cli.GetVersion(context.Background())
		assert.Error(t, err)
	})
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
