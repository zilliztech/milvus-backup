package milvus

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

type schemaServer struct {
	milvuspb.UnimplementedMilvusServiceServer
	requests chan *milvuspb.CreateCollectionRequest
}

func (*schemaServer) Connect(context.Context, *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	return &milvuspb.ConnectResponse{Status: &commonpb.Status{}, ServerInfo: &commonpb.ServerInfo{BuildTags: "99.0.0"}}, nil
}

func (s *schemaServer) CreateCollection(_ context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	s.requests <- request
	return &commonpb.Status{}, nil
}

func TestPreserveFieldIDsCapabilityAndWireRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	srv := &schemaServer{requests: make(chan *milvuspb.CreateCollectionRequest, 1)}
	milvuspb.RegisterMilvusServiceServer(server, srv)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	for _, enabled := range []bool{false, true} {
		name := "Disabled"
		if enabled {
			name = "Enabled"
		}
		t.Run(name, func(t *testing.T) {
			config, err := cfg.Load("", map[string]string{
				"milvus.grpc.address": "127.0.0.1", "milvus.grpc.port": strconv.Itoa(listener.Addr().(*net.TCPAddr).Port),
				"milvus.grpc.tlsMode": cfg.TLSDisabled, "milvus.preserveFieldIDs": strconv.FormatBool(enabled),
			})
			require.NoError(t, err)
			client, err := NewGrpc(&config.Milvus)
			require.NoError(t, err)
			defer client.Close()
			assert.Equal(t, enabled, client.HasFeature(PreserveFieldIDs), "even a future version must not enable an unestablished contract")
			if !enabled {
				return
			}
			schema := &schemapb.CollectionSchema{Name: "restored", EnableDynamicField: true,
				Fields:            []*schemapb.FieldSchema{{FieldID: 100, Name: "id"}, {FieldID: 102, Name: "$meta", IsDynamic: true, DataType: schemapb.DataType_JSON}},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 103, Name: "items", Fields: []*schemapb.FieldSchema{{FieldID: 109, Name: "value", ElementNullable: true}}}}}
			require.NoError(t, client.CreateCollection(context.Background(), CreateCollectionInput{DB: "default", Schema: schema,
				Properties: []*commonpb.KeyValuePair{{Key: "preserve_field_ids", Value: "true"}, {Key: "max_field_id", Value: "111"}}}))
			request := <-srv.requests
			var wireSchema schemapb.CollectionSchema
			require.NoError(t, proto.Unmarshal(request.GetSchema(), &wireSchema))
			assert.True(t, proto.Equal(schema, &wireSchema))
			assert.Equal(t, "preserve_field_ids", request.GetProperties()[0].GetKey())
			assert.Equal(t, "true", request.GetProperties()[0].GetValue())
			assert.Equal(t, "111", request.GetProperties()[1].GetValue())
		})
	}
}
