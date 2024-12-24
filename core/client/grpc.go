package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/version"
)

const (
	disableDatabase uint64 = 1 << iota
)

func defaultDialOpt() []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32), // math.MaxInt32 = 2147483647, 2GB - 1
			// not setting max send msg size, since default is Unlimited
		),
		grpc.WithChainUnaryInterceptor(grpcretry.UnaryClientInterceptor(
			grpcretry.WithMax(6),
			grpcretry.WithBackoff(func(attempt uint) time.Duration {
				return 60 * time.Millisecond * time.Duration(math.Pow(3, float64(attempt)))
			}),
			grpcretry.WithCodes(codes.Unavailable, codes.ResourceExhausted)),
		),
	}

	return opts
}

type Grpc interface {
	Close() error
	SupportMultiDatabase() bool
	GetVersion(ctx context.Context) (string, error)
	CreateDatabase(ctx context.Context, dbName string) error
	ListDatabases(ctx context.Context) ([]string, error)
	DescribeCollection(ctx context.Context, db, collName string) (*milvuspb.DescribeCollectionResponse, error)
	DropCollection(ctx context.Context, db, collectionName string) error
	ListIndex(ctx context.Context, db, collName string) ([]*milvuspb.IndexDescription, error)
	ShowPartitions(ctx context.Context, db, collName string) (*milvuspb.ShowPartitionsResponse, error)
	GetLoadingProgress(ctx context.Context, db, collName string, partitionNames []string) (int64, error)
	GetPersistentSegmentInfo(ctx context.Context, db, collName string) ([]*milvuspb.PersistentSegmentInfo, error)
	Flush(ctx context.Context, db, collName string) (*milvuspb.FlushResponse, error)
	ListCollections(ctx context.Context, db string) (*milvuspb.ShowCollectionsResponse, error)
	HasCollection(ctx context.Context, db, collName string) (bool, error)
	BulkInsert(ctx context.Context, input BulkInsertInput) (int64, error)
	GetBulkInsertState(ctx context.Context, taskID int64) (*milvuspb.GetImportStateResponse, error)
	CreateCollection(ctx context.Context, input CreateCollectionInput) error
	CreatePartition(ctx context.Context, db, collName, partitionName string) error
	HasPartition(ctx context.Context, db, collName, partitionName string) (bool, error)
	CreateIndex(ctx context.Context, input CreateIndexInput) error
	DropIndex(ctx context.Context, db, collName, indexName string) error
	BackupRBAC(ctx context.Context) (*milvuspb.BackupRBACMetaResponse, error)
	RestoreRBAC(ctx context.Context, rbacMeta *milvuspb.RBACMeta) error
}

const (
	authorizationHeader = `authorization`
	identifierHeader    = `identifier`
	databaseHeader      = `dbname`
)

func ok(status *commonpb.Status) bool { return status.GetCode() == 0 }

func checkResponse(resp any, err error) error {
	if err != nil {
		return err
	}

	switch resp.(type) {
	case interface{ GetStatus() *commonpb.Status }:
		if !ok(resp.(interface{ GetStatus() *commonpb.Status }).GetStatus()) {
			return fmt.Errorf("client: operation failed: %v", resp.(interface{ GetStatus() *commonpb.Status }).GetStatus())
		}
	case *commonpb.Status:
		if !ok(resp.(*commonpb.Status)) {
			return fmt.Errorf("client: operation failed: %v", resp.(*commonpb.Status))
		}
	}
	return nil
}

type GrpcClient struct {
	cfg *Cfg

	conn *grpc.ClientConn
	srv  milvuspb.MilvusServiceClient

	auth          string
	serverVersion string
	identifier    string

	flags uint64
}

func NewGrpc(cfg *Cfg) (*GrpcClient, error) {
	addr, opts, err := cfg.parseGrpc()
	if err != nil {
		return nil, fmt.Errorf("client: parse address failed: %w", err)
	}
	auth := cfg.parseAuth()

	opts = append(opts, defaultDialOpt()...)

	conn, err := grpc.NewClient(addr.Host, opts...)
	if err != nil {
		return nil, fmt.Errorf("client: create grpc client failed: %w", err)
	}
	srv := milvuspb.NewMilvusServiceClient(conn)

	cli := &GrpcClient{
		cfg: cfg,

		conn: conn,
		srv:  srv,

		auth: auth,
	}

	return cli, nil
}

func (m *GrpcClient) hasFlags(flags uint64) bool { return (m.flags & flags) > 0 }
func (m *GrpcClient) SupportMultiDatabase() bool { return !m.hasFlags(disableDatabase) }

func (m *GrpcClient) newCtx(ctx context.Context) context.Context {
	if m.auth != "" {
		return metadata.AppendToOutgoingContext(ctx, authorizationHeader, m.auth)
	}
	if m.identifier != "" {
		return metadata.AppendToOutgoingContext(ctx, identifierHeader, m.identifier)
	}
	return ctx
}

func (m *GrpcClient) newCtxWithDB(ctx context.Context, db string) context.Context {
	ctx = m.newCtx(ctx)
	return metadata.AppendToOutgoingContext(ctx, databaseHeader, db)
}

func (m *GrpcClient) connect(ctx context.Context) error {
	hostName, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("get hostname failed: %w", err)
	}

	connReq := &milvuspb.ConnectRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:    "Backup Tool Custom SDK",
			SdkVersion: version.Version,
			LocalTime:  time.Now().String(),
			User:       m.cfg.Username,
			Host:       hostName,
		},
	}

	resp, err := m.srv.Connect(ctx, connReq)
	if err != nil {
		s, ok := status.FromError(err)
		if ok {
			if s.Code() == codes.Unimplemented {
				log.Info("The server does not support the Connect API, skipping")
				m.flags |= disableDatabase
			}
		}
		return fmt.Errorf("client: connect to server failed: %w", err)
	}

	if !ok(resp.GetStatus()) {
		return fmt.Errorf("client: connect to server failed: %v", resp.GetStatus())
	}

	m.serverVersion = resp.GetServerInfo().GetBuildTags()
	m.identifier = strconv.FormatInt(resp.GetIdentifier(), 10)
	return nil
}

func (m *GrpcClient) Close() error {
	return m.conn.Close()
}

func (m *GrpcClient) GetVersion(ctx context.Context) (string, error) {
	ctx = m.newCtx(ctx)
	resp, err := m.srv.GetVersion(ctx, &milvuspb.GetVersionRequest{})
	if err := checkResponse(resp, err); err != nil {
		return "", fmt.Errorf("client: get version failed: %w", err)
	}

	return resp.GetVersion(), nil
}

func (m *GrpcClient) CreateDatabase(ctx context.Context, dbName string) error {
	ctx = m.newCtx(ctx)
	if m.hasFlags(disableDatabase) {
		return errors.New("client: the server does not support database")
	}

	resp, err := m.srv.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: create database failed: %w", err)
	}

	return nil
}

func (m *GrpcClient) ListDatabases(ctx context.Context) ([]string, error) {
	ctx = m.newCtx(ctx)
	if m.hasFlags(disableDatabase) {
		return nil, errors.New("client: the server does not support database")
	}

	resp, err := m.srv.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: list databases failed: %w", err)
	}

	return resp.GetDbNames(), nil
}

func (m *GrpcClient) DescribeCollection(ctx context.Context, db, collName string) (*milvuspb.DescribeCollectionResponse, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: describe collection failed: %w", err)
	}

	return resp, nil
}

func (m *GrpcClient) ListIndex(ctx context.Context, db, collName string) ([]*milvuspb.IndexDescription, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: describe index failed: %w", err)
	}

	return resp.IndexDescriptions, nil
}

func (m *GrpcClient) ShowPartitions(ctx context.Context, db, collName string) (*milvuspb.ShowPartitionsResponse, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: show partitions failed: %w", err)
	}
	return resp, nil
}

func (m *GrpcClient) GetLoadingProgress(ctx context.Context, db, collName string, partitionNames []string) (int64, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{CollectionName: collName, PartitionNames: partitionNames})
	if err != nil {
		return 0, fmt.Errorf("client: get loading progress failed: %w", err)
	}

	return resp.GetProgress(), nil
}

func (m *GrpcClient) GetPersistentSegmentInfo(ctx context.Context, db, collName string) ([]*milvuspb.PersistentSegmentInfo, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: get persistent segment info failed: %w", err)
	}

	return resp.GetInfos(), nil
}

func (m *GrpcClient) Flush(ctx context.Context, db, collName string) (*milvuspb.FlushResponse, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{collName}})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: flush failed: %w", err)
	}

	segmentIDs, has := resp.GetCollSegIDs()[collName]
	ids := segmentIDs.GetData()
	if has && len(ids) > 0 {
		flushed := func() bool {
			getFlushResp, err := m.srv.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs:     ids,
				FlushTs:        resp.GetCollFlushTs()[collName],
				CollectionName: collName,
			})
			if err != nil {
				// TODO max retry
				return false
			}
			return getFlushResp.GetFlushed()
		}
		for !flushed() {
			// respect context deadline/cancel
			select {
			case <-ctx.Done():
				return nil, errors.New("deadline exceeded")
			default:
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	return resp, nil
}

func (m *GrpcClient) ListCollections(ctx context.Context, db string) (*milvuspb.ShowCollectionsResponse, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: list collections failed: %w", err)
	}

	return resp, nil
}

func (m *GrpcClient) HasCollection(ctx context.Context, db, collName string) (bool, error) {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.HasCollection(ctx, &milvuspb.HasCollectionRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return false, fmt.Errorf("client: has collection failed: %w", err)
	}
	return resp.GetValue(), nil
}

type BulkInsertInput struct {
	DB             string
	CollectionName string
	PartitionName  string
	// offset 0 is path to insertLog file, offset 1 is path to deleteLog file
	Paths              []string
	EndTime            int64
	IsL0               bool
	SkipDiskQuotaCheck bool
}

func (m *GrpcClient) BulkInsert(ctx context.Context, input BulkInsertInput) (int64, error) {
	ctx = m.newCtxWithDB(ctx, input.DB)
	var opts []*commonpb.KeyValuePair
	if input.EndTime > 0 {
		opts = append(opts, &commonpb.KeyValuePair{Key: "end_time", Value: strconv.FormatInt(input.EndTime, 10)})
	}
	if input.IsL0 {
		opts = append(opts, &commonpb.KeyValuePair{Key: "l0_import", Value: "true"})
	} else {
		opts = append(opts, &commonpb.KeyValuePair{Key: "backup", Value: "true"})
	}
	skipOpt := &commonpb.KeyValuePair{Key: "skip_disk_quota_check", Value: strconv.FormatBool(input.SkipDiskQuotaCheck)}
	opts = append(opts, skipOpt)

	in := &milvuspb.ImportRequest{
		CollectionName: input.CollectionName,
		PartitionName:  input.PartitionName,
		Files:          input.Paths,
		Options:        opts,
	}
	resp, err := m.srv.Import(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return 0, fmt.Errorf("client: bulk insert failed: %w", err)
	}

	return resp.GetTasks()[0], nil
}

func (m *GrpcClient) GetBulkInsertState(ctx context.Context, taskID int64) (*milvuspb.GetImportStateResponse, error) {
	ctx = m.newCtx(ctx)
	resp, err := m.srv.GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: taskID})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: get bulk insert state failed: %w", err)
	}

	return resp, nil
}

type CreateCollectionInput struct {
	DB           string
	Schema       *schemapb.CollectionSchema
	ConsLevel    commonpb.ConsistencyLevel
	ShardNum     int32
	partitionNum int64
}

func (m *GrpcClient) CreateCollection(ctx context.Context, input CreateCollectionInput) error {
	ctx = m.newCtxWithDB(ctx, input.DB)
	bs, err := proto.Marshal(input.Schema)
	if err != nil {
		return fmt.Errorf("client: create collection failed: %w", err)
	}
	in := &milvuspb.CreateCollectionRequest{
		CollectionName:   input.Schema.Name,
		Schema:           bs,
		ConsistencyLevel: input.ConsLevel,
		ShardsNum:        input.ShardNum,
		NumPartitions:    input.partitionNum,
	}

	resp, err := m.srv.CreateCollection(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: create collection failed: %w", err)
	}

	return nil
}

func (m *GrpcClient) DropCollection(ctx context.Context, db string, collectionName string) error {
	ctx = m.newCtxWithDB(ctx, db)
	resp, err := m.srv.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: collectionName})
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop collection failed: %w", err)
	}

	return nil
}

func (m *GrpcClient) CreatePartition(ctx context.Context, db, collName, partitionName string) error {
	ctx = m.newCtxWithDB(ctx, db)
	in := &milvuspb.CreatePartitionRequest{CollectionName: collName, PartitionName: partitionName}
	resp, err := m.srv.CreatePartition(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: create partition failed: %w", err)
	}

	return nil
}

func (m *GrpcClient) HasPartition(ctx context.Context, db, collName string, partitionName string) (bool, error) {
	ctx = m.newCtxWithDB(ctx, db)
	in := &milvuspb.HasPartitionRequest{CollectionName: collName, PartitionName: partitionName}
	resp, err := m.srv.HasPartition(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return false, fmt.Errorf("client: has partition failed: %w", err)
	}
	return resp.GetValue(), nil
}

func mapKvPairs(m map[string]string) []*commonpb.KeyValuePair {
	pairs := make([]*commonpb.KeyValuePair, 0, len(m))
	for k, v := range m {
		pair := &commonpb.KeyValuePair{Key: k, Value: v}
		pairs = append(pairs, pair)
	}
	return pairs
}

type CreateIndexInput struct {
	DB             string
	CollectionName string
	FieldName      string
	IndexName      string
	Params         map[string]string
}

func (m *GrpcClient) CreateIndex(ctx context.Context, input CreateIndexInput) error {
	ctx = m.newCtxWithDB(ctx, input.DB)
	in := &milvuspb.CreateIndexRequest{
		CollectionName: input.CollectionName,
		FieldName:      input.FieldName,
		IndexName:      input.IndexName,
		ExtraParams:    mapKvPairs(input.Params),
	}

	resp, err := m.srv.CreateIndex(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: create index failed: %w", err)
	}

	return nil
}

func (m *GrpcClient) DropIndex(ctx context.Context, db, collName, indexName string) error {
	ctx = m.newCtxWithDB(ctx, db)
	in := &milvuspb.DropIndexRequest{CollectionName: collName, IndexName: indexName}
	resp, err := m.srv.DropIndex(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop index failed: %w", err)
	}
	return nil
}

func (m *GrpcClient) BackupRBAC(ctx context.Context) (*milvuspb.BackupRBACMetaResponse, error) {
	ctx = m.newCtx(ctx)
	resp, err := m.srv.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: backup rbac failed: %w", err)
	}

	return resp, nil
}

func (m *GrpcClient) RestoreRBAC(ctx context.Context, rbacMeta *milvuspb.RBACMeta) error {
	ctx = m.newCtx(ctx)
	resp, err := m.srv.RestoreRBAC(ctx, &milvuspb.RestoreRBACMetaRequest{RBACMeta: rbacMeta})
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: restore rbac failed: %w", err)
	}

	return nil
}
