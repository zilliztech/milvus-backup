package milvus

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/golang/protobuf/proto"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/milvus-backup/internal/aimd"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/retry"
	"github.com/zilliztech/milvus-backup/version"
)

//go:generate stringer -type=FeatureFlag
type FeatureFlag uint16

const (
	MultiDatabase FeatureFlag = 1 << iota
	DescribeDatabase
	MultiL0InOneJob
	GetSegmentInfo
	FlushAll
	CollectionLevelGCControl
	FuncRuntimeCheck
	ReplicateMessage
)

type featureTuple struct {
	Constraints *semver.Constraints
	Flag        FeatureFlag
}

// _latestDevVersion is used as a fallback when the server returns a non-semver version
// string (e.g. "master-20260226-abcdef" from dev builds). It ensures lower-bound
// constraints (>= X) pass while upper-bound constraints (< Y) correctly fail.
var _latestDevVersion = semver.MustParse("99.0.0")

var _featureTuples = []featureTuple{
	{Constraints: lo.Must(semver.NewConstraint(">= 2.4.3-0")), Flag: DescribeDatabase},
	{Constraints: lo.Must(semver.NewConstraint(">= 2.6.5-0")), Flag: MultiL0InOneJob},
	{Constraints: lo.Must(semver.NewConstraint(">= 2.5.8-0")), Flag: GetSegmentInfo},
	{Constraints: lo.Must(semver.NewConstraint(">= 2.6.11-0")), Flag: FlushAll},
	{Constraints: lo.Must(semver.NewConstraint(">= 2.6.8-0")), Flag: CollectionLevelGCControl},
	{Constraints: lo.Must(semver.NewConstraint(">= 2.6.8-0")), Flag: FuncRuntimeCheck},
	// ReplicateMessage is only used by 2.5 CDC for incremental data replication.
	// Since 2.5 CDC is no longer maintained, consider removing this in the future.
	{Constraints: lo.Must(semver.NewConstraint(">= 2.5.0-0, < 2.6.0-0")), Flag: ReplicateMessage},
}

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
	HasFeature(flag FeatureFlag) bool
	GetVersion(ctx context.Context) (string, error)
	CreateDatabase(ctx context.Context, dbName string) error
	ListDatabases(ctx context.Context) ([]string, error)
	DescribeDatabase(ctx context.Context, dbName string) (*milvuspb.DescribeDatabaseResponse, error)
	DescribeCollection(ctx context.Context, db, collName string) (*milvuspb.DescribeCollectionResponse, error)
	DropCollection(ctx context.Context, db, collectionName string) error
	ListIndex(ctx context.Context, db, collName string) ([]*milvuspb.IndexDescription, error)
	ShowPartitions(ctx context.Context, db, collName string) (*milvuspb.ShowPartitionsResponse, error)
	GetLoadingProgress(ctx context.Context, db, collName string, partitionNames ...string) (int64, error)
	GetPersistentSegmentInfo(ctx context.Context, db, collName string) ([]*milvuspb.PersistentSegmentInfo, error)
	Flush(ctx context.Context, db, collName string) (*milvuspb.FlushResponse, error)
	FlushAll(ctx context.Context) (*milvuspb.FlushAllResponse, error)
	ListCollections(ctx context.Context, db string) (*milvuspb.ShowCollectionsResponse, error)
	HasCollection(ctx context.Context, db, collName string) (bool, error)
	BulkInsert(ctx context.Context, input GrpcBulkInsertInput) (int64, error)
	GetBulkInsertState(ctx context.Context, taskID int64) (*milvuspb.GetImportStateResponse, error)
	CreateCollection(ctx context.Context, input CreateCollectionInput) error
	AlterCollection(ctx context.Context, db, collName string, properties []*commonpb.KeyValuePair) error
	CreatePartition(ctx context.Context, db, collName, partitionName string) error
	HasPartition(ctx context.Context, db, collName, partitionName string) (bool, error)
	AddField(ctx context.Context, db, collName string, field *schemapb.FieldSchema) error
	CreateIndex(ctx context.Context, input CreateIndexInput) error
	DropIndex(ctx context.Context, db, collName, indexName string) error
	BackupRBAC(ctx context.Context) (*milvuspb.BackupRBACMetaResponse, error)
	RestoreRBAC(ctx context.Context, rbacMeta *milvuspb.RBACMeta) error
	ReplicateMessage(ctx context.Context, channelName string) (string, error)
	CreateReplicateStream(ctx context.Context, sourceClusterID string) (milvuspb.MilvusService_CreateReplicateStreamClient, error)
}

const (
	_authorizationHeader = `authorization`
	_identifierHeader    = `identifier`
	_databaseHeader      = `dbname`
	_clusterIDHeader     = "cluster-id"
)

func statusOk(status *commonpb.Status) bool {
	// nolint
	return status.GetCode() == 0 && status.GetErrorCode() == 0
}

func checkResponse(resp any, err error) error {
	if err != nil {
		return err
	}

	switch res := resp.(type) {
	case interface{ GetStatus() *commonpb.Status }:
		if !statusOk(res.GetStatus()) {
			return fmt.Errorf("client: operation failed: %v", resp.(interface{ GetStatus() *commonpb.Status }).GetStatus())
		}
	case *commonpb.Status:
		if !statusOk(res) {
			return fmt.Errorf("client: operation failed: %v", resp.(*commonpb.Status))
		}
	}
	return nil
}

func isRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "rate limit exceeded")
}

type limiters struct {
	flush *aimd.Limiter

	createCollection *aimd.Limiter
	createPartition  *aimd.Limiter
	createDatabase   *aimd.Limiter
	createIndex      *aimd.Limiter
}

func newLimiters() limiters {
	return limiters{
		flush:            aimd.NewLimiter(0.01, 50, 5),
		createCollection: aimd.NewLimiter(1, 100, 5),
		createPartition:  aimd.NewLimiter(1, 100, 5),
		createDatabase:   aimd.NewLimiter(1, 100, 5),
		createIndex:      aimd.NewLimiter(1, 100, 5),
	}
}

func (l *limiters) close() {
	l.flush.Stop()
	l.createCollection.Stop()
	l.createPartition.Stop()
	l.createDatabase.Stop()
	l.createIndex.Stop()
}

var _ Grpc = (*GrpcClient)(nil)

type GrpcClient struct {
	logger *zap.Logger

	conn *grpc.ClientConn
	srv  milvuspb.MilvusServiceClient

	limiters limiters

	user string
	auth string

	// get from connect
	serverVersion string
	identifier    string
	flags         FeatureFlag
}

func grpcAuth(username, password string) string {
	if username != "" || password != "" {
		value := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
		return value
	}

	return ""
}

func transCred(cfg *cfg.MilvusConfig) (credentials.TransportCredentials, error) {
	tlsMode := cfg.TLSMode.Val
	if tlsMode < 0 || tlsMode > 2 {
		return nil, errors.New("milvus.TLSMode is illegal, support value 0, 1, 2")
	}

	// tls mode 0 disable tls
	if tlsMode == 0 {
		return insecure.NewCredentials(), nil
	}

	// tls mode 1, 2

	// validate server cert
	tlsCfg := &tls.Config{ServerName: cfg.ServerName.Val}
	if cfg.CACertPath.Val != "" {
		b, err := os.ReadFile(cfg.CACertPath.Val)
		if err != nil {
			return nil, fmt.Errorf("client: read ca cert %w", err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("client: failed to append ca certificates")
		}

		tlsCfg.RootCAs = cp
	}

	// tls mode 1, server tls
	if tlsMode == 1 {
		return credentials.NewTLS(tlsCfg), nil
	}

	// tls mode 2, mutual tls
	// use mTLS but key/cert path not set, for backward compatibility, use server tls instead
	// WARN: this behavior will be removed after v0.6.0
	if tlsMode == 2 {
		if cfg.MTLSKeyPath.Val == "" || cfg.MTLSCertPath.Val == "" {
			log.Warn("client: mutual tls enabled but key/cert path not set! will use server tls instead")
			return credentials.NewTLS(tlsCfg), nil
		}

		// use mTLS
		cert, err := tls.LoadX509KeyPair(cfg.MTLSCertPath.Val, cfg.MTLSKeyPath.Val)
		if err != nil {
			return nil, fmt.Errorf("client: load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsCfg), nil
}

func isUnimplemented(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.Unimplemented
}

func NewGrpc(cfg *cfg.MilvusConfig) (*GrpcClient, error) {
	logger := log.L()

	host := fmt.Sprintf("%s:%d", cfg.Address.Val, cfg.Port.Val)
	logger.Info("New milvus grpc client", zap.String("host", host))

	auth := grpcAuth(cfg.User.Val, cfg.Password.Val)

	cerd, err := transCred(cfg)
	if err != nil {
		return nil, fmt.Errorf("client: create transport credentials: %w", err)
	}

	opts := defaultDialOpt()
	opts = append(opts, grpc.WithTransportCredentials(cerd))
	conn, err := grpc.NewClient(host, opts...)
	if err != nil {
		return nil, fmt.Errorf("client: create grpc client failed: %w", err)
	}
	srv := milvuspb.NewMilvusServiceClient(conn)

	cli := &GrpcClient{
		logger: logger,

		conn: conn,
		srv:  srv,

		limiters: newLimiters(),

		user: cfg.User.Val,
		auth: auth,
	}

	if err := cli.connect(context.TODO()); err != nil {
		return nil, fmt.Errorf("client: connect to server: %w", err)
	}

	if err := cli.checkFeature(context.TODO()); err != nil {
		return nil, fmt.Errorf("client: check server feature: %w", err)
	}

	return cli, nil
}

func (g *GrpcClient) newAuthMD(ctx context.Context) metadata.MD {
	md := metadata.MD{}
	if outgoingMD, ok := metadata.FromOutgoingContext(ctx); ok {
		md = outgoingMD.Copy()
	}

	if g.auth != "" {
		md.Set(_authorizationHeader, g.auth)
	}
	if g.identifier != "" {
		md.Set(_identifierHeader, g.identifier)
	}

	return md
}

func (g *GrpcClient) newCtx(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, g.newAuthMD(ctx))
}

func (g *GrpcClient) newCtxWithDB(ctx context.Context, db string) context.Context {
	md := g.newAuthMD(ctx)
	md.Set(_databaseHeader, db)

	return metadata.NewOutgoingContext(ctx, md)
}

func (g *GrpcClient) connect(ctx context.Context) error {
	hostName, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("get hostname : %w", err)
	}

	connReq := &milvuspb.ConnectRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:    "BackupToolCustomSDK",
			SdkVersion: version.Version,
			LocalTime:  time.Now().String(),
			User:       g.user,
			Host:       hostName,
		},
	}

	ctx = g.newCtx(ctx)
	resp, err := g.srv.Connect(ctx, connReq)
	if err != nil {
		if isUnimplemented(err) {
			g.logger.Info("the server does NOT support connect, skip")
			return nil
		}
		return fmt.Errorf("client: connect to server failed: %w", err)
	}

	g.logger.Info("connect to server", zap.String("server", resp.GetServerInfo().GetBuildTags()))
	if !statusOk(resp.GetStatus()) {
		return fmt.Errorf("client: connect to server failed: %v", resp.GetStatus())
	}

	g.serverVersion = resp.GetServerInfo().GetBuildTags()
	g.identifier = strconv.FormatInt(resp.GetIdentifier(), 10)
	return nil
}

func (g *GrpcClient) Close() error {
	g.limiters.close()
	return g.conn.Close()
}

func (g *GrpcClient) HasFeature(flag FeatureFlag) bool {
	return (g.flags & flag) != 0
}

func (g *GrpcClient) GetVersion(ctx context.Context) (string, error) {
	if _, err := semver.NewVersion(g.serverVersion); err == nil {
		g.logger.Info("get version from connect", zap.String("version", g.serverVersion))
		return g.serverVersion, nil
	}

	ctx = g.newCtx(ctx)
	resp, err := g.srv.GetVersion(ctx, &milvuspb.GetVersionRequest{})
	if err := checkResponse(resp, err); err != nil {
		return "", fmt.Errorf("client: get version failed: %w", err)
	}

	ver := resp.GetVersion()
	g.logger.Info("get version from RPC", zap.String("version", ver))
	return ver, nil
}

func (g *GrpcClient) checkFeature(ctx context.Context) error {
	ctx = g.newCtx(ctx)
	_, err := g.srv.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
	if err != nil {
		if isUnimplemented(err) {
			g.logger.Info("the server does NOT support multi database")
		} else {
			return fmt.Errorf("client: check multi database feature: %w", err)
		}
	} else {
		g.flags |= MultiDatabase
	}

	ver, err := g.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("client: get version: %w", err)
	}
	sem, err := semver.NewVersion(ver)
	if err != nil {
		// Dev/master builds may return non-semver strings like "master-20260226-abcdef".
		g.logger.Warn("cannot parse server version as semver, treat as latest dev build",
			zap.String("version", ver), zap.Error(err))
		sem = _latestDevVersion
	}

	for _, tuple := range _featureTuples {
		if tuple.Constraints.Check(sem) {
			g.logger.Info("server support feature", zap.String("feature", tuple.Flag.String()))
			g.flags |= tuple.Flag
		} else {
			g.logger.Info("server does NOT support feature", zap.String("feature", tuple.Flag.String()))
		}
	}

	return nil
}

func (g *GrpcClient) CreateDatabase(ctx context.Context, dbName string) error {
	if !g.HasFeature(MultiDatabase) {
		return errors.New("client: the server does not support database")
	}

	ctx = g.newCtx(ctx)
	if err := g.limiters.createDatabase.Wait(ctx); err != nil {
		return fmt.Errorf("client: create database wait: %w", err)
	}

	return retry.Do(ctx, func() error {
		resp, err := g.srv.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
		if err := checkResponse(resp, err); err != nil {
			if isRateLimitError(err) {
				g.limiters.createDatabase.Failure()
				return fmt.Errorf("client: create database failed due to rate limit: %w", err)
			} else {
				return retry.Unrecoverable(fmt.Errorf("client: create database: %w", err))
			}
		}
		g.limiters.createDatabase.Success()

		return nil
	})
}

func (g *GrpcClient) ListDatabases(ctx context.Context) ([]string, error) {
	ctx = g.newCtx(ctx)
	if !g.HasFeature(MultiDatabase) {
		return nil, errors.New("client: the server does not support database")
	}

	resp, err := g.srv.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: list databases failed: %w", err)
	}

	return resp.GetDbNames(), nil
}

func (g *GrpcClient) DescribeCollection(ctx context.Context, db, collName string) (*milvuspb.DescribeCollectionResponse, error) {
	ctx = g.newCtxWithDB(ctx, db)
	resp, err := g.srv.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: describe collection failed: %w", err)
	}

	return resp, nil
}

func (g *GrpcClient) DescribeDatabase(ctx context.Context, dbName string) (*milvuspb.DescribeDatabaseResponse, error) {
	if !g.HasFeature(MultiDatabase) {
		return nil, errors.New("client: the server does not support database")
	}

	ctx = g.newCtxWithDB(ctx, dbName)
	resp, err := g.srv.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{DbName: dbName})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: describe database failed: %w", err)
	}

	return resp, nil
}

func (g *GrpcClient) ListIndex(ctx context.Context, db, collName string) ([]*milvuspb.IndexDescription, error) {
	ctx = g.newCtxWithDB(ctx, db)
	resp, err := g.srv.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{CollectionName: collName})
	if err != nil {
		return nil, fmt.Errorf("client: describe index failed: %w", err)
	}
	// Some Milvus versions return IndexNotExist error code when collection has no index
	// nolint
	if resp.GetStatus().GetErrorCode() == commonpb.ErrorCode_IndexNotExist {
		return nil, nil
	}
	if err := checkResponse(resp, nil); err != nil {
		return nil, fmt.Errorf("client: describe index failed: %w", err)
	}

	return resp.IndexDescriptions, nil
}

func (g *GrpcClient) ShowPartitions(ctx context.Context, db, collName string) (*milvuspb.ShowPartitionsResponse, error) {
	ctx = g.newCtxWithDB(ctx, db)
	resp, err := g.srv.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: show partitions failed: %w", err)
	}
	return resp, nil
}

func (g *GrpcClient) GetLoadingProgress(ctx context.Context, db, collName string, partitionNames ...string) (int64, error) {
	ctx = g.newCtxWithDB(ctx, db)
	var resp *milvuspb.GetLoadingProgressResponse

	err := retry.Do(ctx, func() error {
		var err error
		req := &milvuspb.GetLoadingProgressRequest{CollectionName: collName, PartitionNames: partitionNames}
		resp, err = g.srv.GetLoadingProgress(ctx, req)
		if err != nil {
			return fmt.Errorf("client: get loading progress: %w", err)
		}

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("client: get loading progress after retry: %w", err)
	}

	return resp.GetProgress(), nil
}

func (g *GrpcClient) GetPersistentSegmentInfo(ctx context.Context, db, collName string) ([]*milvuspb.PersistentSegmentInfo, error) {
	ctx = g.newCtxWithDB(ctx, db)
	var resp *milvuspb.GetPersistentSegmentInfoResponse
	// The GetPersistentSegmentInfo interface may return a Segment not found error
	// when compaction/stats is in progress.
	// So retry several times.
	err := retry.Do(ctx, func() error {
		var err error
		resp, err = g.srv.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{CollectionName: collName})
		if err := checkResponse(resp, err); err != nil {
			return fmt.Errorf("client: get persistent segment info: %w", err)
		}

		return nil
	}, retry.Attempts(50), retry.MaxSleepTime(100*time.Millisecond))

	if err != nil {
		return nil, fmt.Errorf("client: get persistent segment info: %w", err)
	}

	return resp.GetInfos(), nil
}

func (g *GrpcClient) Flush(ctx context.Context, db, collName string) (*milvuspb.FlushResponse, error) {
	ctx = g.newCtxWithDB(ctx, db)
	ns := namespace.New(db, collName)

	var resp *milvuspb.FlushResponse
	err := retry.Do(ctx, func() error {
		start := time.Now()
		if err := g.limiters.flush.Wait(ctx); err != nil {
			return retry.Unrecoverable(fmt.Errorf("client: flush wait: %w", err))
		}
		cost := time.Since(start)
		g.logger.Info("flush wait aimd", zap.Duration("cost", cost), zap.String("ns", ns.String()))

		innerResp, innerErr := g.srv.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{ns.CollName()}})
		if err := checkResponse(innerResp, innerErr); err != nil {
			if isRateLimitError(err) {
				g.limiters.flush.Failure()
			}
			return fmt.Errorf("client: flush failed due to rate limit: %w", err)
		}
		g.limiters.flush.Success()
		resp = innerResp
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("client: flush : %w", err)
	}

	segmentIDs, has := resp.GetCollSegIDs()[ns.CollName()]
	ids := segmentIDs.GetData()
	if has {
		flushTS := resp.GetCollFlushTs()[ns.CollName()]
		if err := g.checkFlush(ctx, ids, flushTS, ns); err != nil {
			return nil, fmt.Errorf("client: check flush : %w", err)
		}
	}

	return resp, nil
}

func (g *GrpcClient) checkFlush(ctx context.Context, segIDs []int64, flushTS uint64, ns namespace.NS) error {
	start := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resp, err := g.srv.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs:     segIDs,
				FlushTs:        flushTS,
				CollectionName: ns.CollName(),
			})
			if err != nil {
				g.logger.Warn("get flush state failed, will retry", zap.Error(err))
			}
			if resp.GetFlushed() {
				return nil
			}

			cost := time.Since(start)
			if cost > 30*time.Minute {
				g.logger.Warn("waiting for the flush to complete took too much time! may milvus is not healthy",
					zap.Duration("cost", cost),
					zap.String("ns", ns.String()),
					zap.Int64s("segment_ids", segIDs),
					zap.Uint64("flush_ts", flushTS))
			}
		}
	}
}

func (g *GrpcClient) FlushAll(ctx context.Context) (*milvuspb.FlushAllResponse, error) {
	ctx = g.newCtx(ctx)

	var resp *milvuspb.FlushAllResponse
	err := retry.Do(ctx, func() error {
		innerResp, innerErr := g.srv.FlushAll(ctx, &milvuspb.FlushAllRequest{})
		if err := checkResponse(innerResp, innerErr); err != nil {
			return fmt.Errorf("client: flush all: %w", err)
		}
		resp = innerResp
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("client: flush all: %w", err)
	}

	pchTS := make(map[string]uint64, len(resp.GetFlushAllMsgs()))
	for pch, msg := range resp.GetFlushAllMsgs() {
		tt, err := GetTT(msg)
		if err != nil {
			return nil, fmt.Errorf("client: get tt from flush all msg: %w", err)
		}
		pchTS[pch] = tt
	}

	if err := g.checkFlushAll(ctx, pchTS); err != nil {
		return nil, fmt.Errorf("client: check flush all: %w", err)
	}

	return resp, nil
}

func (g *GrpcClient) checkFlushAll(ctx context.Context, flushAllTss map[string]uint64) error {
	start := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			req := &milvuspb.GetFlushAllStateRequest{FlushAllTss: flushAllTss}
			resp, err := g.srv.GetFlushAllState(ctx, req)
			if err != nil {
				return fmt.Errorf("client: get flush all state: %w", err)
			}
			if resp.GetFlushed() {
				return nil
			}

			cost := time.Since(start)
			if cost > 30*time.Minute {
				g.logger.Warn("waiting for the flush to complete took too much time! may milvus is not healthy",
					zap.Duration("cost", cost),
					zap.Any("flush_all_tss", flushAllTss))
			}
		}
	}
}

func (g *GrpcClient) ListCollections(ctx context.Context, db string) (*milvuspb.ShowCollectionsResponse, error) {
	ctx = g.newCtxWithDB(ctx, db)
	resp, err := g.srv.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: list collections failed: %w", err)
	}

	return resp, nil
}

func (g *GrpcClient) HasCollection(ctx context.Context, db, collName string) (bool, error) {
	ctx = g.newCtxWithDB(ctx, db)
	resp, err := g.srv.HasCollection(ctx, &milvuspb.HasCollectionRequest{CollectionName: collName})
	if err := checkResponse(resp, err); err != nil {
		return false, fmt.Errorf("client: has collection failed: %w", err)
	}
	return resp.GetValue(), nil
}

type GrpcBulkInsertInput struct {
	DB             string
	CollectionName string
	PartitionName  string
	Paths          []string // offset 0 is path to insertLog file, offset 1 is path to deleteLog file
	BackupTS       uint64
	IsL0           bool
	StorageVersion int64
	EZK            string
}

func (in *GrpcBulkInsertInput) opts() []*commonpb.KeyValuePair {
	opts := []*commonpb.KeyValuePair{{Key: "skip_disk_quota_check", Value: "true"}}

	if in.BackupTS > 0 {
		opts = append(opts, &commonpb.KeyValuePair{Key: "end_ts", Value: strconv.FormatUint(in.BackupTS, 10)})
	}

	if in.IsL0 {
		opts = append(opts, &commonpb.KeyValuePair{Key: "l0_import", Value: "true"})
	} else {
		opts = append(opts, &commonpb.KeyValuePair{Key: "backup", Value: "true"})
	}

	if in.StorageVersion > 0 {
		opt := &commonpb.KeyValuePair{Key: "storage_version", Value: strconv.FormatInt(in.StorageVersion, 10)}
		opts = append(opts, opt)
	}

	if in.EZK != "" {
		opts = append(opts, &commonpb.KeyValuePair{Key: "ezk", Value: in.EZK})
	}

	return opts
}

func (g *GrpcClient) BulkInsert(ctx context.Context, input GrpcBulkInsertInput) (int64, error) {
	ctx = g.newCtxWithDB(ctx, input.DB)

	in := &milvuspb.ImportRequest{
		CollectionName: input.CollectionName,
		PartitionName:  input.PartitionName,
		Files:          input.Paths,
		Options:        input.opts(),
	}
	resp, err := g.srv.Import(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return 0, fmt.Errorf("client: bulk insert failed: %w", err)
	}

	return resp.GetTasks()[0], nil
}

func (g *GrpcClient) GetBulkInsertState(ctx context.Context, taskID int64) (*milvuspb.GetImportStateResponse, error) {
	ctx = g.newCtx(ctx)

	var resp *milvuspb.GetImportStateResponse
	err := retry.Do(ctx, func() error {
		innerResp, innerErr := g.srv.GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: taskID})
		if err := checkResponse(innerResp, innerErr); err != nil {
			return fmt.Errorf("client: get bulk insert state: %w", err)
		}
		resp = innerResp
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("client: get bulk insert state after retry: %w", err)
	}

	return resp, nil
}

type CreateCollectionInput struct {
	DB           string
	Schema       *schemapb.CollectionSchema
	ConsLevel    commonpb.ConsistencyLevel
	ShardNum     int32
	PartitionNum int
	Properties   []*commonpb.KeyValuePair
}

func (g *GrpcClient) CreateCollection(ctx context.Context, input CreateCollectionInput) error {
	ctx = g.newCtxWithDB(ctx, input.DB)

	bs, err := proto.Marshal(input.Schema)
	if err != nil {
		return fmt.Errorf("client: create collection marshal proto: %w", err)
	}
	in := &milvuspb.CreateCollectionRequest{
		CollectionName:   input.Schema.Name,
		Schema:           bs,
		ConsistencyLevel: input.ConsLevel,
		ShardsNum:        input.ShardNum,
		NumPartitions:    int64(input.PartitionNum),
		Properties:       input.Properties,
	}

	return retry.Do(ctx, func() error {
		if err := g.limiters.createCollection.Wait(ctx); err != nil {
			return retry.Unrecoverable(fmt.Errorf("client: create collection wait: %w", err))
		}

		resp, err := g.srv.CreateCollection(ctx, in)
		if err := checkResponse(resp, err); err != nil {
			if isRateLimitError(err) {
				g.limiters.createCollection.Failure()
				return fmt.Errorf("client: create collection failed: %w", err)
			}

			return retry.Unrecoverable(fmt.Errorf("client: create collection: %w", err))
		}
		g.limiters.createCollection.Success()

		return nil
	})
}

func (g *GrpcClient) AlterCollection(ctx context.Context, db, collName string, properties []*commonpb.KeyValuePair) error {
	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.AlterCollectionRequest{CollectionName: collName, Properties: properties}
	resp, err := g.srv.AlterCollection(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: alter collection failed: %w", err)
	}

	return nil
}

func (g *GrpcClient) DropCollection(ctx context.Context, db, collectionName string) error {
	ctx = g.newCtxWithDB(ctx, db)
	resp, err := g.srv.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: collectionName})
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop collection failed: %w", err)
	}

	return nil
}

func (g *GrpcClient) CreatePartition(ctx context.Context, db, collName, partitionName string) error {
	ctx = g.newCtxWithDB(ctx, db)

	in := &milvuspb.CreatePartitionRequest{CollectionName: collName, PartitionName: partitionName}
	return retry.Do(ctx, func() error {
		if err := g.limiters.createPartition.Wait(ctx); err != nil {
			return retry.Unrecoverable(fmt.Errorf("client: create partition wait: %w", err))
		}

		resp, err := g.srv.CreatePartition(ctx, in)
		if err := checkResponse(resp, err); err != nil {
			if isRateLimitError(err) {
				g.limiters.createPartition.Failure()
				return fmt.Errorf("client: create partition failed due to rate limit: %w", err)
			} else {
				return retry.Unrecoverable(fmt.Errorf("client: create partition: %w", err))
			}
		}
		g.limiters.createPartition.Success()

		return nil
	})
}

func (g *GrpcClient) HasPartition(ctx context.Context, db, collName, partitionName string) (bool, error) {
	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.HasPartitionRequest{CollectionName: collName, PartitionName: partitionName}
	resp, err := g.srv.HasPartition(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return false, fmt.Errorf("client: has partition failed: %w", err)
	}
	return resp.GetValue(), nil
}

func (g *GrpcClient) AddField(ctx context.Context, db, collName string, field *schemapb.FieldSchema) error {
	ctx = g.newCtxWithDB(ctx, db)

	bytes, err := proto.Marshal(field)
	if err != nil {
		return fmt.Errorf("client: add field marshal proto: %w", err)
	}

	in := &milvuspb.AddCollectionFieldRequest{CollectionName: collName, Schema: bytes}
	resp, err := g.srv.AddCollectionField(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: add field failed: %w", err)
	}

	return nil
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

func (g *GrpcClient) CreateIndex(ctx context.Context, input CreateIndexInput) error {
	ctx = g.newCtxWithDB(ctx, input.DB)

	in := &milvuspb.CreateIndexRequest{
		CollectionName: input.CollectionName,
		FieldName:      input.FieldName,
		IndexName:      input.IndexName,
		ExtraParams:    mapKvPairs(input.Params),
	}

	return retry.Do(ctx, func() error {
		if err := g.limiters.createIndex.Wait(ctx); err != nil {
			return retry.Unrecoverable(fmt.Errorf("client: create index wait: %w", err))
		}

		resp, err := g.srv.CreateIndex(ctx, in)
		if err := checkResponse(resp, err); err != nil {
			if isRateLimitError(err) {
				g.limiters.createIndex.Failure()
				return fmt.Errorf("client: create index failed due to rate limit: %w", err)
			} else {
				return retry.Unrecoverable(fmt.Errorf("client: create index: %w", err))
			}
		}
		g.limiters.createIndex.Success()

		return nil
	})
}

func (g *GrpcClient) DropIndex(ctx context.Context, db, collName, indexName string) error {
	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.DropIndexRequest{CollectionName: collName, IndexName: indexName}
	resp, err := g.srv.DropIndex(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop index failed: %w", err)
	}
	return nil
}

func (g *GrpcClient) BackupRBAC(ctx context.Context) (*milvuspb.BackupRBACMetaResponse, error) {
	ctx = g.newCtx(ctx)
	resp, err := g.srv.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: backup rbac failed: %w", err)
	}

	return resp, nil
}

func (g *GrpcClient) RestoreRBAC(ctx context.Context, rbacMeta *milvuspb.RBACMeta) error {
	ctx = g.newCtx(ctx)
	resp, err := g.srv.RestoreRBAC(ctx, &milvuspb.RestoreRBACMetaRequest{RBACMeta: rbacMeta})
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: restore rbac failed: %w", err)
	}

	return nil
}

func (g *GrpcClient) ReplicateMessage(ctx context.Context, channelName string) (string, error) {
	ctx = g.newCtx(ctx)
	resp, err := g.srv.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{ChannelName: channelName})
	if err := checkResponse(resp, err); err != nil {
		return "", fmt.Errorf("client: replicate message: %w", err)
	}

	return resp.GetPosition(), nil
}

func (g *GrpcClient) CreateReplicateStream(ctx context.Context, sourceClusterID string) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
	md := g.newAuthMD(ctx)
	md.Set(_clusterIDHeader, sourceClusterID)

	stream, err := g.srv.CreateReplicateStream(metadata.NewOutgoingContext(ctx, md))
	if err != nil {
		return nil, fmt.Errorf("client: create replicate stream failed: %w", err)
	}

	return stream, nil
}
