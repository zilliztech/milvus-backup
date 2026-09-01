package milvus

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/internal/aimd"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
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
	Snapshot
)

type featureTuple struct {
	Constraints *semver.Constraints
	Flag        FeatureFlag
}

// _latestDevVersion is used as a fallback when none of the strings the server reports about
// itself contains a version at all, e.g. the build tags "master-20260226-abcdef" or
// "unknown-20260608-7ea6e3526", which carry only a branch name, a build date and a commit.
// It ensures lower-bound constraints (>= X) pass while upper-bound constraints (< Y)
// correctly fail.
var _latestDevVersion = semver.MustParse("99.0.0")

// _headOfLinePatch stands in for the patch level of a version that is only known down to
// MAJOR.MINOR: the "2.6" in the dev build tag "2.6-20260404-31fb3fc", or in the vendor
// description "Zilliz Cloud Vector Database(Compatible with Milvus 2.6)". Such a server sits
// somewhere on the 2.6 line and is far more likely to be at its head than at 2.6.0, so it is
// treated as the newest patch of that line.
//
// Unlike _latestDevVersion this still fails the constraints of every other line, which is what
// keeps ReplicateMessage ("< 2.6.0-0") enabled on a 2.5 dev build and disabled on a 2.6 one.
const _headOfLinePatch = 9999

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
	// Snapshot landed on the 3.0 line. The 2.6 branch carries no snapshot code at all, and no
	// 2.6.x tag exposes the RPCs, so this is a clean major-line boundary rather than a patch one.
	{Constraints: lo.Must(semver.NewConstraint(">= 3.0.0-0")), Flag: Snapshot},
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
	HasCollectionByID(ctx context.Context, collectionID int64) (bool, error)
	BulkInsert(ctx context.Context, input GrpcBulkInsertInput) (int64, error)
	GetBulkInsertState(ctx context.Context, taskID int64) (*milvuspb.GetImportStateResponse, error)
	CreateCollection(ctx context.Context, input CreateCollectionInput) error
	AlterCollection(ctx context.Context, db, collName string, properties []*commonpb.KeyValuePair) error
	DropCollectionProperties(ctx context.Context, db, collName string, keys []string) error
	DropCollectionFieldProperties(ctx context.Context, db, collName, fieldName string, keys []string) error
	CreatePartition(ctx context.Context, db, collName, partitionName string) error
	HasPartition(ctx context.Context, db, collName, partitionName string) (bool, error)
	AddField(ctx context.Context, db, collName string, field *schemapb.FieldSchema) error
	CreateIndex(ctx context.Context, input CreateIndexInput) error
	DropIndex(ctx context.Context, db, collName, indexName string) error
	DropIndexProperties(ctx context.Context, db, collName, indexName string, keys []string) error
	GetReplicateConfiguration(ctx context.Context) (*commonpb.ReplicateConfiguration, error)
	GetReplicateInfo(ctx context.Context, sourceClusterID, targetPchannel string) (*milvuspb.GetReplicateInfoResponse, error)
	BackupRBAC(ctx context.Context) (*milvuspb.BackupRBACMetaResponse, error)
	RestoreRBAC(ctx context.Context, rbacMeta *milvuspb.RBACMeta) error
	ReplicateMessage(ctx context.Context, channelName string) (string, error)
	CreateReplicateStream(ctx context.Context, sourceClusterID string) (milvuspb.MilvusService_CreateReplicateStreamClient, error)
	CreateSnapshot(ctx context.Context, db, collName, snapshotName string, compactionProtection time.Duration) error
	DropSnapshot(ctx context.Context, db, collName, snapshotName string) error
	DescribeSnapshot(ctx context.Context, db, collName, snapshotName string) (*milvuspb.DescribeSnapshotResponse, error)
	ExportSnapshot(ctx context.Context, input ExportSnapshotInput) (int64, error)
	GetExportSnapshotState(ctx context.Context, jobID int64) (*milvuspb.ExportSnapshotInfo, error)
	RestoreExternalSnapshot(ctx context.Context, input RestoreExternalSnapshotInput) (int64, error)
	GetRestoreSnapshotState(ctx context.Context, jobID int64) (*milvuspb.RestoreSnapshotInfo, error)
}

// _collectionNotFoundCode is merr.ErrCollectionNotFound's code. Newer servers
// report it in Status.Code while older ones set the legacy CollectionNotExists
// error code, so both are treated as the collection being absent.
const _collectionNotFoundCode = 100

const (
	_authorizationHeader = `authorization`
	_identifierHeader    = `identifier`
	_databaseHeader      = `dbname`
	_clusterIDHeader     = "cluster-id"
)

func statusOk(status *commonpb.Status) bool {
	//nolint:staticcheck // SA1019: GetErrorCode is needed for backward compatibility with older Milvus versions
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

func transCred(c *v2.MilvusGrpcConfig) (credentials.TransportCredentials, error) {
	if c.TLSMode.Val == v2.TLSDisabled {
		return insecure.NewCredentials(), nil
	}

	// Both server and mutual verify the server certificate.
	tlsCfg := &tls.Config{ServerName: c.ServerName.Val}
	if c.CACertPath.Val != "" {
		b, err := os.ReadFile(c.CACertPath.Val)
		if err != nil {
			return nil, fmt.Errorf("client: read ca cert %w", err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("client: failed to append ca certificates")
		}

		tlsCfg.RootCAs = cp
	}

	// Mutual additionally presents a client certificate. v1 quietly fell back to
	// server TLS when the key pair was missing; nothing falls back here, because
	// a v2 config is rejected in validation and a v1 config is downgraded while
	// it is translated. Silently weakening TLS is not something to keep doing.
	if c.TLSMode.Val == v2.TLSMutual {
		cert, err := tls.LoadX509KeyPair(c.MTLSCertPath.Val, c.MTLSKeyPath.Val)
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

func NewGrpc(c *v2.MilvusConfig) (*GrpcClient, error) {
	logger := log.L()

	host := net.JoinHostPort(c.Grpc.Address.Val, strconv.Itoa(c.Grpc.Port.Val))
	logger.Info("New milvus grpc client", zap.String("host", host))

	auth := grpcAuth(c.User.Val, c.Password.Val)

	cerd, err := transCred(&c.Grpc)
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

		user: c.User.Val,
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
	sem := g.parseVersionForFeature(g.serverVersion, ver)

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

// releaseVersion parses ver as an exact MAJOR.MINOR.PATCH release version.
//
// It strips an optional leading "v" (Milvus releases report build tags like "v2.2.16") and
// then uses StrictNewVersion. This deliberately rejects dev build tags like
// "2.6-20260404-31fb3fc" or "master-20260226-abcdef", leaving them to embeddedVersion.
//
// The lenient semver.NewVersion cannot be used here because it parses
// "2.6-20260404-31fb3fc" as "2.6.0-20260404-31fb3fc" — a prerelease of 2.6.0 — which
// compares LESS than 2.6.x and silently disables features on dev builds.
func releaseVersion(ver string) (*semver.Version, bool) {
	v := strings.TrimPrefix(ver, "v")
	// Collapse four-part hotfix versions to their release base, e.g. "2.3.22.6" -> "2.3.22".
	if parts := strings.SplitN(v, ".", 4); len(parts) == 4 {
		v = strings.Join(parts[:3], ".")
	}

	sem, err := semver.StrictNewVersion(v)
	if err != nil {
		return nil, false
	}

	return sem, true
}

// _versionPattern recovers MAJOR.MINOR[.PATCH] from a string that is not a version itself.
// It matches only at the start of the string, for dev build tags like "2.6-20260404-31fb3fc",
// or right after "Milvus", for vendor descriptions like
// "Zilliz Cloud Vector Database(Compatible with Milvus 2.6)". Anchoring both alternatives is
// what keeps an unrelated number elsewhere in a product name from being read as the server
// version.
var _versionPattern = regexp.MustCompile(`(?i)(?:^|milvus[ _-]*)v?(\d+)\.(\d+)(?:\.(\d+))?`)

// embeddedVersion recovers a version embedded in a string that is not a release version
// itself, such as a dev build tag or a vendor product description. A match that stops at
// MAJOR.MINOR resolves to the head of that line, see _headOfLinePatch.
func embeddedVersion(ver string) (*semver.Version, bool) {
	match := _versionPattern.FindStringSubmatch(ver)
	if match == nil {
		return nil, false
	}

	major, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return nil, false
	}
	minor, err := strconv.ParseUint(match[2], 10, 64)
	if err != nil {
		return nil, false
	}

	patch := uint64(_headOfLinePatch)
	if match[3] != "" {
		if patch, err = strconv.ParseUint(match[3], 10, 64); err != nil {
			return nil, false
		}
	}

	return semver.New(major, minor, patch, "", ""), true
}

// parseVersionForFeature picks the version to check feature constraints against out of every
// string the server reports about itself.
//
// Connect and GetVersion answer with the same build tag on open source Milvus, but not on
// hosted ones: Zilliz Cloud answers Connect with a product description that names the Milvus
// line ("... (Compatible with Milvus 2.6)") and GetVersion with an internal build tag that
// carries no version at all ("unknown-20260608-7ea6e3526"). Taking all of them means the one
// source that does know the version is used. An exact release version wins over an embedded
// one no matter which call reported it.
func (g *GrpcClient) parseVersionForFeature(vers ...string) *semver.Version {
	for _, ver := range vers {
		if sem, ok := releaseVersion(ver); ok {
			g.logger.Info("check features against the server release version",
				zap.String("version", ver), zap.String("resolved", sem.String()))
			return sem
		}
	}

	for _, ver := range vers {
		if sem, ok := embeddedVersion(ver); ok {
			g.logger.Info("the server does not report a release version, check features against the version embedded in it",
				zap.String("version", ver), zap.String("resolved", sem.String()))
			return sem
		}
	}

	g.logger.Warn("the server reports no version at all, treat it as the latest dev build",
		zap.Strings("versions", vers))
	return _latestDevVersion
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
			}
			return retry.Unrecoverable(fmt.Errorf("client: create database: %w", err))
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
	//nolint:staticcheck // SA1019: GetErrorCode is needed for backward compatibility with older Milvus versions
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

// HasCollectionByID reports whether the collection with this id is usable on the
// server. DescribeCollection resolves by id when the name is left empty, and the
// server answers from the same view a restore's own writes are checked against:
// a collection that exists but is not in the created state -- one that is still
// being reclaimed after a drop, say -- reads as absent here, which is what the
// caller needs to know.
func (g *GrpcClient) HasCollectionByID(ctx context.Context, collectionID int64) (bool, error) {
	resp, err := g.srv.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionID: collectionID})
	if err != nil {
		return false, fmt.Errorf("client: has collection by id failed: %w", err)
	}
	// Absent is an answer, not a failure: it is what the caller is asking about.
	//nolint:staticcheck // SA1019: GetErrorCode is needed for backward compatibility with older Milvus versions
	if resp.GetStatus().GetErrorCode() == commonpb.ErrorCode_CollectionNotExists ||
		resp.GetStatus().GetCode() == _collectionNotFoundCode {
		return false, nil
	}
	if err := checkResponse(resp, nil); err != nil {
		return false, fmt.Errorf("client: has collection by id failed: %w", err)
	}
	return true, nil
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
	}, retry.Attempts(20))
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

// DropCollectionProperties removes collection-level property overrides, letting each key
// fall back to the target cluster's own default.
func (g *GrpcClient) DropCollectionProperties(ctx context.Context, db, collName string, keys []string) error {
	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.AlterCollectionRequest{CollectionName: collName, DeleteKeys: keys}
	resp, err := g.srv.AlterCollection(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop collection properties failed: %w", err)
	}

	return nil
}

// DropCollectionFieldProperties removes field-level property overrides, letting each key
// fall back to the target cluster's own default.
func (g *GrpcClient) DropCollectionFieldProperties(ctx context.Context, db, collName, fieldName string, keys []string) error {
	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.AlterCollectionFieldRequest{CollectionName: collName, FieldName: fieldName, DeleteKeys: keys}
	resp, err := g.srv.AlterCollectionField(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop collection field properties failed: %w", err)
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
			}
			return retry.Unrecoverable(fmt.Errorf("client: create partition: %w", err))
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
			}
			return retry.Unrecoverable(fmt.Errorf("client: create index: %w", err))
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

// DropIndexProperties removes index-level param overrides, letting each key fall back to
// the target cluster's own default.
func (g *GrpcClient) DropIndexProperties(ctx context.Context, db, collName, indexName string, keys []string) error {
	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.AlterIndexRequest{CollectionName: collName, IndexName: indexName, DeleteKeys: keys}
	resp, err := g.srv.AlterIndex(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop index properties failed: %w", err)
	}
	return nil
}

// GetReplicateConfiguration returns the cross-cluster replication configuration
// the cluster currently holds. A cluster that has never been given one answers
// with an empty configuration rather than an error.
func (g *GrpcClient) GetReplicateConfiguration(ctx context.Context) (*commonpb.ReplicateConfiguration, error) {
	ctx = g.newCtx(ctx)
	resp, err := g.srv.GetReplicateConfiguration(ctx, &milvuspb.GetReplicateConfigurationRequest{})
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: get replicate configuration: %w", err)
	}

	return resp.GetConfiguration(), nil
}

// GetReplicateInfo returns the replication checkpoint the cluster holds for one
// of its OWN pchannels. Passing a pchannel that belongs to another cluster does
// not fail: the lookup blocks until the caller's deadline.
func (g *GrpcClient) GetReplicateInfo(ctx context.Context, sourceClusterID, targetPchannel string) (*milvuspb.GetReplicateInfoResponse, error) {
	ctx = g.newCtx(ctx)
	resp, err := g.srv.GetReplicateInfo(ctx, &milvuspb.GetReplicateInfoRequest{
		SourceClusterId: sourceClusterID,
		TargetPchannel:  targetPchannel,
	})
	if err != nil {
		return nil, fmt.Errorf("client: get replicate info of %s: %w", targetPchannel, err)
	}

	return resp, nil
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
	resp, err := g.srv.ReplicateMessage(ctx, &milvuspb.ReplicateMessageRequest{ChannelName: channelName}) //nolint:staticcheck // SA1019: deprecated CDC API still needed
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

// errSnapshotUnsupported is returned by every snapshot call when the server predates the feature,
// so callers get one recognizable error instead of an opaque "unknown method" from gRPC.
var errSnapshotUnsupported = errors.New("client: the server does not support snapshot")

// CreateSnapshot freezes a point-in-time view of a collection. It only registers metadata; no
// binlog is copied. compactionProtection additionally holds off compaction of the referenced
// segments for that long — pass 0 to leave compaction alone. Protection against GC needs no
// duration: it lasts as long as the snapshot exists.
func (g *GrpcClient) CreateSnapshot(ctx context.Context, db, collName, snapshotName string, compactionProtection time.Duration) error {
	if !g.HasFeature(Snapshot) {
		return errSnapshotUnsupported
	}

	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.CreateSnapshotRequest{
		CollectionName:              collName,
		Name:                        snapshotName,
		CompactionProtectionSeconds: int64(compactionProtection.Seconds()),
	}
	resp, err := g.srv.CreateSnapshot(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: create snapshot failed: %w", err)
	}

	return nil
}

// DropSnapshot deletes a snapshot. The server rejects the call while the snapshot has active
// pins, and an export job holds one until it reaches a terminal state, so drop only once the
// export is done with the snapshot.
func (g *GrpcClient) DropSnapshot(ctx context.Context, db, collName, snapshotName string) error {
	if !g.HasFeature(Snapshot) {
		return errSnapshotUnsupported
	}

	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.DropSnapshotRequest{CollectionName: collName, Name: snapshotName}
	resp, err := g.srv.DropSnapshot(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return fmt.Errorf("client: drop snapshot failed: %w", err)
	}

	return nil
}

// DescribeSnapshot returns the snapshot metadata. The response carries no segment list, but its
// S3Location points at the metadata file that does — the server writes that path for every
// snapshot, not only exported ones.
func (g *GrpcClient) DescribeSnapshot(ctx context.Context, db, collName, snapshotName string) (*milvuspb.DescribeSnapshotResponse, error) {
	if !g.HasFeature(Snapshot) {
		return nil, errSnapshotUnsupported
	}

	ctx = g.newCtxWithDB(ctx, db)
	in := &milvuspb.DescribeSnapshotRequest{CollectionName: collName, Name: snapshotName}
	resp, err := g.srv.DescribeSnapshot(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: describe snapshot failed: %w", err)
	}

	return resp, nil
}

// ExportSnapshotInput describes one snapshot export.
//
// TargetPath is the root prefix the bundle is written under, either an object key in the
// instance bucket or a complete storage URI.
//
// ExternalSpec is an optional storage-config JSON for the target, of which only the extfs
// section is read. Leave it empty to have Milvus write with its own object-storage credential
// and rely on bucket policy to authorize the target. Either way the server rejects a copy that
// crosses providers or endpoints, since it is executed as a provider-side copy.
type ExportSnapshotInput struct {
	DB             string
	CollectionName string
	SnapshotName   string
	TargetPath     string
	ExternalSpec   string
}

// ExportSnapshot copies a snapshot into a self-contained bundle — metadata, segment manifests and
// the data files they reference — and returns the id of the job doing the copying. Nothing has
// been copied when this returns: the call durably accepts the job and nothing more. Poll
// GetExportSnapshotState for progress and for the metadata uri that restore needs.
//
// The server keeps the source snapshot pinned until the job is terminal, so the referenced files
// stay safe from GC for the whole copy window without the caller holding a pin of its own.
func (g *GrpcClient) ExportSnapshot(ctx context.Context, input ExportSnapshotInput) (int64, error) {
	if !g.HasFeature(Snapshot) {
		return 0, errSnapshotUnsupported
	}

	ctx = g.newCtxWithDB(ctx, input.DB)
	in := &milvuspb.ExportSnapshotRequest{
		Name:           input.SnapshotName,
		CollectionName: input.CollectionName,
		TargetS3Path:   input.TargetPath,
		ExternalSpec:   input.ExternalSpec,
	}
	resp, err := g.srv.ExportSnapshot(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return 0, fmt.Errorf("client: export snapshot failed: %w", err)
	}

	return resp.GetJobId(), nil
}

// GetExportSnapshotState reports how an export job is doing. A job that failed comes back through
// the returned info — its state and reason — not as an error; the error return covers the state
// query itself. SnapshotMetadataUri is populated only once the job reaches the completed state,
// and TotalBytes only then accounts for the whole bundle.
func (g *GrpcClient) GetExportSnapshotState(ctx context.Context, jobID int64) (*milvuspb.ExportSnapshotInfo, error) {
	if !g.HasFeature(Snapshot) {
		return nil, errSnapshotUnsupported
	}

	ctx = g.newCtx(ctx)
	in := &milvuspb.GetExportSnapshotStateRequest{JobId: jobID}
	resp, err := g.srv.GetExportSnapshotState(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: get export snapshot state failed: %w", err)
	}

	return resp.GetInfo(), nil
}

// RestoreExternalSnapshotInput describes one restore from an exported bundle.
//
// TargetCollectionName must not already exist in the target db — the restore creates it.
//
// SnapshotMetadataURI must be a complete URI with a scheme and a host; an object key is rejected
// before the server reads anything. Query parameters and fragments are rejected too, so presigned
// and SAS URLs cannot stand in for credentials — use ExternalSpec for those. The path has to keep
// the snapshots/{collectionID}/metadata/{snapshotID}.json shape the bundle was written with,
// because the server derives the bundle root from that anchor.
//
// ExternalSpec carries the source credentials under the same rules as the export side.
type RestoreExternalSnapshotInput struct {
	DB                   string
	TargetCollectionName string
	SnapshotMetadataURI  string
	ExternalSpec         string
}

// RestoreExternalSnapshot restores a collection from a bundle in object storage instead of from
// the target cluster's own snapshot registry, and returns the id of the job doing the work. Like
// the export side it only accepts the job; poll GetRestoreSnapshotState for the outcome.
func (g *GrpcClient) RestoreExternalSnapshot(ctx context.Context, input RestoreExternalSnapshotInput) (int64, error) {
	if !g.HasFeature(Snapshot) {
		return 0, errSnapshotUnsupported
	}

	ctx = g.newCtxWithDB(ctx, input.DB)
	in := &milvuspb.RestoreExternalSnapshotRequest{
		TargetCollectionName: input.TargetCollectionName,
		SnapshotMetadataUri:  input.SnapshotMetadataURI,
		ExternalSpec:         input.ExternalSpec,
	}
	resp, err := g.srv.RestoreExternalSnapshot(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return 0, fmt.Errorf("client: restore external snapshot failed: %w", err)
	}

	return resp.GetJobId(), nil
}

// GetRestoreSnapshotState reports how a restore job is doing. As on the export side, a failed job
// is reported through the returned info rather than as an error.
func (g *GrpcClient) GetRestoreSnapshotState(ctx context.Context, jobID int64) (*milvuspb.RestoreSnapshotInfo, error) {
	if !g.HasFeature(Snapshot) {
		return nil, errSnapshotUnsupported
	}

	ctx = g.newCtx(ctx)
	in := &milvuspb.GetRestoreSnapshotStateRequest{JobId: jobID}
	resp, err := g.srv.GetRestoreSnapshotState(ctx, in)
	if err := checkResponse(resp, err); err != nil {
		return nil, fmt.Errorf("client: get restore snapshot state failed: %w", err)
	}

	return resp.GetInfo(), nil
}
