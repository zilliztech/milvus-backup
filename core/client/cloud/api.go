package cloud

import (
	"context"
	"fmt"
	"strings"

	"github.com/imroc/req/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

type statusCoder interface {
	code() int
}

type common[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}

func (c *common[T]) code() int {
	return c.Code
}

type Credentials struct {
	ExpireTime   string `json:"expireTime"`
	SessionToken string `json:"sessionToken"`
	TmpAK        string `json:"tmpAK"`
	TmpSK        string `json:"tmpSK"`
}

func mask(s string) string {
	if len(s) <= 6 {
		return "****"
	}
	return s[:2] + "****" + s[len(s)-2:]
}

func (cred *Credentials) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("expireTime: %s, ", cred.ExpireTime))
	sb.WriteString(fmt.Sprintf("sessionToken: %s, ", mask(cred.SessionToken)))
	sb.WriteString(fmt.Sprintf("tmpAK: %s, ", mask(cred.TmpAK)))
	sb.WriteString(fmt.Sprintf("tmpSK: %s", mask(cred.TmpSK)))

	return sb.String()
}

type Condition struct {
	MaxContentLength int `json:"maxContentLength"`
}

func (c *Condition) String() string {
	return fmt.Sprintf("maxContentLength: %d", c.MaxContentLength)
}

type ApplyStageResp struct {
	BucketName  string      `json:"bucketName"`
	Cloud       string      `json:"cloud"`
	Condition   Condition   `json:"condition"`
	Credentials Credentials `json:"credentials"`
	Endpoint    string      `json:"endpoint"`
	Region      string      `json:"region"`
	UploadPath  string      `json:"uploadPath"`
}

func (a *ApplyStageResp) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("bucketName: %s, ", mask(a.BucketName)))
	sb.WriteString(fmt.Sprintf("cloud: %s, ", a.Cloud))
	sb.WriteString(fmt.Sprintf("condition: %s, ", a.Condition.String()))
	sb.WriteString(fmt.Sprintf("credentials: %s, ", a.Credentials.String()))
	sb.WriteString(fmt.Sprintf("endpoint: %s, ", a.Endpoint))
	sb.WriteString(fmt.Sprintf("region: %s, ", a.Region))
	sb.WriteString(fmt.Sprintf("uploadPath: %s", mask(a.UploadPath)))

	return sb.String()
}

type Client interface {
	ApplyStage(ctx context.Context, clusterID, dir string) (*ApplyStageResp, error)
	Migrate(ctx context.Context, source Source, dest Destination) (string, error)
}

var _ Client = (*APIClient)(nil)

type APIClient struct {
	cli *req.Client

	logger *zap.Logger
}

func NewClient(address, apikey string) *APIClient {
	cli := req.C().
		SetBaseURL(address).
		SetCommonBearerAuthToken(apikey)
	return &APIClient{cli: cli, logger: log.L()}
}

type applyStageReq struct {
	ClusterID string `json:"clusterId"`
	Path      string `json:"path"`
}

func (a *APIClient) checkResp(httpResp *req.Response, resp statusCoder, err error) error {
	if err != nil {
		return fmt.Errorf("cloud: send reqeust failed: %w", err)
	}
	if httpResp.IsErrorState() {
		return fmt.Errorf("cloud: http resp code is error: %v", httpResp)
	}
	if resp.code() != 0 {
		return fmt.Errorf("cloud: inner resp code is error: %v", resp)
	}

	return nil
}

func (a *APIClient) ApplyStage(ctx context.Context, clusterID, dir string) (*ApplyStageResp, error) {
	in := applyStageReq{ClusterID: clusterID, Path: dir}
	a.logger.Debug("send apply stage request", zap.Any("request", in))

	var applyResp common[ApplyStageResp]
	resp, err := a.cli.R().
		SetContext(ctx).
		SetBody(in).
		SetSuccessResult(&applyResp).
		Post("/v2/stages/apply")
	a.logger.Debug("apply stage response", zap.String("response", applyResp.Data.String()))
	if err := a.checkResp(resp, &applyResp, err); err != nil {
		return nil, fmt.Errorf("cloud: apply stage: %w", err)
	}

	return &applyResp.Data, nil
}

type Source struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	Token     string `json:"token"`

	BucketName string `json:"bucketName"`
	Cloud      string `json:"cloud"`
	Path       string `json:"path"`
	Region     string `json:"region"`
}

func (s *Source) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("accessKey: %s, ", mask(s.AccessKey)))
	sb.WriteString(fmt.Sprintf("secretKey: %s, ", mask(s.SecretKey)))
	sb.WriteString(fmt.Sprintf("token: %s, ", mask(s.Token)))
	sb.WriteString(fmt.Sprintf("bucketName: %s, ", mask(s.BucketName)))
	sb.WriteString(fmt.Sprintf("cloud: %s, ", s.Cloud))
	sb.WriteString(fmt.Sprintf("path: %s, ", mask(s.Path)))
	sb.WriteString(fmt.Sprintf("region: %s", s.Region))

	return sb.String()
}

type Destination struct {
	ClusterID string `json:"clusterId"`
}

func (d *Destination) String() string {
	return fmt.Sprintf("clusterId: %s", d.ClusterID)
}

type Migrate struct {
	Source      Source      `json:"source"`
	Destination Destination `json:"destination"`
}

func (m *Migrate) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("source: %s, ", m.Source.String()))
	sb.WriteString(fmt.Sprintf("destination: %s", m.Destination.String()))

	return sb.String()
}

type MigrateResp struct {
	JobID string `json:"jobId"`
}

func (a *APIClient) Migrate(ctx context.Context, source Source, dest Destination) (string, error) {
	in := Migrate{Source: source, Destination: dest}
	a.logger.Debug("send migrate request", zap.String("request", in.String()))

	var migrateResp common[MigrateResp]
	resp, err := a.cli.R().
		SetContext(ctx).
		SetBody(in).
		SetSuccessResult(&migrateResp).
		Post("/v2/migrations/startByRemote")
	a.logger.Debug("migrate response", zap.Any("response", resp))
	if err := a.checkResp(resp, &migrateResp, err); err != nil {
		return "", fmt.Errorf("cloud: start migrate job: %w", err)
	}

	return migrateResp.Data.JobID, nil
}
