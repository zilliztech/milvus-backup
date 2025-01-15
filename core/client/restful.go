package client

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/imroc/req/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

type Restful interface {
	BulkInsert(ctx context.Context, db, collName, partitionName string, files []string, endTime int64, isL0 bool, skipDiskQuotaCheck bool) (string, error)
	GetBulkInsertState(ctx context.Context, db, jobID string) (*GetProcessResp, error)
}

type ImportDetail struct {
	FileName     string    `json:"fileName"`
	FileSize     int       `json:"fileSize"`
	State        string    `json:"state"`
	Progress     int       `json:"progress"`
	CompleteTime time.Time `json:"completeTime"`
	Reason       string    `json:"reason"`
}

type GetProcessResp struct {
	Code int `json:"code"`
	Data struct {
		JobId          string `json:"jobId"`
		CollectionName string `json:"collectionName"`
		FileName       string `json:"fileName"`
		FileSize       int    `json:"fileSize"`
		State          string `json:"state"`
		Progress       int    `json:"progress"`
		CompleteTime   string `json:"completeTime"`
		Reason         string `json:"reason"`
		TotalRows      int    `json:"totalRows"`
		Details        []struct {
			FileName     string `json:"fileName"`
			FileSize     int    `json:"fileSize"`
			State        string `json:"state"`
			Progress     int    `json:"progress"`
			CompleteTime string `json:"completeTime"`
			Reason       string `json:"reason"`
		} `json:"details"`
	} `json:"data"`
}

type GetProgressReq struct {
	DBName string `json:"dbName,omitempty"`
	JobId  string `json:"jobId"`
}

type createImportResp struct {
	Code int `json:"code"`
	Data struct {
		JobId string `json:"jobId"`
	} `json:"data"`
}

type createImportReq struct {
	DbName         string            `json:"dbName,omitempty"`
	CollectionName string            `json:"collectionName"`
	PartitionName  string            `json:"partitionName,omitempty"`
	Files          [][]string        `json:"files"`
	Options        map[string]string `json:"options,omitempty"`
}

type RestfulClient struct {
	cli *req.Client
}

func (r *RestfulClient) BulkInsert(ctx context.Context, db, collName, partitionName string, files []string, endTime int64, isL0 bool, skipDiskQuotaCheck bool) (string, error) {
	opts := make(map[string]string)
	if endTime > 0 {
		opts["end_time"] = strconv.FormatInt(endTime, 10)
	}
	if isL0 {
		opts["l0_import"] = "true"
	} else {
		opts["backup"] = "true"
	}
	opts["skip_disk_quota_check"] = strconv.FormatBool(skipDiskQuotaCheck)

	createReq := createImportReq{
		DbName:         db,
		CollectionName: collName,
		PartitionName:  partitionName,
		Files:          [][]string{files},
		Options:        opts,
	}
	var createResp createImportResp
	log.Info("create import job via restful", zap.Any("createReq", createReq))
	resp, err := r.cli.R().
		SetContext(ctx).
		SetBody(createReq).
		SetSuccessResult(&createResp).
		Post("/v2/vectordb/jobs/import/create")
	if err != nil {
		return "", fmt.Errorf("client: failed to create import job via restful: %w", err)
	}
	log.Info("create import job via restful", zap.Any("createResp", resp))
	if resp.IsErrorState() {
		return "", fmt.Errorf("client: failed to create import job via restful: %v", resp)
	}
	if createResp.Code != 0 {
		return "", fmt.Errorf("client: failed to create import job via restful: %v", createResp)
	}

	return createResp.Data.JobId, nil
}

func (r *RestfulClient) GetBulkInsertState(ctx context.Context, dbName, jobID string) (*GetProcessResp, error) {
	getReq := &GetProgressReq{DBName: dbName, JobId: jobID}

	var getResp GetProcessResp
	resp, err := r.cli.R().
		SetContext(ctx).
		SetBody(getReq).
		SetSuccessResult(&getResp).
		Post("/v2/vectordb/jobs/import/describe")
	if err != nil {
		return nil, fmt.Errorf("client: failed to get import job state via restful: %w", err)
	}
	log.Info("get import job state via restful", zap.Any("getResp", resp))
	if resp.IsErrorState() {
		return nil, fmt.Errorf("client: failed to get import job state via restful: %v", resp)
	}

	return &getResp, nil
}

func NewRestful(cfg *Cfg) (*RestfulClient, error) {
	baseURL, err := cfg.parseRestful()
	if err != nil {
		return nil, fmt.Errorf("client: failed to parse restful address: %w", err)
	}
	cli := req.C().
		SetBaseURL(baseURL.String()).
		SetCommonBearerAuthToken(fmt.Sprintf("%s:%s", cfg.Username, cfg.Password))
	return &RestfulClient{cli: cli}, nil
}
