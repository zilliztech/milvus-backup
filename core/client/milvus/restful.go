package milvus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/imroc/req/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

type ImportState string

const (
	ImportStatePending   ImportState = "Pending"
	ImportStateImporting ImportState = "Importing"
	ImportStateCompleted ImportState = "Completed"
	ImportStateFailed    ImportState = "Failed"
)

type RestfulBulkInsertInput struct {
	DB             string
	CollectionName string
	PartitionName  string
	Paths          [][]string // offset 0 is path to insertLog file, offset 1 is path to deleteLog file
	BackupTS       uint64
	IsL0           bool
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
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
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

type getSegmentInfoResp struct {
	Code int `json:"code"`
	Data struct {
		SegmentInfos []SegmentInfo `json:"segmentInfos"`
	} `json:"data"`
}

type getSegmentInfoReq struct {
	DbName       string  `json:"dbName"`
	CollectionID int64   `json:"collectionId"`
	SegmentIDs   []int64 `json:"segmentIds"`
}

type BinlogInfo struct {
	FieldID int64   `json:"fieldID"`
	LogIDs  []int64 `json:"logIDs"`
}

type SegmentInfo struct {
	CollectionID int64        `json:"collectionID"`
	DeltaLogs    []BinlogInfo `json:"deltaLogs"`
	InsertLogs   []BinlogInfo `json:"insertLogs"`
	IsSorted     bool         `json:"isSorted"`
	Level        int          `json:"level"`
	NumRows      int          `json:"numRows"`
	PartitionID  int64        `json:"partitionID"`
	SegmentID    int64        `json:"segmentID"`
	State        int          `json:"state"`
	StatsLogs    []BinlogInfo `json:"statsLogs"`
	VChannel     string       `json:"vChannel"`
}

type Restful interface {
	BulkInsert(ctx context.Context, input RestfulBulkInsertInput) (string, error)
	GetBulkInsertState(ctx context.Context, db, jobID string) (*GetProcessResp, error)
	GetSegmentInfo(ctx context.Context, db string, collID, segID int64) (*SegmentInfo, error)
}

var _ Restful = (*RestfulClient)(nil)

type RestfulClient struct {
	cli *req.Client
}

func (r *RestfulClient) BulkInsert(ctx context.Context, input RestfulBulkInsertInput) (string, error) {
	opts := make(map[string]string)
	if input.BackupTS > 0 {
		opts["end_ts"] = strconv.FormatUint(input.BackupTS, 10)
	}
	if input.IsL0 {
		opts["l0_import"] = "true"
	} else {
		opts["backup"] = "true"
	}
	opts["skip_disk_quota_check"] = "true"

	createReq := createImportReq{
		DbName:         input.DB,
		CollectionName: input.CollectionName,
		PartitionName:  input.PartitionName,
		Files:          input.Paths,
		Options:        opts,
	}
	var createResp createImportResp
	log.Info("send create import job via restful", zap.Any("createReq", createReq))
	resp, err := r.cli.R().
		SetHeader("Request-Timeout", "600").
		SetContext(ctx).
		SetBody(createReq).
		SetSuccessResult(&createResp).
		Post("/v2/vectordb/jobs/import/create")
	if err != nil {
		return "", fmt.Errorf("client: failed to create import job via restful: %w", err)
	}
	log.Info("create import job via restful done", zap.Any("createResp", resp))
	if resp.IsErrorState() {
		return "", fmt.Errorf("client: failed to create import job via restful: %v", resp)
	}
	if createResp.Code != 0 {
		return "", fmt.Errorf("client: failed to create import job via restful: %v", createResp)
	}

	return createResp.Data.JobId, nil
}

func (r *RestfulClient) GetBulkInsertState(ctx context.Context, dbName, jobID string) (*GetProcessResp, error) {
	var getResp GetProcessResp
	getReq := &GetProgressReq{DBName: dbName, JobId: jobID}

	err := retry.Do(ctx, func() error {
		resp, err := r.cli.R().
			SetContext(ctx).
			SetBody(getReq).
			SetSuccessResult(&getResp).
			Post("/v2/vectordb/jobs/import/describe")
		if err != nil {
			return fmt.Errorf("client: failed to get import job state via restful: %w", err)
		}
		log.Debug("get import job state via restful", zap.Any("getResp", resp))
		if resp.IsErrorState() {
			return fmt.Errorf("client: failed to get import job state via restful: %v", resp)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &getResp, nil
}

func (r *RestfulClient) GetSegmentInfo(ctx context.Context, db string, collID, segID int64) (*SegmentInfo, error) {
	getReq := getSegmentInfoReq{DbName: db, CollectionID: collID, SegmentIDs: []int64{segID}}
	var getResp getSegmentInfoResp

	resp, err := r.cli.R().
		SetContext(ctx).
		SetBody(getReq).
		SetSuccessResult(&getResp).
		Post("/v2/vectordb/segments/describe")
	if err != nil {
		return nil, fmt.Errorf("client: failed to get segment info via restful: %w", err)
	}
	log.Debug("get segment info via restful", zap.Any("getResp", resp))
	if resp.IsErrorState() {
		return nil, fmt.Errorf("client: failed to get segment info via restful: %v", resp)
	}

	if len(getResp.Data.SegmentInfos) == 0 {
		return nil, fmt.Errorf("client: no segment info found for segment id %d", segID)
	}
	if getResp.Code != 0 {
		return nil, fmt.Errorf("client: failed to get segment info via restful: %v", getResp)
	}

	return &getResp.Data.SegmentInfos[0], nil
}

func restfulAuth(username, password string) string {
	if username != "" || password != "" {
		return fmt.Sprintf("%s:%s", username, password)
	}

	return ""
}

func NewRestful(cfg *paramtable.MilvusConfig) (*RestfulClient, error) {
	host := net.JoinHostPort(cfg.Address, cfg.Port)
	log.Info("new milvus restful client", zap.String("host", host))

	var baseURL string
	if cfg.TLSMode == 0 {
		baseURL = "http://" + host
	} else if cfg.TLSMode == 1 || cfg.TLSMode == 2 {
		baseURL = "https://" + host
	} else {
		return nil, errors.New("client: invalid tls mode")
	}

	cli := req.C().SetBaseURL(baseURL)

	if auth := restfulAuth(cfg.User, cfg.Password); len(auth) != 0 {
		cli.SetCommonBearerAuthToken(auth)
	}

	return &RestfulClient{cli: cli}, nil
}
