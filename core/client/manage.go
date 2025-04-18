package client

import (
	"context"
	"fmt"
	"strconv"

	"github.com/imroc/req/v3"
)

type Manage interface {
	PauseGC(ctx context.Context, sec int32) (string, error)
	ResumeGC(ctx context.Context) (string, error)
}

type ManageImpl struct {
	cli *req.Client
}

func NewManage(base string) *ManageImpl {
	cli := req.C().SetBaseURL(base)
	return &ManageImpl{cli: cli}
}

func (m *ManageImpl) PauseGC(ctx context.Context, sec int32) (string, error) {
	resp, err := m.cli.R().SetContext(ctx).
		SetQueryParam("pause_seconds", strconv.Itoa(int(sec))).
		Get("/management/datacoord/garbage_collection/pause")
	if err != nil {
		return "", fmt.Errorf("client: pause gc: %w", err)
	}

	if resp.IsErrorState() {
		return "", fmt.Errorf("client: pause gc: %v", resp)
	}

	return resp.String(), nil
}

func (m *ManageImpl) ResumeGC(ctx context.Context) (string, error) {
	resp, err := m.cli.R().SetContext(ctx).
		Get("/management/datacoord/garbage_collection/resume")
	if err != nil {
		return "", fmt.Errorf("client: resume gc: %w", err)
	}

	if resp.IsErrorState() {
		return "", fmt.Errorf("client: resume gc: %v", resp)
	}

	return resp.String(), nil
}
