package milvus

import (
	"context"
	"fmt"
	"strconv"

	"github.com/imroc/req/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

type Option[T any] func(*T)

func newOption[T any](opts ...Option[T]) *T {
	var opt T
	for _, o := range opts {
		o(&opt)
	}
	return &opt
}

type pauseGCOption struct {
	pauseSeconds int32
	collectionID int64
}

func WithPauseSeconds(sec int32) Option[pauseGCOption] {
	return func(o *pauseGCOption) {
		o.pauseSeconds = sec
	}
}

func WithCollectionID(collID int64) Option[pauseGCOption] {
	return func(o *pauseGCOption) {
		o.collectionID = collID
	}
}

func WithTicket(ticket string) Option[resumeGCOpt] {
	return func(o *resumeGCOpt) {
		o.ticket = ticket
	}
}

type resumeGCOpt struct {
	ticket string
}

type CommonResp[T any] struct {
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type pauseGCResp struct {
	Ticket string `json:"ticket"`
}

type Manage interface {
	// PauseGC pause GC if collectionID is 0, otherwise pause GC for the collection
	// return the ticket id if milvus support collection level GC control.
	PauseGC(ctx context.Context, opts ...Option[pauseGCOption]) (string, error)
	// ResumeGC resume GC
	ResumeGC(ctx context.Context, opts ...Option[resumeGCOpt]) error
	GetEZK(ctx context.Context, dbName string) (string, error)
}

type ManageImpl struct {
	cli *req.Client
}

func NewManage(base string) *ManageImpl {
	cli := req.C().SetBaseURL(base)
	return &ManageImpl{cli: cli}
}

type ezkResp struct {
	EZK string `json:"ezk"`
}

func (m *ManageImpl) GetEZK(ctx context.Context, dbName string) (string, error) {
	var result CommonResp[ezkResp]
	resp, err := m.cli.R().SetContext(ctx).
		SetQueryParam("db_name", dbName).
		SetSuccessResult(&result).
		Get("/management/rootcoord/ez/backup")

	if err != nil {
		return "", fmt.Errorf("client: get ezk: %w", err)
	}

	if resp.IsErrorState() {
		return "", fmt.Errorf("client: get ezk: %v", resp)
	}

	if result.Msg != "OK" {
		return "", fmt.Errorf("client: get ezk: %v", result)
	}

	return result.Data.EZK, nil
}

func (m *ManageImpl) PauseGC(ctx context.Context, opts ...Option[pauseGCOption]) (string, error) {
	opt := newOption(opts...)

	var result CommonResp[pauseGCResp]
	resp, err := m.cli.R().SetContext(ctx).
		SetQueryParam("pause_seconds", strconv.Itoa(int(opt.pauseSeconds))).
		SetQueryParam("collection_id", strconv.FormatInt(opt.collectionID, 10)).
		SetSuccessResult(&result).
		Get("/management/datacoord/garbage_collection/pause")
	if err != nil {
		return "", fmt.Errorf("client: pause gc: %w", err)
	}

	if resp.IsErrorState() {
		return "", fmt.Errorf("client: pause gc: %v", resp)
	}

	return result.Data.Ticket, nil
}

func (m *ManageImpl) ResumeGC(ctx context.Context, opts ...Option[resumeGCOpt]) error {
	opt := newOption(opts...)

	r := m.cli.R().SetContext(ctx)
	if opt.ticket != "" {
		r.SetQueryParam("ticket", opt.ticket)
	}

	resp, err := r.
		SetSuccessResult(&CommonResp[string]{}).
		Get("/management/datacoord/garbage_collection/resume")
	if err != nil {
		return fmt.Errorf("client: resume gc: %w", err)
	}
	log.Debug("resume gc resp", zap.String("resp", resp.String()))

	if resp.IsErrorState() {
		return fmt.Errorf("client: resume gc: %v", resp)
	}

	return nil
}
