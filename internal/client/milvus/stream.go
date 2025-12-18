package milvus

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

type Stream interface {
	Send(immutableMessage *commonpb.ImmutableMessage) error
	WaitConfirm()
}

type StreamClient struct {
	sourceClusterID string

	pchClient map[string]*pchClient

	logger *zap.Logger
}

func NewStreamClient(taskID, srcClusterID string, pch []string, grpc Grpc) (*StreamClient, error) {
	pchClients := make(map[string]*pchClient, len(pch))
	for _, p := range pch {
		streamCli, err := grpc.CreateReplicateStream(srcClusterID)
		if err != nil {
			return nil, fmt.Errorf("create replicate stream: %w", err)
		}
		pchClients[p] = newPchClient(taskID, p, streamCli)
	}

	s := &StreamClient{sourceClusterID: srcClusterID, pchClient: pchClients, logger: log.With(zap.String("task_id", taskID))}

	return s, nil
}

func (s *StreamClient) Send(immutableMessage *commonpb.ImmutableMessage) error {
	log.Debug("stream: send message", zap.String("msg", immutableMessage.String()))

	pch := GetPch(immutableMessage)
	if pch == "" {
		return fmt.Errorf("stream: no pch in message")
	}
	cli, ok := s.pchClient[pch]
	if !ok {
		return fmt.Errorf("stream: no pch client for %s", pch)
	}

	req := &milvuspb.ReplicateRequest{
		Request: &milvuspb.ReplicateRequest_ReplicateMessage{
			ReplicateMessage: &milvuspb.ReplicateMessage{
				SourceClusterId: s.sourceClusterID,
				Message:         immutableMessage,
			},
		},
	}

	if err := cli.send(req); err != nil {
		return fmt.Errorf("stream: send message: %w", err)
	}

	return nil
}

func (s *StreamClient) WaitConfirm() {
	for _, cli := range s.pchClient {
		cli.waitConfirm()
	}
}

type pchClient struct {
	cli milvuspb.MilvusService_CreateReplicateStreamClient

	cond        *sync.Cond
	mu          sync.Mutex
	sentTT      uint64
	confirmedTT uint64

	logger *zap.Logger
}

func newPchClient(taskID, pch string, cli milvuspb.MilvusService_CreateReplicateStreamClient) *pchClient {
	p := &pchClient{
		cli:    cli,
		logger: log.With(zap.String("task_id", taskID), zap.String("pch", pch)),
	}
	p.cond = sync.NewCond(&p.mu)

	go p.recvLoop()
	return p
}

func (p *pchClient) send(req *milvuspb.ReplicateRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	sentTT, err := GetTT(req.GetReplicateMessage().GetMessage())
	if err != nil {
		return fmt.Errorf("stream: get tt: %w", err)
	}
	p.sentTT = sentTT

	if err := p.cli.Send(req); err != nil {
		return fmt.Errorf("stream: send message: %w", err)
	}

	return nil
}

func (p *pchClient) recvLoop() {
	for {
		resp, err := p.cli.Recv()
		if err != nil {
			p.logger.Warn("recv from stream error", zap.Error(err))
			return
		}

		confirmTT := resp.GetReplicateConfirmedMessageInfo().GetConfirmedTimeTick()
		p.logger.Info("recv confirm", zap.Uint64("confirmed_tt", confirmTT), zap.Uint64("sent_tt", p.sentTT))
		p.mu.Lock()
		p.confirmedTT = confirmTT
		p.mu.Unlock()
		p.cond.Signal()
	}
}

func (p *pchClient) waitConfirm() {
	p.mu.Lock()

	for p.confirmedTT < p.sentTT {
		p.logger.Info("wait confirm", zap.Uint64("confirmed_tt", p.confirmedTT), zap.Uint64("sent_tt", p.sentTT))
		p.cond.Wait()
	}

	p.mu.Unlock()
}
