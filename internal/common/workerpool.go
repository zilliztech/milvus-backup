package common

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// WorkerPool a pool that can control the total amount and rate of concurrency
type WorkerPool struct {
	job    chan Job
	g      *errgroup.Group
	subCtx context.Context

	workerNum int
	lim       *rate.Limiter
}

type Job func(ctx context.Context) error

// NewWorkerPool build a worker pool, rps 0 is unlimited
func NewWorkerPool(ctx context.Context, workerNum int, rps int32) *WorkerPool {
	g, subCtx := errgroup.WithContext(ctx)

	var lim *rate.Limiter
	if rps != 0 {
		lim = rate.NewLimiter(rate.Every(time.Second/time.Duration(rps)), 1)
	}

	return &WorkerPool{job: make(chan Job), workerNum: workerNum, g: g, lim: lim, subCtx: subCtx}
}

func (p *WorkerPool) Start() {
	for i := 0; i < p.workerNum; i++ {
		p.g.Go(p.work)
	}
}

func (p *WorkerPool) work() error {
	for job := range p.job {
		if p.lim != nil {
			if err := p.lim.Wait(p.subCtx); err != nil {
				return err
			}
		}

		if err := job(p.subCtx); err != nil {
			return err
		}
	}

	return nil
}

func (p *WorkerPool) Submit(job Job) { p.job <- job }
func (p *WorkerPool) Done()          { close(p.job) }
func (p *WorkerPool) Wait() error    { return p.g.Wait() }
