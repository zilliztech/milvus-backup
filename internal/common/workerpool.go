package common

import (
	"context"
	"errors"
	"fmt"
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

	retry int
}

type Job func(ctx context.Context) error

// NewWorkerPool build a worker pool, rps 0 is unlimited
func NewWorkerPool(ctx context.Context, workerNum int, rps int32) (*WorkerPool, error) {
	if workerNum <= 0 {
		return nil, errors.New("workerpool: worker num can not less than 0")
	}
	g, subCtx := errgroup.WithContext(ctx)
	// Including the main worker
	g.SetLimit(workerNum + 1)

	var lim *rate.Limiter
	if rps != 0 {
		lim = rate.NewLimiter(rate.Every(time.Second/time.Duration(rps)), 1)
	}

	return &WorkerPool{job: make(chan Job), workerNum: workerNum, g: g, lim: lim, subCtx: subCtx}, nil
}

func (p *WorkerPool) work() error {
	for job := range p.job {
		p.g.Go(func() error {
			if p.lim != nil {
				if err := p.lim.Wait(p.subCtx); err != nil {
					return fmt.Errorf("workerpool: wait token %w", err)
				}
			}

			var errs error
			if err := job(p.subCtx); err != nil {
				errs = errors.Join(errs, fmt.Errorf("workerpool: execute job %w", err))
				for i := 0; i < p.retry; i++ {
					if err := job(p.subCtx); err != nil {
						errs = errors.Join(errs, fmt.Errorf("workerpool: execute job %w retry cnt: %d", err, i))
						continue
					} else {
						return nil
					}
				}
			}

			return errs
		})
	}
	return nil
}

func (p *WorkerPool) Start()         { p.g.Go(p.work) }
func (p *WorkerPool) Submit(job Job) { p.job <- job }
func (p *WorkerPool) Done()          { close(p.job) }
func (p *WorkerPool) Wait() error    { return p.g.Wait() }
