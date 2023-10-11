package common

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/atomic"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// WorkerPool a pool that can control the total amount and rate of concurrency
type WorkerPool struct {
	job    chan JobWithId
	g      *errgroup.Group
	subCtx context.Context

	workerNum int
	lim       *rate.Limiter

	nextId     atomic.Int64
	jobsStatus sync.Map
	jobsError  sync.Map
}

type Job func(ctx context.Context) error

type JobWithId struct {
	job Job
	id  int64
}

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

	return &WorkerPool{job: make(chan JobWithId), workerNum: workerNum, g: g, lim: lim, subCtx: subCtx}, nil
}

func (p *WorkerPool) Start() {
	//p.jobsStatus = make(map[*Job]string)
	//p.jobsError = make(map[*Job]error)
	p.g.Go(p.work)
	p.nextId = atomic.Int64{}
}

func (p *WorkerPool) work() error {
	for j := range p.job {
		jobWithId := j
		p.g.Go(func() error {
			if p.lim != nil {
				if err := p.lim.Wait(p.subCtx); err != nil {
					return fmt.Errorf("workerpool: wait token %w", err)
				}
			}

			if err := jobWithId.job(p.subCtx); err != nil {
				p.jobsError.Store(jobWithId.id, err)
				p.jobsStatus.Store(jobWithId.id, "done")
				return fmt.Errorf("workerpool: execute job %w", err)
			}
			p.jobsStatus.Store(jobWithId.id, "done")
			return nil
		})
	}
	return nil
}

func (p *WorkerPool) Submit(job Job) {
	jobId := p.nextId.Inc()
	p.job <- JobWithId{job: job, id: jobId}
	//p.jobsStatus.Store(jobId, "started")
}
func (p *WorkerPool) Done()       { close(p.job) }
func (p *WorkerPool) Wait() error { return p.g.Wait() }

func (p *WorkerPool) SubmitWithId(job Job) int64 {
	jobId := p.nextId.Inc()
	p.job <- JobWithId{job: job, id: jobId}
	return jobId
}

func (p *WorkerPool) WaitJobs(jobIds []int64) error {
	for {
		var done = true
		var err error = nil
		for _, jobId := range jobIds {
			if value, ok := p.jobsStatus.Load(jobId); ok && value == "done" {
				done = done
			} else {
				done = false
				break
			}

			if jobError, exist := p.jobsError.Load(jobId); exist {
				err = jobError.(error)
				break
			}
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}
