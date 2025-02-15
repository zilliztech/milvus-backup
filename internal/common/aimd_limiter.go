package common

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/atomic"
)

// AIMDLimiter is an implementation of Additive Increase Multiplicative Decrease algorithm.
// It uses leaky bucket to limit the rate.
type AIMDLimiter struct {
	minRPS float64
	maxRPS float64
	curRPS atomic.Float64

	bucket chan struct{}

	stop chan struct{}
}

func NewAIMDLimiter(minRPS, maxRPS, initRPS float64) *AIMDLimiter {
	a := &AIMDLimiter{
		minRPS: minRPS,
		maxRPS: maxRPS,
		bucket: make(chan struct{}),
		stop:   make(chan struct{}),
	}
	a.curRPS.Store(initRPS)

	go a.loop()

	return a
}

func (a *AIMDLimiter) loop() {
	for {
		every := time.Duration(float64(time.Second) / a.curRPS.Load())
		select {
		case <-time.After(every):
			a.putToken()
		case <-a.stop:
			return
		}
	}
}

// putToken tries to put a token into the bucket.
// It returns true if the token is put successfully.
func (a *AIMDLimiter) putToken() {
	select {
	case a.bucket <- struct{}{}:
	case <-a.stop:
		return
	}
}

func (a *AIMDLimiter) Wait(ctx context.Context) error {
	select {
	case <-a.bucket:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("aimd_limter: context cancelled: %w", ctx.Err())
	}
}

func (a *AIMDLimiter) Success() {
	curRPS := a.curRPS.Load()
	if curRPS >= a.maxRPS {
		return
	}
	if curRPS < 1 {
		a.curRPS.Add(0.1)
	} else {
		a.curRPS.Add(1)
	}
}

func (a *AIMDLimiter) Failure() {
	oldRPS := a.curRPS.Load()
	newRPS := oldRPS / 2
	if newRPS <= a.minRPS {
		newRPS = a.minRPS
	}

	if newRPS == oldRPS {
		return
	}

	a.curRPS.Store(newRPS)
}

func (a *AIMDLimiter) CurRPS() float64 { return a.curRPS.Load() }
func (a *AIMDLimiter) Stop()           { close(a.stop) }
