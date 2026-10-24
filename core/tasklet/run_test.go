package tasklet

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

type funcTask func(ctx context.Context) error

func (f funcTask) Execute(ctx context.Context) error { return f(ctx) }

func TestRunConcurrent(t *testing.T) {
	t.Run("CapsInFlightAtSemaphoreCapacity", func(t *testing.T) {
		sem := semaphore.NewWeighted(2)

		var inFlight, maxInFlight atomic.Int64
		tasks := make([]Tasklet, 0, 5)
		for range 5 {
			tasks = append(tasks, funcTask(func(ctx context.Context) error {
				n := inFlight.Add(1)
				for {
					m := maxInFlight.Load()
					if n <= m || maxInFlight.CompareAndSwap(m, n) {
						break
					}
				}
				time.Sleep(time.Hour)
				inFlight.Add(-1)
				return nil
			}))
		}

		synctest.Test(t, func(t *testing.T) {
			assert.NoError(t, RunConcurrent(context.Background(), sem, tasks))
		})

		assert.EqualValues(t, 2, maxInFlight.Load())
	})

	t.Run("ReturnsFirstErrorAndReleasesAllSlots", func(t *testing.T) {
		sem := semaphore.NewWeighted(2)
		sentinel := errors.New("boom")

		var canceledObserved atomic.Bool
		tasks := []Tasklet{
			funcTask(func(ctx context.Context) error { return sentinel }),
			funcTask(func(ctx context.Context) error {
				<-ctx.Done()
				canceledObserved.Store(true)
				return ctx.Err()
			}),
			funcTask(func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}),
		}

		synctest.Test(t, func(t *testing.T) {
			err := RunConcurrent(context.Background(), sem, tasks)
			assert.ErrorIs(t, err, sentinel)
		})

		assert.True(t, canceledObserved.Load())
		// every slot came back: the full capacity is acquirable again
		assert.True(t, sem.TryAcquire(2))
	})

	t.Run("CanceledContextFailsAcquire", func(t *testing.T) {
		sem := semaphore.NewWeighted(1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := RunConcurrent(ctx, sem, []Tasklet{funcTask(func(ctx context.Context) error { return nil })})

		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("AcquireFailureJoinsInFlightTasks", func(t *testing.T) {
		sem := semaphore.NewWeighted(1)
		ctx, cancel := context.WithCancel(context.Background())

		started := make(chan struct{})
		var finished atomic.Bool
		tasks := []Tasklet{
			funcTask(func(ctx context.Context) error {
				close(started)
				time.Sleep(time.Hour) // outlives the acquire failure
				finished.Store(true)
				return nil
			}),
			funcTask(func(ctx context.Context) error {
				t.Error("the second task must never be spawned")
				return nil
			}),
		}

		synctest.Test(t, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- RunConcurrent(ctx, sem, tasks) }()

			<-started
			cancel() // the Acquire queued behind the in-flight task now fails

			select {
			case err := <-done:
				assert.Failf(t, "RunConcurrent returned while a task was still in flight", "err: %v", err)
				return
			case <-time.After(time.Millisecond):
				// the runner is still parked waiting for the in-flight task
			}

			err := <-done // virtual time advances past the task's sleep
			assert.ErrorIs(t, err, context.Canceled)
		})

		// the in-flight task ran to completion and gave its slot back
		assert.True(t, finished.Load())
		assert.True(t, sem.TryAcquire(1))
	})
}

func TestRunConcurrentLimit(t *testing.T) {
	newBlockingTasks := func(n int, inFlight, maxInFlight *atomic.Int64) []Tasklet {
		tasks := make([]Tasklet, 0, n)
		for range n {
			tasks = append(tasks, funcTask(func(ctx context.Context) error {
				cur := inFlight.Add(1)
				for {
					peak := maxInFlight.Load()
					if cur <= peak || maxInFlight.CompareAndSwap(peak, cur) {
						break
					}
				}
				time.Sleep(time.Hour)
				inFlight.Add(-1)
				return nil
			}))
		}

		return tasks
	}

	t.Run("CapsInFlightAtLimit", func(t *testing.T) {
		var inFlight, maxInFlight atomic.Int64
		tasks := newBlockingTasks(5, &inFlight, &maxInFlight)

		synctest.Test(t, func(t *testing.T) {
			assert.NoError(t, RunConcurrentLimit(context.Background(), 2, tasks))
		})

		assert.EqualValues(t, 2, maxInFlight.Load())
	})

	t.Run("NonPositiveLimitRunsUnbounded", func(t *testing.T) {
		var inFlight, maxInFlight atomic.Int64
		tasks := newBlockingTasks(3, &inFlight, &maxInFlight)

		synctest.Test(t, func(t *testing.T) {
			assert.NoError(t, RunConcurrentLimit(context.Background(), 0, tasks))
		})

		assert.EqualValues(t, 3, maxInFlight.Load())
	})
}
