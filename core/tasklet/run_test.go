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
}
