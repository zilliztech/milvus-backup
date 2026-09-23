package aimd

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAIMDLimiter_Success(t *testing.T) {
	t.Run("IncreaseRPS", func(t *testing.T) {
		limiter := NewLimiter(1, 10, 5)
		defer limiter.Stop()

		limiter.Success()
		assert.Equal(t, 6.0, limiter.CurRPS())
	})

	t.Run("MaxRPS", func(t *testing.T) {
		limiter := NewLimiter(1, 10, 5)
		defer limiter.Stop()

		for range 10 {
			limiter.Success()
		}
		assert.Equal(t, 10.0, limiter.CurRPS())
	})
}

func TestAIMDLimiter_OnFailure(t *testing.T) {
	t.Run("DecreaseRPS", func(t *testing.T) {
		limiter := NewLimiter(1, 10, 5)
		defer limiter.Stop()

		limiter.Failure()
		assert.Equal(t, 2.5, limiter.CurRPS())
	})

	t.Run("MinRPS", func(t *testing.T) {
		limiter := NewLimiter(1, 10, 5)
		defer limiter.Stop()

		for range 10 {
			limiter.Failure()
		}
		assert.Equal(t, 1.0, limiter.CurRPS())
	})

	t.Run("ConcurrentFailuresAllLand", func(t *testing.T) {
		limiter := NewLimiter(0.5, 100, 64)
		defer limiter.Stop()

		// Without a CAS each concurrent Failure loads the same value and stores
		// half of it, so N concurrent failures would halve only once.
		const failures = 6
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < failures; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				limiter.Failure()
			}()
		}
		close(start)
		wg.Wait()

		assert.Equal(t, 1.0, limiter.CurRPS())
	})
}

func TestAIMDLimiter_Wait(t *testing.T) {
	t.Run("ContextCancelled", func(t *testing.T) {
		limiter := NewLimiter(1, 10, 5)
		defer limiter.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := limiter.Wait(ctx)
		assert.Error(t, err)
	})

	t.Run("Stopped", func(t *testing.T) {
		limiter := NewLimiter(1, 10, 5)
		limiter.Stop()

		err := limiter.Wait(context.Background())
		assert.ErrorIs(t, err, ErrStopped)
	})
}

func TestAIMDLimiter_Stop(t *testing.T) {
	t.Run("Idempotent", func(t *testing.T) {
		limiter := NewLimiter(1, 10, 5)

		assert.NotPanics(t, func() {
			limiter.Stop()
			limiter.Stop()
		})
	})
}
