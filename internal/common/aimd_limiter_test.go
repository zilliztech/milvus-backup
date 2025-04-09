package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAIMDLimiter_CurrentRPS(t *testing.T) {
	limiter := NewAIMDLimiter(1, 10, 5)
	defer limiter.Stop()

	assert.Equal(t, 5.0, limiter.CurRPS())
}

func TestAIMDLimiter_Success(t *testing.T) {
	t.Run("IncreaseRPS", func(t *testing.T) {
		limiter := NewAIMDLimiter(1, 10, 5)
		defer limiter.Stop()

		limiter.Success()
		assert.Equal(t, 6.0, limiter.CurRPS())
	})

	t.Run("MaxRPS", func(t *testing.T) {
		limiter := NewAIMDLimiter(1, 10, 5)
		defer limiter.Stop()

		for i := 0; i < 10; i++ {
			limiter.Success()
		}
		assert.Equal(t, 10.0, limiter.CurRPS())
	})
}

func TestAIMDLimiter_OnFailure(t *testing.T) {
	t.Run("DecreaseRPS", func(t *testing.T) {
		limiter := NewAIMDLimiter(1, 10, 5)
		defer limiter.Stop()

		limiter.Failure()
		assert.Equal(t, 2.5, limiter.CurRPS())
	})

	t.Run("MinRPS", func(t *testing.T) {
		limiter := NewAIMDLimiter(1, 10, 5)
		defer limiter.Stop()

		for i := 0; i < 10; i++ {
			limiter.Failure()
		}
		assert.Equal(t, limiter.CurRPS(), 1.0)
	})
}

func TestAIMDLimiter_Wait(t *testing.T) {
	t.Run("ContextCancelled", func(t *testing.T) {
		limiter := NewAIMDLimiter(1, 10, 5)
		defer limiter.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := limiter.Wait(ctx)
		assert.Error(t, err)
	})
}
