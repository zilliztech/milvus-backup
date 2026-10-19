package tasklet

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Semaphore bounds how many tasks RunConcurrent keeps in flight.
// *semaphore.Weighted satisfies it as-is.
type Semaphore interface {
	Acquire(ctx context.Context, n int64) error
	Release(n int64)
}

// RunConcurrent runs every task in its own goroutine, acquiring one slot per
// task before spawning it, so no more than sem's capacity is in flight. The
// first error cancels the context handed to the remaining tasks; the first
// error raised is the one returned.
func RunConcurrent(ctx context.Context, sem Semaphore, tasks []Tasklet) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("tasklet: acquire semaphore: %w", err)
		}

		g.Go(func() error {
			defer sem.Release(1)

			return task.Execute(subCtx)
		})
	}

	return g.Wait()
}
