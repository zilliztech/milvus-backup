package tasklet

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
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
// error raised is the one returned. A failed acquire stops scheduling but
// never abandons tasks already in flight.
func RunConcurrent(ctx context.Context, sem Semaphore, tasks []Tasklet) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		if err := sem.Acquire(ctx, 1); err != nil {
			// Report through the group rather than returning here, so
			// g.Wait drains the tasks already spawned before the call ends.
			g.Go(func() error {
				return fmt.Errorf("tasklet: acquire semaphore: %w", err)
			})
			break
		}

		g.Go(func() error {
			defer sem.Release(1)

			return task.Execute(subCtx)
		})
	}

	return g.Wait()
}

// RunConcurrentLimit is RunConcurrent with a budget no other call shares: it
// mints a semaphore of capacity limit for this call alone. A non-positive
// limit runs unbounded, matching errgroup's SetLimit.
func RunConcurrentLimit(ctx context.Context, limit int, tasks []Tasklet) error {
	if limit <= 0 {
		limit = len(tasks)
	}

	return RunConcurrent(ctx, semaphore.NewWeighted(int64(max(limit, 1))), tasks)
}
