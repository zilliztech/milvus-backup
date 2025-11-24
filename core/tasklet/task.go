package tasklet

import "context"

type Tasklet interface {
	// Execute the task, can take long time
	Execute(ctx context.Context) error
}
