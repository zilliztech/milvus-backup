package core

import "context"

type Task interface {
	// Prepare the task, return the task meta, should not take too long time.
	Prepare(ctx context.Context) error
	// Execute the task, can take long time
	Execute(ctx context.Context) error
}
