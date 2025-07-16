package core

import "context"

type Task interface {
	// Execute the task, can take long time
	Execute(ctx context.Context) error
}
