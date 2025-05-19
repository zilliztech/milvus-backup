package taskmgr

import (
	"fmt"
	"sync"
)

var DefaultMgr = NewMgr()

func NewMgr() *Mgr {
	return &Mgr{
		restoreTask: make(map[string]*RestoreTask),
	}
}

type Mgr struct {
	mu sync.RWMutex

	// restoreID -> RestoreTask
	restoreTask map[string]*RestoreTask
}

func (t *Mgr) AddRestoreTask(taskID string, totalSize int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.restoreTask[taskID] = newRestoreTask(taskID, totalSize)
}

func (t *Mgr) UpdateRestoreTask(taskID string, opts ...RestoreTaskOpt) {
	t.mu.Lock()
	task := t.restoreTask[taskID]
	t.mu.Unlock()

	for _, opt := range opts {
		opt(task)
	}
}

func (t *Mgr) GetRestoreTask(taskID string) (RestoreTaskView, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	task, ok := t.restoreTask[taskID]
	if !ok {
		return nil, fmt.Errorf("progress: restore task %s not found", taskID)
	}

	return task, nil
}
