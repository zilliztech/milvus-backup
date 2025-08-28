package taskmgr

import (
	"fmt"
	"sync"
)

var DefaultMgr = NewMgr()

func NewMgr() *Mgr {
	return &Mgr{
		restoreTask: make(map[string]*RestoreTask),
		migrateTask: make(map[string]*MigrateTask),
	}
}

type Mgr struct {
	mu sync.RWMutex

	// restoreID -> RestoreTask
	restoreTask map[string]*RestoreTask
	// migrateID -> MigrateTask
	migrateTask map[string]*MigrateTask
}

func (t *Mgr) AddRestoreTask(taskID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.restoreTask[taskID] = newRestoreTask(taskID)
}

func (t *Mgr) UpdateRestoreTask(taskID string, opts ...RestoreTaskOpt) {
	t.mu.RLock()
	task := t.restoreTask[taskID]
	t.mu.RUnlock()

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

func (t *Mgr) AddMigrateTask(taskID string, totalSize int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.migrateTask[taskID] = newMigrateTask(taskID, totalSize)
}

func (t *Mgr) UpdateMigrateTask(taskID string, opts ...MigrateTaskOpt) {
	t.mu.RLock()
	task := t.migrateTask[taskID]
	t.mu.RUnlock()

	for _, opt := range opts {
		opt(task)
	}
}

func (t *Mgr) GetMigrateTask(taskID string) (*MigrateTask, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	task, ok := t.migrateTask[taskID]
	if !ok {
		return nil, fmt.Errorf("progress: migrate task %s not found", taskID)
	}

	return task, nil
}
