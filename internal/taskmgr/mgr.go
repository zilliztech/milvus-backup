package taskmgr

import (
	"errors"
	"fmt"
	"sync"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

var ErrTaskNotFound = errors.New("task not found")

var DefaultMgr = sync.OnceValue(func() *Mgr { return NewMgr() })

func NewMgr() *Mgr {
	return &Mgr{
		restoreTask:        make(map[string]*RestoreTask),
		migrateTask:        make(map[string]*MigrateTask),
		backupTask:         make(map[string]*BackupTask),
		backupNameBackupID: make(map[string]string),
	}
}

type Mgr struct {
	mu sync.RWMutex

	// restoreID -> RestoreTask
	restoreTask map[string]*RestoreTask

	// migrateID -> MigrateTask
	migrateTask map[string]*MigrateTask

	// backupID -> BackupTask
	backupTask map[string]*BackupTask
	// backupName -> backupID
	backupNameBackupID map[string]string
}

func (m *Mgr) AddRestoreTask(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.restoreTask[taskID] = newRestoreTask(taskID)
}

func (m *Mgr) UpdateRestoreTask(taskID string, opts ...RestoreTaskOpt) {
	m.mu.RLock()
	task := m.restoreTask[taskID]
	m.mu.RUnlock()

	for _, opt := range opts {
		opt(task)
	}
}

func (m *Mgr) GetRestoreTask(taskID string) (RestoreTaskView, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.restoreTask[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return task, nil
}

func (m *Mgr) AddMigrateTask(taskID string, totalSize int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.migrateTask[taskID] = newMigrateTask(taskID, totalSize)
}

func (m *Mgr) UpdateMigrateTask(taskID string, opts ...MigrateTaskOpt) {
	m.mu.RLock()
	task := m.migrateTask[taskID]
	m.mu.RUnlock()

	for _, opt := range opts {
		opt(task)
	}
}

func (m *Mgr) GetMigrateTask(taskID string) (*MigrateTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.migrateTask[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return task, nil
}

func (m *Mgr) AddBackupTask(taskID, backupName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existID, ok := m.backupNameBackupID[backupName]
	if !ok {
		m.backupTask[taskID] = newBackupTask(taskID, backupName)
		m.backupNameBackupID[backupName] = taskID
		return nil
	}

	task := m.backupTask[existID]
	if task == nil {
		m.backupTask[taskID] = newBackupTask(taskID, backupName)
		m.backupNameBackupID[backupName] = taskID
		return nil
	}

	switch task.StateCode() {
	case backuppb.BackupTaskStateCode_BACKUP_FAIL,
		backuppb.BackupTaskStateCode_BACKUP_TIMEOUT:
	default:
		return fmt.Errorf("%s (existing task %s)", backupName, existID)
	}

	m.backupTask[taskID] = newBackupTask(taskID, backupName)
	m.backupNameBackupID[backupName] = taskID
	return nil
}

func (m *Mgr) UpdateBackupTask(taskID string, opts ...BackupTaskOpt) {
	m.mu.RLock()
	task := m.backupTask[taskID]
	m.mu.RUnlock()

	for _, opt := range opts {
		opt(task)
	}
}

func (m *Mgr) GetBackupTask(taskID string) (BackupTaskView, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.backupTask[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return task, nil
}

func (m *Mgr) GetBackupTaskByName(backupName string) (BackupTaskView, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	taskID, ok := m.backupNameBackupID[backupName]
	if !ok {
		return nil, ErrTaskNotFound
	}

	task, ok := m.backupTask[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return task, nil
}
