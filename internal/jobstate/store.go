package jobstate

import (
	"errors"
	"fmt"
	"sync"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

var ErrTaskNotFound = errors.New("task not found")

var Default = sync.OnceValue(NewStore)

func NewStore() *Store {
	return &Store{
		restoreTask:        make(map[string]*RestoreTask),
		migrateTask:        make(map[string]*migrateTask),
		backupTask:         make(map[string]*BackupTask),
		backupNameBackupID: make(map[string]string),
		deleteTask:         make(map[string]*deleteTask),
		deleteNameDeleteID: make(map[string]string),
	}
}

type Store struct {
	mu sync.RWMutex

	// restoreID -> RestoreTask
	restoreTask map[string]*RestoreTask

	// migrateID -> migrateTask
	migrateTask map[string]*migrateTask

	// backupID -> BackupTask
	backupTask map[string]*BackupTask
	// backupName -> backupID
	backupNameBackupID map[string]string

	// deleteID -> deleteTask
	deleteTask map[string]*deleteTask
	// backupName -> deleteID
	deleteNameDeleteID map[string]string
}

func (m *Store) AddRestoreTask(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.restoreTask[taskID] = newRestoreTask(taskID)
}

func (m *Store) UpdateRestoreTask(taskID string, opts ...RestoreTaskOpt) {
	m.mu.RLock()
	task := m.restoreTask[taskID]
	m.mu.RUnlock()

	for _, opt := range opts {
		opt(task)
	}
}

func (m *Store) GetRestoreTask(taskID string) (RestoreTaskView, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.restoreTask[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return task, nil
}

func (m *Store) AddBackupTask(taskID, backupName string) error {
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

func (m *Store) UpdateBackupTask(taskID string, opts ...BackupTaskOpt) {
	m.mu.RLock()
	task := m.backupTask[taskID]
	m.mu.RUnlock()

	for _, opt := range opts {
		opt(task)
	}
}

func (m *Store) GetBackupTask(taskID string) (BackupTaskView, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.backupTask[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return task, nil
}

func (m *Store) GetBackupTaskByName(backupName string) (BackupTaskView, error) {
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
