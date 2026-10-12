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

// AddBackupTask registers one backup job and returns its tracker. A backup
// name with a live job is refused, so two jobs cannot write one artifact
// directory concurrently; a failed job does not block a retry.
func (m *Store) AddBackupTask(taskID, backupName string) (*BackupTracker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existID, ok := m.backupNameBackupID[backupName]; ok {
		if exist := m.backupTask[existID]; exist != nil {
			switch exist.StateCode() {
			case backuppb.BackupTaskStateCode_BACKUP_FAIL,
				backuppb.BackupTaskStateCode_BACKUP_TIMEOUT:
			default:
				return nil, fmt.Errorf("%s (existing task %s)", backupName, existID)
			}
		}
	}

	task := newBackupTask(taskID, backupName)
	m.backupTask[taskID] = task
	m.backupNameBackupID[backupName] = taskID

	return &BackupTracker{task}, nil
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
