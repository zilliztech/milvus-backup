package jobstate

import (
	"errors"
	"fmt"
	"sync"
)

var ErrTaskNotFound = errors.New("task not found")

var Default = sync.OnceValue(NewStore)

func NewStore() *Store {
	return &Store{
		restoreTask:        make(map[string]*restoreTask),
		migrateTask:        make(map[string]*migrateTask),
		backupTask:         make(map[string]*backupTask),
		backupNameBackupID: make(map[string]string),
		deleteTask:         make(map[string]*deleteTask),
		deleteNameDeleteID: make(map[string]string),
	}
}

type Store struct {
	mu sync.RWMutex

	// restoreID -> restoreTask
	restoreTask map[string]*restoreTask

	// migrateID -> migrateTask
	migrateTask map[string]*migrateTask

	// backupID -> backupTask
	backupTask map[string]*backupTask
	// backupName -> backupID
	backupNameBackupID map[string]string

	// deleteID -> deleteTask
	deleteTask map[string]*deleteTask
	// backupName -> deleteID
	deleteNameDeleteID map[string]string
}

// AddRestoreTask registers one restore job and returns its tracker, the write
// side the job holds for its whole run.
func (m *Store) AddRestoreTask(taskID string) *RestoreTracker {
	m.mu.Lock()
	defer m.mu.Unlock()

	task := newRestoreTask(taskID)
	m.restoreTask[taskID] = task

	return &RestoreTracker{task: task}
}

// GetRestoreTask reads a snapshot of one restore job.
func (m *Store) GetRestoreTask(taskID string) (RestoreStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.restoreTask[taskID]
	if !ok {
		return RestoreStatus{}, ErrTaskNotFound
	}

	return task.snapshot(), nil
}

// AddBackupTask registers one backup job and returns its tracker. A backup
// name with a live job is refused, so two jobs cannot write one artifact
// directory concurrently; a failed job does not block a retry. A succeeded
// one does: the name is taken by its artifact for as long as this process
// remembers it.
func (m *Store) AddBackupTask(taskID, backupName string) (*BackupTracker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existID, ok := m.backupNameBackupID[backupName]; ok {
		if exist := m.backupTask[existID]; exist != nil && exist.snapshot().State != BackupStateFail {
			return nil, fmt.Errorf("%s (existing task %s)", backupName, existID)
		}
	}

	task := newBackupTask(taskID, backupName)
	m.backupTask[taskID] = task
	m.backupNameBackupID[backupName] = taskID

	return &BackupTracker{task: task}, nil
}

// GetBackupTask reads a snapshot of one backup job.
func (m *Store) GetBackupTask(taskID string) (BackupStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.backupTask[taskID]
	if !ok {
		return BackupStatus{}, ErrTaskNotFound
	}

	return task.snapshot(), nil
}

// GetBackupTaskByName reads a snapshot of the job behind one backup name.
func (m *Store) GetBackupTaskByName(backupName string) (BackupStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	taskID, ok := m.backupNameBackupID[backupName]
	if !ok {
		return BackupStatus{}, ErrTaskNotFound
	}

	task, ok := m.backupTask[taskID]
	if !ok {
		return BackupStatus{}, ErrTaskNotFound
	}

	return task.snapshot(), nil
}
