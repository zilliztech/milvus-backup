package jobstate

import (
	"sync"
	"time"
)

// MigrateStatus is a point-in-time snapshot of one migrate job, copied out
// under the lock and handed over by value. Readers get dead data: no shared
// locks with the writer, no interface to mock in tests.
type MigrateStatus struct {
	ID string

	// TotalSize is the backup size recorded in backupinfo. It excludes
	// partition-level L0 segments (partition id == -1) and the meta files,
	// so CopiedSize can run past it near the end; renderers treat it as an
	// approximate denominator.
	TotalSize  int64
	CopiedSize int64

	CopyStarted bool
	CopyDone    bool
	StartTime   time.Time

	MigrateJobID string
}

type migrateTask struct {
	mu sync.RWMutex

	id string

	totalSize  int64
	copiedSize int64

	copyStarted bool
	copyDone    bool
	startTime   time.Time

	migrateJobID string
}

func newMigrateTask(id string, totalSize int64) *migrateTask {
	return &migrateTask{id: id, totalSize: totalSize}
}

func (t *migrateTask) snapshot() MigrateStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return MigrateStatus{
		ID:           t.id,
		TotalSize:    t.totalSize,
		CopiedSize:   t.copiedSize,
		CopyStarted:  t.copyStarted,
		CopyDone:     t.copyDone,
		StartTime:    t.startTime,
		MigrateJobID: t.migrateJobID,
	}
}

// MigrateTracker is the write side of one migrate job, minted by
// AddMigrateTask and held by the job for its whole run. It holds the task
// pointer directly, so a write can never land on an unknown id: the store
// only hands out a tracker for a task it just registered.
type MigrateTracker struct {
	task *migrateTask
}

func (t *MigrateTracker) SetCopyStart() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.copyStarted = true
	t.task.startTime = time.Now()
}

func (t *MigrateTracker) IncCopied(n int64) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.copiedSize += n
}

func (t *MigrateTracker) SetCopyDone() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.copyDone = true
}

func (t *MigrateTracker) SetJobID(jobID string) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.migrateJobID = jobID
}

// Snapshot reads the tracked task's current state. The tracker is the write
// handle, but the job that holds it is also the one reporting its outcome, so
// it reads through here instead of round-tripping the store map.
func (t *MigrateTracker) Snapshot() MigrateStatus {
	return t.task.snapshot()
}

// AddMigrateTask registers one migrate job and returns its tracker.
func (s *Store) AddMigrateTask(taskID string, totalSize int64) *MigrateTracker {
	s.mu.Lock()
	defer s.mu.Unlock()

	task := newMigrateTask(taskID, totalSize)
	s.migrateTask[taskID] = task

	return &MigrateTracker{task: task}
}

// GetMigrateTask reads a snapshot of one migrate job.
func (s *Store) GetMigrateTask(taskID string) (MigrateStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, ok := s.migrateTask[taskID]
	if !ok {
		return MigrateStatus{}, ErrTaskNotFound
	}

	return task.snapshot(), nil
}
