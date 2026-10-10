package jobstate

import (
	"sync"
	"time"
)

// MigrateState is the lifecycle of one migrate job. It is jobstate's own enum
// rather than anything from the wire format: transports convert at the edge,
// so this package stays free of pb types.
type MigrateState uint32

const (
	MigrateStateInitial MigrateState = iota
	MigrateStateRunning
	MigrateStateSuccess
	MigrateStateFail
)

// MigrateStatus is a point-in-time snapshot of one migrate job, copied out
// under the lock and handed over by value. Readers get dead data: no shared
// locks with the writer, no interface to mock in tests.
type MigrateStatus struct {
	ID string

	State        MigrateState
	ErrorMessage string

	// TotalSize is the backup size recorded in backupinfo. It excludes
	// partition-level L0 segments (partition id == -1) and the meta files,
	// so CopiedSize can run past it near the end; renderers treat it as an
	// approximate denominator.
	TotalSize  int64
	CopiedSize int64

	CopyStarted bool
	CopyDone    bool

	// StartTime is when the copy started, not when the job registered: the
	// volume application ahead of the copy is not upload time. EndTime is
	// when the job settled.
	StartTime time.Time
	EndTime   time.Time

	MigrateJobID string
}

// Terminal reports whether the job has settled and the status will not
// change anymore. Transports polling for an outcome wait on this.
func (s MigrateStatus) Terminal() bool {
	return s.State == MigrateStateSuccess || s.State == MigrateStateFail
}

type migrateTask struct {
	mu sync.RWMutex

	id string

	state        MigrateState
	errorMessage string

	totalSize  int64
	copiedSize int64

	copyStarted bool
	copyDone    bool

	startTime time.Time
	endTime   time.Time

	migrateJobID string
}

func newMigrateTask(id string, totalSize int64) *migrateTask {
	return &migrateTask{id: id, state: MigrateStateInitial, totalSize: totalSize}
}

func (t *migrateTask) snapshot() MigrateStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return MigrateStatus{
		ID:           t.id,
		State:        t.state,
		ErrorMessage: t.errorMessage,
		TotalSize:    t.totalSize,
		CopiedSize:   t.copiedSize,
		CopyStarted:  t.copyStarted,
		CopyDone:     t.copyDone,
		StartTime:    t.startTime,
		EndTime:      t.endTime,
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

func (t *MigrateTracker) SetRunning() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = MigrateStateRunning
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

func (t *MigrateTracker) SetFail(err error) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = MigrateStateFail
	t.task.errorMessage = err.Error()
	t.task.endTime = time.Now()
}

func (t *MigrateTracker) SetSuccess() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = MigrateStateSuccess
	t.task.endTime = time.Now()
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
