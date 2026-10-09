package jobstate

import (
	"fmt"
	"sync"
	"time"
)

// DeleteState is the lifecycle of one delete job. It is jobstate's own enum
// rather than anything from the wire format: transports convert at the edge,
// so this package stays free of pb types.
type DeleteState uint32

const (
	DeleteStateInitial DeleteState = iota
	DeleteStateRunning
	DeleteStateSuccess
	DeleteStateFail
)

// DeleteStatus is a point-in-time snapshot of one delete job, copied out
// under the lock and handed over by value. Readers get dead data: no shared
// locks with the writer, no interface to mock in tests.
type DeleteStatus struct {
	ID   string
	Name string

	State        DeleteState
	ErrorMessage string

	StartTime time.Time
	EndTime   time.Time

	// Discovered counts the objects the lister has found so far and becomes
	// the final total once ListingDone is set. Deleted counts completed
	// deletions. Deleted can briefly exceed Discovered: the lister and the
	// deleter each run their own listing of the prefix, and the two views
	// are not one transactional read.
	Discovered  int64
	Deleted     int64
	ListingDone bool
}

// Terminal reports whether the job has settled and the status will not
// change anymore. Transports polling for an outcome wait on this.
func (s DeleteStatus) Terminal() bool {
	return s.State == DeleteStateSuccess || s.State == DeleteStateFail
}

type deleteTask struct {
	mu sync.RWMutex

	id   string
	name string

	state        DeleteState
	errorMessage string

	startTime time.Time
	endTime   time.Time

	discovered  int64
	deleted     int64
	listingDone bool
}

func newDeleteTask(id, name string) *deleteTask {
	return &deleteTask{
		id:        id,
		name:      name,
		state:     DeleteStateInitial,
		startTime: time.Now(),
	}
}

func (t *deleteTask) snapshot() DeleteStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return DeleteStatus{
		ID:           t.id,
		Name:         t.name,
		State:        t.state,
		ErrorMessage: t.errorMessage,
		StartTime:    t.startTime,
		EndTime:      t.endTime,
		Discovered:   t.discovered,
		Deleted:      t.deleted,
		ListingDone:  t.listingDone,
	}
}

func (t *deleteTask) terminal() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.state == DeleteStateSuccess || t.state == DeleteStateFail
}

// DeleteTracker is the write side of one delete job, minted by AddDeleteTask
// and held by the job for its whole run. It holds the task pointer directly,
// so a write can never land on an unknown id: the store only hands out a
// tracker for a task it just registered.
type DeleteTracker struct {
	task *deleteTask
}

func (t *DeleteTracker) SetRunning() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = DeleteStateRunning
}

// Snapshot reads the tracked task's current state. The tracker is the write
// handle, but the job that holds it is also the one answering Wait, so it
// reads through here instead of round-tripping the store map.
func (t *DeleteTracker) Snapshot() DeleteStatus {
	return t.task.snapshot()
}

func (t *DeleteTracker) AddDiscovered(n int64) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.discovered += n
}

func (t *DeleteTracker) IncDeleted() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.deleted++
}

func (t *DeleteTracker) SetListingDone() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.listingDone = true
}

func (t *DeleteTracker) SetFail(err error) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = DeleteStateFail
	t.task.errorMessage = err.Error()
	t.task.endTime = time.Now()
}

func (t *DeleteTracker) SetSuccess() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = DeleteStateSuccess
	t.task.endTime = time.Now()
}

// AddDeleteTask registers one delete job and returns its tracker. A backup
// name with a delete still in flight is refused, so two jobs cannot shred
// the same directory concurrently; a finished job does not block a retry.
func (s *Store) AddDeleteTask(taskID, backupName string) (*DeleteTracker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existID, ok := s.deleteNameDeleteID[backupName]; ok {
		if exist := s.deleteTask[existID]; exist != nil && !exist.terminal() {
			return nil, fmt.Errorf("%s (existing task %s)", backupName, existID)
		}
	}

	task := newDeleteTask(taskID, backupName)
	s.deleteTask[taskID] = task
	s.deleteNameDeleteID[backupName] = taskID

	return &DeleteTracker{task: task}, nil
}

// GetDeleteTask reads a snapshot of one delete job.
func (s *Store) GetDeleteTask(taskID string) (DeleteStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, ok := s.deleteTask[taskID]
	if !ok {
		return DeleteStatus{}, ErrTaskNotFound
	}

	return task.snapshot(), nil
}
