package jobstate

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/internal/collref"
)

// BackupState is the lifecycle of one backup job. It is jobstate's own enum
// rather than anything from the wire format: transports convert at the edge,
// so this package stays free of pb types. The two executing phases are the
// job's real stages — database meta first, then collection data — not a
// rendering distinction.
type BackupState uint32

const (
	BackupStateInitial BackupState = iota
	BackupStateDatabaseExecuting
	BackupStateCollectionExecuting
	BackupStateSuccess
	BackupStateFail
)

// BackupCollState is the lifecycle of one collection inside a backup job:
// pending, then schema (DDL), then data (DML), then settled.
type BackupCollState uint32

const (
	BackupCollStatePending BackupCollState = iota
	BackupCollStateDDLExecuting
	BackupCollStateDDLDone
	BackupCollStateDMLPrepare
	BackupCollStateDMLExecuting
	BackupCollStateDMLDone
	BackupCollStateSuccess
	BackupCollStateFail
)

// BackupStatus is a point-in-time snapshot of one backup job, copied out
// under the lock and handed over by value. Readers get dead data: no shared
// locks with the writer, no interface to mock in tests.
type BackupStatus struct {
	ID   string
	Name string

	State        BackupState
	ErrorMessage string

	StartTime time.Time
	EndTime   time.Time

	// Colls holds one entry per collection the job covers, registered when
	// the collection phase starts. The map is this snapshot's own copy.
	Colls map[collref.Name]BackupCollStatus
}

// Terminal reports whether the job has settled and the status will not
// change anymore. Transports polling for an outcome wait on this.
func (s BackupStatus) Terminal() bool {
	return s.State == BackupStateSuccess || s.State == BackupStateFail
}

// BackupCollStatus is the snapshot of one collection's slice of a backup
// job. TotalSize is the bytes the collection has to copy, CopiedSize the
// bytes already copied; progress renderers derive ratios from the two.
type BackupCollStatus struct {
	State        BackupCollState
	ErrorMessage string

	TotalSize  int64
	CopiedSize int64

	EndTime time.Time
}

type backupTask struct {
	mu sync.RWMutex

	id   string
	name string

	state        BackupState
	errorMessage string

	startTime time.Time
	endTime   time.Time

	collTask map[collref.Name]*backupCollTask
}

func newBackupTask(id, name string) *backupTask {
	return &backupTask{
		id:        id,
		name:      name,
		state:     BackupStateInitial,
		startTime: time.Now(),
		collTask:  make(map[collref.Name]*backupCollTask),
	}
}

func (t *backupTask) snapshot() BackupStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	colls := make(map[collref.Name]BackupCollStatus, len(t.collTask))
	for collRef, collTask := range t.collTask {
		colls[collRef] = collTask.snapshot()
	}

	return BackupStatus{
		ID:           t.id,
		Name:         t.name,
		State:        t.state,
		ErrorMessage: t.errorMessage,
		StartTime:    t.startTime,
		EndTime:      t.endTime,
		Colls:        colls,
	}
}

type backupCollTask struct {
	mu sync.RWMutex

	state        BackupCollState
	errorMessage string

	totalSize  int64
	copiedSize int64

	endTime time.Time
}

func (t *backupCollTask) snapshot() BackupCollStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return BackupCollStatus{
		State:        t.state,
		ErrorMessage: t.errorMessage,
		TotalSize:    t.totalSize,
		CopiedSize:   t.copiedSize,
		EndTime:      t.endTime,
	}
}

// BackupTracker is the write side of one backup job, minted by AddBackupTask
// and held by the job for its whole run. It holds the task pointer directly,
// so a write can never land on an unknown id: the store only hands out a
// tracker for a task it just registered.
type BackupTracker struct {
	task *backupTask
}

func (t *BackupTracker) setState(state BackupState) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = state
}

func (t *BackupTracker) SetDatabaseExecuting() {
	t.setState(BackupStateDatabaseExecuting)
}

// StartCollectionPhase opens the collection phase: every covered collection
// is registered as Pending and minted its own write handle, and the job
// flips to executing collections. Holding a handle is what being registered
// means — a collection-level write carries no key, so it cannot miss.
func (t *BackupTracker) StartCollectionPhase(collRefs []collref.Name) map[collref.Name]*BackupCollTracker {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	trackers := make(map[collref.Name]*BackupCollTracker, len(collRefs))
	for _, collRef := range collRefs {
		collTask := &backupCollTask{state: BackupCollStatePending}
		t.task.collTask[collRef] = collTask
		trackers[collRef] = &BackupCollTracker{task: collTask}
	}
	t.task.state = BackupStateCollectionExecuting

	return trackers
}

// SetFail settles the job as failed. Only the job's own goroutine may call
// it: the terminal state is the settlement of the job, so it is written by
// whoever observes the task answer, in one place.
func (t *BackupTracker) SetFail(err error) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = BackupStateFail
	t.task.errorMessage = err.Error()
	t.task.endTime = time.Now()
}

// SetSuccess settles the job as successful, under the same single-writer rule
// as SetFail.
func (t *BackupTracker) SetSuccess() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = BackupStateSuccess
	t.task.endTime = time.Now()
}

// Snapshot reads the tracked task's current state. The tracker is the write
// handle, but the job that holds it is also the one answering Wait, so it
// reads through here instead of round-tripping the store map.
func (t *BackupTracker) Snapshot() BackupStatus {
	return t.task.snapshot()
}

// BackupCollTracker is the write handle for one collection's slice of a
// backup job, minted by StartCollectionPhase. It holds the coll task
// pointer directly, the same way the job tracker holds the task: a write
// needs no collection key, and a write for a collection the job does not
// cover cannot be expressed at all.
type BackupCollTracker struct {
	task *backupCollTask
}

func (t *BackupCollTracker) setState(state BackupCollState) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = state
}

func (t *BackupCollTracker) SetDDLExecuting() { t.setState(BackupCollStateDDLExecuting) }

func (t *BackupCollTracker) SetDDLDone() { t.setState(BackupCollStateDDLDone) }

func (t *BackupCollTracker) SetDMLPrepare() { t.setState(BackupCollStateDMLPrepare) }

func (t *BackupCollTracker) SetDMLExecuting(totalSize int64) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = BackupCollStateDMLExecuting
	t.task.totalSize = totalSize
}

func (t *BackupCollTracker) SetDMLDone() { t.setState(BackupCollStateDMLDone) }

func (t *BackupCollTracker) SetFail(err error) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = BackupCollStateFail
	t.task.errorMessage = err.Error()
	t.task.endTime = time.Now()
}

func (t *BackupCollTracker) SetSuccess() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = BackupCollStateSuccess
	t.task.endTime = time.Now()
}

// AddCopiedSize records n copied bytes. Its name takes the verb, not the
// field: callers pass a delta, the tracker owns the accumulation.
func (t *BackupCollTracker) AddCopiedSize(size int64) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.copiedSize += size
}
