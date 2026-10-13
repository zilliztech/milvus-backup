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

// BackupCollDDLState is the lifecycle of one collection's schema half:
// describe, indexes and partitions. It settles on its own, independently of
// the data half — the two share no ordering inside a backup job.
type BackupCollDDLState uint32

const (
	BackupCollDDLStatePending BackupCollDDLState = iota
	BackupCollDDLStateExecuting
	BackupCollDDLStateSuccess
	BackupCollDDLStateFail
)

// BackupCollDMLState is the lifecycle of one collection's data half: an
// optional flush, then prepare (segment listing, snapshot creation), then
// the copy itself, then settled.
type BackupCollDMLState uint32

const (
	BackupCollDMLStatePending BackupCollDMLState = iota
	BackupCollDMLStateFlushing
	BackupCollDMLStatePrepare
	BackupCollDMLStateExecuting
	BackupCollDMLStateSuccess
	BackupCollDMLStateFail
)

// BackupUnit is what a collection's TotalSize and CopiedSize count. The
// binlog format copies files through this process and reports bytes; the
// snapshot format asks Milvus to export and reports the export job's file
// count, whose total grows as the export discovers files. Renderers pick
// the display unit and the approximate marker from this, not from the
// numbers.
type BackupUnit uint32

const (
	BackupUnitBytes BackupUnit = iota
	BackupUnitFiles
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

	// Unit is what every collection's TotalSize and CopiedSize count, fixed
	// when the collection phase opens: the format is a job-level resolution,
	// so the unit is uniform across collections.
	Unit BackupUnit

	// Colls holds one entry per collection the job covers. A collection
	// appears the first time one of its tasks creates a tracker, so a job
	// that never runs a data task — meta_only — has entries with no DML
	// half at all. The map is this snapshot's own copy.
	Colls map[collref.Name]BackupCollStatus
}

// Terminal reports whether the job has settled and the status will not
// change anymore. Transports polling for an outcome wait on this.
func (s BackupStatus) Terminal() bool {
	return s.State == BackupStateSuccess || s.State == BackupStateFail
}

// BackupCollStatus is the snapshot of one collection's slice of a backup
// job: two independent halves. A nil half means no task of that kind ever
// registered for the collection — for the DML half that is how a meta_only
// job reads, which is a different thing from a data task that has not started.
type BackupCollStatus struct {
	DDL *BackupCollDDLStatus
	DML *BackupCollDMLStatus
}

// BackupCollDDLStatus is the snapshot of the schema half.
type BackupCollDDLStatus struct {
	State        BackupCollDDLState
	ErrorMessage string

	EndTime time.Time
}

// BackupCollDMLStatus is the snapshot of the data half. TotalSize is what
// the collection has to copy, CopiedSize what is already copied, in the
// job's Unit. StartTime is when the copy phase began; progress renderers
// derive the copy rate from the three.
type BackupCollDMLStatus struct {
	State        BackupCollDMLState
	ErrorMessage string

	StartTime time.Time
	EndTime   time.Time

	TotalSize  int64
	CopiedSize int64
}

type backupTask struct {
	mu sync.RWMutex

	id   string
	name string

	state        BackupState
	errorMessage string

	startTime time.Time
	endTime   time.Time

	unit BackupUnit

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
		Unit:         t.unit,
		Colls:        colls,
	}
}

// backupCollTask is one collection's node in the job. The two halves are
// created independently and guarded by the node's own lock; lock order is
// task first, then node, and a half never locks the task.
type backupCollTask struct {
	mu sync.RWMutex

	ddl *backupCollDDLTask
	dml *backupCollDMLTask
}

func (t *backupCollTask) snapshot() BackupCollStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ddl *BackupCollDDLStatus
	if t.ddl != nil {
		ddl = &BackupCollDDLStatus{
			State:        t.ddl.state,
			ErrorMessage: t.ddl.errorMessage,
			EndTime:      t.ddl.endTime,
		}
	}

	var dml *BackupCollDMLStatus
	if t.dml != nil {
		dml = &BackupCollDMLStatus{
			State:        t.dml.state,
			ErrorMessage: t.dml.errorMessage,
			StartTime:    t.dml.startTime,
			EndTime:      t.dml.endTime,
			TotalSize:    t.dml.totalSize,
			CopiedSize:   t.dml.copiedSize,
		}
	}

	return BackupCollStatus{DDL: ddl, DML: dml}
}

type backupCollDDLTask struct {
	state        BackupCollDDLState
	errorMessage string

	endTime time.Time
}

type backupCollDMLTask struct {
	state        BackupCollDMLState
	errorMessage string

	startTime time.Time
	endTime   time.Time

	totalSize  int64
	copiedSize int64
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

// SetCollectionExecuting opens the collection phase. Unlike the database
// phase it registers nothing: collections appear as their tasks create
// trackers, so a reader sees the phase start before the first collection
// shows up. The unit declares what the collections' size counters will
// count, and is fixed for the whole phase.
func (t *BackupTracker) SetCollectionExecuting(unit BackupUnit) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = BackupStateCollectionExecuting
	t.task.unit = unit
}

// collNode returns the collection's node, creating it on first use: a
// collection is registered exactly by having a task create one of its halves.
func (t *BackupTracker) collNode(collRef collref.Name) *backupCollTask {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	node, ok := t.task.collTask[collRef]
	if !ok {
		node = &backupCollTask{}
		t.task.collTask[collRef] = node
	}

	return node
}

// NewCollDDLTracker hands the schema-half write handle for one collection,
// registering the half on first call: two handles to one collection write the
// same node, which is what lets a plan and its data task settle the same half
// from different places.
func (t *BackupTracker) NewCollDDLTracker(collRef collref.Name) *BackupCollDDLTracker {
	node := t.collNode(collRef)

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.ddl == nil {
		node.ddl = &backupCollDDLTask{state: BackupCollDDLStatePending}
	}

	return &BackupCollDDLTracker{coll: node}
}

// NewCollDMLTracker hands the data-half write handle for one collection,
// under the same rules as NewCollDDLTracker.
func (t *BackupTracker) NewCollDMLTracker(collRef collref.Name) *BackupCollDMLTracker {
	node := t.collNode(collRef)

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.dml == nil {
		node.dml = &backupCollDMLTask{state: BackupCollDMLStatePending}
	}

	return &BackupCollDMLTracker{coll: node}
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

// BackupCollDDLTracker is the write handle for one collection's schema half,
// created by NewCollDDLTracker. It holds the coll node pointer directly: a
// write needs no collection key, and a write for a collection the job does
// not cover cannot be expressed at all.
type BackupCollDDLTracker struct {
	coll *backupCollTask
}

func (t *BackupCollDDLTracker) setState(state BackupCollDDLState) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.state = state
}

func (t *BackupCollDDLTracker) SetExecuting() { t.setState(BackupCollDDLStateExecuting) }

func (t *BackupCollDDLTracker) SetFail(err error) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.state = BackupCollDDLStateFail
	t.coll.ddl.errorMessage = err.Error()
	t.coll.ddl.endTime = time.Now()
}

func (t *BackupCollDDLTracker) SetSuccess() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.state = BackupCollDDLStateSuccess
	t.coll.ddl.endTime = time.Now()
}

// BackupCollDMLTracker is the write handle for one collection's data half,
// created by NewCollDMLTracker under the same rules as the schema half.
type BackupCollDMLTracker struct {
	coll *backupCollTask
}

func (t *BackupCollDMLTracker) setState(state BackupCollDMLState) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.dml.state = state
}

func (t *BackupCollDMLTracker) SetPrepare() { t.setState(BackupCollDMLStatePrepare) }

func (t *BackupCollDMLTracker) SetFlushing() { t.setState(BackupCollDMLStateFlushing) }

// SetExecuting opens the copy phase with its total. The snapshot format
// reports its total repeatedly as the export job discovers files, so the
// start time sticks to the first call: the copy rate a renderer derives
// from it covers the whole phase, not the latest report.
func (t *BackupCollDMLTracker) SetExecuting(totalSize int64) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	if t.coll.dml.startTime.IsZero() {
		t.coll.dml.startTime = time.Now()
	}
	t.coll.dml.state = BackupCollDMLStateExecuting
	t.coll.dml.totalSize = totalSize
}

func (t *BackupCollDMLTracker) SetFail(err error) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.dml.state = BackupCollDMLStateFail
	t.coll.dml.errorMessage = err.Error()
	t.coll.dml.endTime = time.Now()
}

func (t *BackupCollDMLTracker) SetSuccess() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.dml.state = BackupCollDMLStateSuccess
	t.coll.dml.endTime = time.Now()
}

// AddCopiedSize records n copied bytes. Its name takes the verb, not the
// field: callers pass a delta, the tracker owns the accumulation.
func (t *BackupCollDMLTracker) AddCopiedSize(size int64) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.dml.copiedSize += size
}
