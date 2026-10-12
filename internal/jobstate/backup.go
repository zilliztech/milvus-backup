package jobstate

import (
	"fmt"
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

// BackupTracker is the write side of one backup job, minted by
// AddBackupTask and held for the job's whole run — the backup counterpart of
// DeleteTracker. It holds the task pointer directly, so a write can never
// land on an unknown id: the store only hands out a tracker for a task it
// just registered. The embedded task is also the read side: its methods are
// the BackupTaskView the get APIs consume, so one handle serves the job's
// Wait and the task tree's progress writes alike.
type BackupTracker struct {
	*BackupTask
}

var _ BackupTaskView = (*BackupTracker)(nil)

// SetFail settles the job as failed. Only the job's own goroutine may call
// it: the terminal state is the settlement of the job, so it is written by
// whoever observes the task answer, in one place.
func (t *BackupTracker) SetFail(err error) {
	task := t.BackupTask
	task.mu.Lock()
	defer task.mu.Unlock()

	task.stateCode = _backupTaskStateFail
	task.endTime = time.Now()
	task.errorMessage = err.Error()
}

// SetSuccess settles the job as successful, under the same single-writer
// rule as SetFail.
func (t *BackupTracker) SetSuccess() {
	task := t.BackupTask
	task.mu.Lock()
	defer task.mu.Unlock()

	task.stateCode = _backupTaskStateSuccess
	task.endTime = time.Now()
}

func (t *BackupTracker) SetDatabaseExecuting() {
	t.setTaskState(_backupTaskSateDatabaseExecuting)
}

func (t *BackupTracker) SetCollectionExecuting() {
	t.setTaskState(_backupTaskStateCollectionExecuting)
}

func (t *BackupTracker) setTaskState(state backupTaskState) {
	task := t.BackupTask
	task.mu.Lock()
	defer task.mu.Unlock()

	task.stateCode = state
}

// AddCollTasks registers the collections the backup covers. It runs before
// any collection work starts, so every later coll-level write finds its
// task.
func (t *BackupTracker) AddCollTasks(collRefs []collref.Name) {
	task := t.BackupTask
	task.mu.Lock()
	defer task.mu.Unlock()

	for _, collRef := range collRefs {
		task.collTask[collRef] = newBackupCollTask(collRef)
	}
}

// coll returns the coll task for collRef. A write for a collection that was
// never registered is a wiring bug in the caller, not a state to record, so
// it panics naming the collection instead of silently losing progress.
func (t *BackupTracker) coll(collRef collref.Name) *backupCollTask {
	t.mu.RLock()
	collTask := t.collTask[collRef]
	t.mu.RUnlock()

	if collTask == nil {
		panic(fmt.Sprintf("jobstate: coll task %s not registered", collRef))
	}
	return collTask
}

func (t *BackupTracker) SetCollDDLExecuting(collRef collref.Name) {
	t.setCollState(collRef, _backupCollStatusDDLExecuting)
}

func (t *BackupTracker) SetCollDDLDone(collRef collref.Name) {
	t.setCollState(collRef, _backupCollStatusDDLDone)
}

func (t *BackupTracker) SetCollDMLPrepare(collRef collref.Name) {
	t.setCollState(collRef, _backupCollStatusDMLPrepare)
}

func (t *BackupTracker) SetCollDMLDone(collRef collref.Name) {
	t.setCollState(collRef, _backupCollStatusDMLDone)
}

func (t *BackupTracker) setCollState(collRef collref.Name, state backupCollStatus) {
	collTask := t.coll(collRef)
	collTask.mu.Lock()
	defer collTask.mu.Unlock()

	collTask.stateCode = state
}

func (t *BackupTracker) SetCollDMLExecuting(collRef collref.Name, totalSize int64) {
	collTask := t.coll(collRef)
	collTask.mu.Lock()
	defer collTask.mu.Unlock()

	collTask.stateCode = _backupCollStatusDMLExecuting
	collTask.totalSize = totalSize
}

func (t *BackupTracker) SetCollFail(collRef collref.Name, err error) {
	collTask := t.coll(collRef)
	collTask.mu.Lock()
	defer collTask.mu.Unlock()

	collTask.stateCode = _backupCollStatusFailed
	collTask.endTime = time.Now()
	collTask.errorMessage = err.Error()
}

func (t *BackupTracker) SetCollSuccess(collRef collref.Name) {
	collTask := t.coll(collRef)
	collTask.mu.Lock()
	defer collTask.mu.Unlock()

	collTask.stateCode = _backupCollStatusSuccess
	collTask.endTime = time.Now()
}

// AddCollCopiedSize records n copied bytes for one collection. Its name
// takes the verb, not the field: callers pass a delta, the tracker owns the
// accumulation.
func (t *BackupTracker) AddCollCopiedSize(collRef collref.Name, size int64) {
	collTask := t.coll(collRef)
	collTask.mu.Lock()
	defer collTask.mu.Unlock()

	collTask.backupSize += size
}

// BackupTaskView is read-only view of backup task.
type BackupTaskView interface {
	ID() string
	Name() string

	StateCode() backuppb.BackupTaskStateCode
	ErrorMessage() string

	StartTime() time.Time
	EndTime() time.Time

	Progress() int32
	TotalSize() int64

	CollTasks() map[collref.Name]BackupCollTaskView
}

var _ BackupTaskView = (*BackupTask)(nil)

type backupTaskState uint32

const (
	_backupTaskStateInitial backupTaskState = iota
	_backupTaskSateDatabaseExecuting
	_backupTaskStateCollectionExecuting
	_backupTaskStateSuccess
	_backupTaskStateFail
)

type BackupTask struct {
	mu sync.RWMutex

	id   string
	name string

	stateCode    backupTaskState
	errorMessage string

	startTime time.Time
	endTime   time.Time

	collTask map[collref.Name]*backupCollTask
}

func newBackupTask(id, name string) *BackupTask {
	return &BackupTask{
		id:        id,
		name:      name,
		stateCode: _backupTaskStateInitial,
		startTime: time.Now(),
		collTask:  make(map[collref.Name]*backupCollTask),
	}
}

func (b *BackupTask) ID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.id
}

func (b *BackupTask) Name() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.name
}

func (b *BackupTask) StateCode() backuppb.BackupTaskStateCode {
	b.mu.RLock()
	defer b.mu.RUnlock()

	switch b.stateCode {
	case _backupTaskStateInitial:
		return backuppb.BackupTaskStateCode_BACKUP_INITIAL
	case _backupTaskSateDatabaseExecuting, _backupTaskStateCollectionExecuting:
		return backuppb.BackupTaskStateCode_BACKUP_EXECUTING
	case _backupTaskStateSuccess:
		return backuppb.BackupTaskStateCode_BACKUP_SUCCESS
	case _backupTaskStateFail:
		return backuppb.BackupTaskStateCode_BACKUP_FAIL
	default:
		panic(fmt.Sprintf("unknown backup task state: %d", b.stateCode))
	}
}

func (b *BackupTask) ErrorMessage() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.errorMessage
}

func (b *BackupTask) StartTime() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.startTime
}

func (b *BackupTask) EndTime() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.endTime
}

func (b *BackupTask) Progress() int32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	switch b.stateCode {
	case _backupTaskStateInitial:
		return 1
	case _backupTaskSateDatabaseExecuting:
		return 5
	case _backupTaskStateCollectionExecuting:
		// map [0, 100] to [5, 100]
		var totalProgress int32
		for _, task := range b.collTask {
			totalProgress += task.Progress()
		}

		return int32(float64(totalProgress)/float64(len(b.collTask))*0.95) + 5

	case _backupTaskStateSuccess:
		return 100
	case _backupTaskStateFail:
		return 0
	default:
		panic(fmt.Sprintf("unknown backup task state: %d", b.stateCode))
	}
}

func (b *BackupTask) totalSize() int64 {
	var size int64
	for _, task := range b.collTask {
		size += task.TotalSize()
	}

	return size
}

func (b *BackupTask) TotalSize() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.totalSize()
}

func (b *BackupTask) CollTasks() map[collref.Name]BackupCollTaskView {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// copy and return a new map, to avoid concurrent modification
	tasks := make(map[collref.Name]BackupCollTaskView, len(b.collTask))
	for collRef, task := range b.collTask {
		tasks[collRef] = task
	}

	return tasks
}

var _ BackupCollTaskView = (*backupCollTask)(nil)

// fsm
type backupCollStatus uint32

const (
	_backupCollStatusPending backupCollStatus = iota
	_backupCollStatusDDLExecuting
	_backupCollStatusDDLDone
	_backupCollStatusDMLPrepare
	_backupCollStatusDMLExecuting
	_backupCollStatusDMLDone
	_backupCollStatusSuccess
	_backupCollStatusFailed
)

type backupCollTask struct {
	mu sync.RWMutex

	id string

	collRef collref.Name

	stateCode    backupCollStatus
	errorMessage string

	totalSize  int64
	backupSize int64

	startTime time.Time
	endTime   time.Time
}

func newBackupCollTask(collRef collref.Name) *backupCollTask {
	return &backupCollTask{collRef: collRef, stateCode: _backupCollStatusPending}
}

func (b *backupCollTask) ID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.id
}

func (b *backupCollTask) StateCode() backuppb.BackupTaskStateCode {
	b.mu.RLock()
	defer b.mu.RUnlock()

	switch b.stateCode {
	case _backupCollStatusPending:
		return backuppb.BackupTaskStateCode_BACKUP_INITIAL
	case _backupCollStatusDDLExecuting,
		_backupCollStatusDDLDone,
		_backupCollStatusDMLPrepare,
		_backupCollStatusDMLExecuting,
		_backupCollStatusDMLDone:
		return backuppb.BackupTaskStateCode_BACKUP_EXECUTING
	case _backupCollStatusSuccess:
		return backuppb.BackupTaskStateCode_BACKUP_SUCCESS
	case _backupCollStatusFailed:
		return backuppb.BackupTaskStateCode_BACKUP_FAIL
	default:
		panic(fmt.Sprintf("unknown backup coll %s task state: %d", b.collRef.String(), b.stateCode))
	}
}

func (b *backupCollTask) ErrorMessage() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.errorMessage
}

func (b *backupCollTask) StartTime() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.startTime
}

func (b *backupCollTask) EndTime() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.endTime
}

func (b *backupCollTask) Progress() int32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	switch b.stateCode {
	case _backupCollStatusPending:
		return 1
	case _backupCollStatusDDLExecuting:
		return 5
	case _backupCollStatusDDLDone:
		return 10
	case _backupCollStatusDMLPrepare:
		return 15
	case _backupCollStatusDMLExecuting:
		// if total size is 0, it means there is no data should be backup, so we just return 15
		if b.totalSize == 0 {
			return 15
		}
		// the range is [15, 100], so we need to map [0, 100] to [15, 100]
		progress := float64(b.backupSize) / float64(b.totalSize) * 100
		mapped := int32(progress*0.85) + 15
		return mapped
	case _backupCollStatusSuccess, _backupCollStatusDMLDone:
		return 100
	case _backupCollStatusFailed:
		return 0
	default:
		panic(fmt.Sprintf("unknown backup coll %s task state: %d", b.collRef.String(), b.stateCode))
	}
}

func (b *backupCollTask) TotalSize() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.totalSize
}

// BackupCollTaskView is read-only view of backup coll task.
type BackupCollTaskView interface {
	ID() string

	StateCode() backuppb.BackupTaskStateCode
	ErrorMessage() string

	StartTime() time.Time
	EndTime() time.Time

	Progress() int32
	TotalSize() int64
}
