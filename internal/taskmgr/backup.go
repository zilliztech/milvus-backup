package taskmgr

import (
	"fmt"
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

type BackupTaskOpt func(task *BackupTask)

func setBackupState(state backupTaskState) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = state
	}
}

func SetBackupDatabaseExecuting() BackupTaskOpt {
	return setBackupState(_backupTaskSateDatabaseExecuting)
}

func SetBackupCollectionExecuting() BackupTaskOpt {
	return setBackupState(_backupTaskStateCollectionExecuting)
}

func SetBackupFail(err error) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = _backupTaskStateFail
		task.endTime = time.Now()
		task.errorMessage = err.Error()
	}
}

func SetBackupSuccess() BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = _backupTaskStateSuccess
		task.endTime = time.Now()
	}
}

func AddBackupCollTasks(nss []namespace.NS) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		for _, ns := range nss {
			task.collTask[ns] = newBackupCollTask(ns)
		}
	}
}

func setBackupCollState(ns namespace.NS, state backupCollStatus) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = state
	}
}

func SetBackupCollDDLExecuting(ns namespace.NS) BackupTaskOpt {
	return setBackupCollState(ns, _backupCollStatusDDLExecuting)
}

func SetBackupCollDDLDone(ns namespace.NS) BackupTaskOpt {
	return setBackupCollState(ns, _backupCollStatusDDLDone)
}

func SetBackupCollDMLPrepare(ns namespace.NS) BackupTaskOpt {
	return setBackupCollState(ns, _backupCollStatusDMLPrepare)
}

func SetBackupCollDMLExecuting(ns namespace.NS, totalSize int64) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = _backupCollStatusDMLExecuting
		collTask.totalSize = totalSize
	}
}

func SetBackupCollDMLDone(ns namespace.NS) BackupTaskOpt {
	return setBackupCollState(ns, _backupCollStatusDMLDone)
}

func SetBackupCollFail(ns namespace.NS, err error) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = _backupCollStatusFailed
		collTask.endTime = time.Now()
		collTask.errorMessage = err.Error()
	}
}

func SetBackupCollSuccess(ns namespace.NS) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = _backupCollStatusSuccess
		collTask.endTime = time.Now()
	}
}

func IncBackupCollCopiedSize(ns namespace.NS, size int64, _ time.Duration) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.backupSize += size
	}
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

	CollTasks() map[namespace.NS]BackupCollTaskView
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

	collTask map[namespace.NS]*backupCollTask
}

func newBackupTask(id, name string) *BackupTask {
	return &BackupTask{
		id:        id,
		name:      name,
		stateCode: _backupTaskStateInitial,
		startTime: time.Now(),
		collTask:  make(map[namespace.NS]*backupCollTask),
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

func (b *BackupTask) CollTasks() map[namespace.NS]BackupCollTaskView {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// copy and return a new map, to avoid concurrent modification
	tasks := make(map[namespace.NS]BackupCollTaskView, len(b.collTask))
	for ns, task := range b.collTask {
		tasks[ns] = task
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

	ns namespace.NS

	stateCode    backupCollStatus
	errorMessage string

	totalSize  int64
	backupSize int64

	startTime time.Time
	endTime   time.Time
}

func newBackupCollTask(ns namespace.NS) *backupCollTask {
	return &backupCollTask{ns: ns, stateCode: _backupCollStatusPending}
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
		panic(fmt.Sprintf("unknown backup coll %s task state: %d", b.ns.String(), b.stateCode))
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
		panic(fmt.Sprintf("unknown backup coll %s task state: %d", b.ns.String(), b.stateCode))
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
