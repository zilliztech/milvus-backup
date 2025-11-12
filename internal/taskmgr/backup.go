package taskmgr

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

type BackupTaskOpt func(task *BackupTask)

func SetBackupExecuting() BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = backuppb.BackupTaskStateCode_BACKUP_EXECUTING
	}
}

func SetBackupFail(err error) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = backuppb.BackupTaskStateCode_BACKUP_FAIL
		task.endTime = time.Now()
		task.errorMessage = err.Error()
	}
}

func SetBackupSuccess() BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = backuppb.BackupTaskStateCode_BACKUP_SUCCESS
		task.endTime = time.Now()
	}
}

func AddBackupCollTask(ns namespace.NS) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.collTask[ns] = newBackupCollTask(ns)
	}
}

func SetBackupCollExecuting(ns namespace.NS) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.BackupTaskStateCode_BACKUP_EXECUTING
	}
}

func SetBackupCollFail(ns namespace.NS, err error) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.BackupTaskStateCode_BACKUP_FAIL
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

		collTask.stateCode = backuppb.BackupTaskStateCode_BACKUP_SUCCESS
		collTask.endTime = time.Now()
	}
}

func SetBackupCollTotalSize(ns namespace.NS, totaSize int64) BackupTaskOpt {
	return func(task *BackupTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.totalSize = totaSize
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

type BackupTask struct {
	mu sync.RWMutex

	id   string
	name string

	stateCode    backuppb.BackupTaskStateCode
	errorMessage string

	startTime time.Time
	endTime   time.Time

	collTask map[namespace.NS]*backupCollTask
}

func newBackupTask(id, name string) *BackupTask {
	return &BackupTask{
		id:        id,
		name:      name,
		stateCode: backuppb.BackupTaskStateCode_BACKUP_INITIAL,
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

	return b.stateCode
}

func (b *BackupTask) ErrorMessage() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.errorMessage
}

func (b *BackupTask) StartTime() time.Time {
	b.mu.RLock()
	b.mu.RUnlock()

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

	if b.stateCode == backuppb.BackupTaskStateCode_BACKUP_SUCCESS {
		return 100
	}

	var backupSize int64
	for _, task := range b.collTask {
		backupSize += task.TotalSize() * int64(task.Progress()) / 100
	}

	totalSize := b.totalSize()
	// avoid divide by zero
	if totalSize == 0 {
		return 1
	}

	progress := int32(float64(backupSize) / float64(totalSize) * 100)
	// don't return zero
	if progress == 0 {
		return 1
	}

	return progress
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

type backupCollTask struct {
	mu sync.RWMutex

	id string

	ns namespace.NS

	stateCode    backuppb.BackupTaskStateCode
	errorMessage string

	totalSize  int64
	backupSize int64

	startTime time.Time
	endTime   time.Time
}

func newBackupCollTask(ns namespace.NS) *backupCollTask {
	return &backupCollTask{
		ns:        ns,
		stateCode: backuppb.BackupTaskStateCode_BACKUP_INITIAL,
	}
}

func (b *backupCollTask) ID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.id
}

func (b *backupCollTask) StateCode() backuppb.BackupTaskStateCode {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.stateCode
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

	if b.stateCode == backuppb.BackupTaskStateCode_BACKUP_SUCCESS {
		return 100
	}
	if b.stateCode == backuppb.BackupTaskStateCode_BACKUP_INITIAL {
		return 0
	}

	// avoid divide by zero
	if b.totalSize == 0 {
		return 0
	}

	return int32(float64(b.backupSize) / float64(b.totalSize) * 100)
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
