package taskmgr

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/core/namespace"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type RestoreTaskOpt func(task *RestoreTask)

func AddRestoreCollTask(ns namespace.NS, totalSize int64) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.collTask[ns] = newRestoreCollectionTask(ns, totalSize)
	}
}

func SetRestoreExecuting() RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = backuppb.RestoreTaskStateCode_EXECUTING
	}
}

func SetRestoreSuccess() RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = backuppb.RestoreTaskStateCode_SUCCESS
		task.endTime = time.Now()
	}
}

func SetRestoreFail(err error) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.stateCode = backuppb.RestoreTaskStateCode_FAIL
		task.endTime = time.Now()
		task.errorMessage = err.Error()
	}
}

func SetRestoreCollExecuting(ns namespace.NS) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.RestoreTaskStateCode_EXECUTING
	}
}

func SetRestoreCollSuccess(ns namespace.NS) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.RestoreTaskStateCode_SUCCESS
		collTask.endTime = time.Now()
	}
}

func SetRestoreCollFail(ns namespace.NS, err error) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.RestoreTaskStateCode_FAIL
		collTask.endTime = time.Now()
		collTask.errorMessage = err.Error()
	}
}

func AddRestoreImportJob(ns namespace.NS, jobID string, totalSize int64) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.importJob[jobID] = &importJob{totalSize: totalSize}
	}
}

func UpdateRestoreImportJob(ns namespace.NS, jobID string, progress int) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[ns]

		collTask.mu.RLock()
		defer collTask.mu.RUnlock()
		job := collTask.importJob[jobID]

		job.mu.Lock()
		defer job.mu.Unlock()
		job.progress = progress
	}
}

type RestoreTaskView interface {
	ID() string
	StateCode() backuppb.RestoreTaskStateCode
	ErrorMessage() string

	StartTime() time.Time
	EndTime() time.Time

	Progress() int32
	TotalSize() int64

	CollTasks() map[namespace.NS]RestoreCollTaskView
}

var _ RestoreTaskView = (*RestoreTask)(nil)

type RestoreTask struct {
	mu sync.RWMutex

	id string

	stateCode    backuppb.RestoreTaskStateCode
	errorMessage string

	startTime time.Time
	endTime   time.Time
	totalSize int64

	collTask map[namespace.NS]*restoreCollectionTask
}

func newRestoreTask(id string, totalSize int64) *RestoreTask {
	return &RestoreTask{
		id:        id,
		stateCode: backuppb.RestoreTaskStateCode_INITIAL,
		startTime: time.Now(),
		totalSize: totalSize,
		collTask:  make(map[namespace.NS]*restoreCollectionTask),
	}
}

func (t *RestoreTask) ID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.id
}

func (t *RestoreTask) StateCode() backuppb.RestoreTaskStateCode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.stateCode
}

func (t *RestoreTask) ErrorMessage() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.errorMessage
}

func (t *RestoreTask) StartTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.startTime
}

func (t *RestoreTask) EndTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.endTime
}

func (t *RestoreTask) Progress() int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.stateCode == backuppb.RestoreTaskStateCode_SUCCESS {
		return 100
	}

	var restoredSize int64
	for _, task := range t.collTask {
		restoredSize += task.TotalSize() * int64(task.Progress()) / 100
	}

	// avoid divide by zero
	if t.totalSize == 0 {
		return 1
	}

	progress := int32(float64(restoredSize) / float64(t.totalSize) * 100)
	// don't return zero,
	if progress == 0 {
		return 1
	}

	return progress
}

func (t *RestoreTask) TotalSize() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.totalSize
}

func (t *RestoreTask) CollTasks() map[namespace.NS]RestoreCollTaskView {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// copy and return a new map, to avoid concurrent modification
	tasks := make(map[namespace.NS]RestoreCollTaskView, len(t.collTask))
	for ns, task := range t.collTask {
		tasks[ns] = task
	}

	return tasks
}

// RestoreCollTaskView is read-only view of restore coll task.
type RestoreCollTaskView interface {
	ID() string

	StateCode() backuppb.RestoreTaskStateCode
	ErrorMessage() string

	StartTime() time.Time
	EndTime() time.Time

	Progress() int32
	TotalSize() int64
}

var _ RestoreCollTaskView = (*restoreCollectionTask)(nil)

type restoreCollectionTask struct {
	mu sync.RWMutex

	id string

	targetNS namespace.NS

	stateCode    backuppb.RestoreTaskStateCode
	errorMessage string
	totalSize    int64
	startTime    time.Time
	endTime      time.Time

	// import job id -> import job
	importJob map[string]*importJob
}

func (t *restoreCollectionTask) ID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.id
}

func (t *restoreCollectionTask) StateCode() backuppb.RestoreTaskStateCode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.stateCode
}

func (t *restoreCollectionTask) ErrorMessage() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.errorMessage
}

func (t *restoreCollectionTask) StartTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.startTime
}

func (t *restoreCollectionTask) EndTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.endTime
}

func (t *restoreCollectionTask) Progress() int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.stateCode == backuppb.RestoreTaskStateCode_SUCCESS {
		return 100
	}

	if t.totalSize == 0 {
		return 1
	}

	var restoredSize int64
	for _, job := range t.importJob {
		job.mu.RLock()
		restoredSize += job.totalSize * int64(job.progress) / 100
		job.mu.RUnlock()
	}

	progress := int32(float64(restoredSize) / float64(t.totalSize) * 100)
	// don't return zero
	if progress == 0 {
		progress = 1
	}

	return progress
}

func (t *restoreCollectionTask) TotalSize() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.totalSize
}

func newRestoreCollectionTask(ns namespace.NS, totalSize int64) *restoreCollectionTask {
	return &restoreCollectionTask{
		stateCode: backuppb.RestoreTaskStateCode_INITIAL,
		targetNS:  ns,
		startTime: time.Now(),
		totalSize: totalSize,
		importJob: make(map[string]*importJob),
	}
}

type importJob struct {
	mu sync.RWMutex

	totalSize int64
	progress  int
}
