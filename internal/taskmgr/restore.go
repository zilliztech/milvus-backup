package taskmgr

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

type RestoreTaskOpt func(task *RestoreTask)

func AddRestoreCollTask(collRef collref.Name, totalSize int64) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.collTask[collRef] = newRestoreCollectionTask(collRef, totalSize)
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

func SetRestoreCollExecuting(collRef collref.Name) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[collRef]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.RestoreTaskStateCode_EXECUTING
	}
}

func SetRestoreCollSuccess(collRef collref.Name) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[collRef]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.RestoreTaskStateCode_SUCCESS
		collTask.endTime = time.Now()
	}
}

func SetRestoreCollFail(collRef collref.Name, err error) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[collRef]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.stateCode = backuppb.RestoreTaskStateCode_FAIL
		collTask.endTime = time.Now()
		collTask.errorMessage = err.Error()
	}
}

func AddRestoreImportJob(collRef collref.Name, jobID string, totalSize int64) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[collRef]

		collTask.mu.Lock()
		defer collTask.mu.Unlock()

		collTask.importJob[jobID] = &importJob{totalSize: totalSize}
	}
}

func UpdateRestoreImportJob(collRef collref.Name, jobID string, progress int) RestoreTaskOpt {
	return func(task *RestoreTask) {
		task.mu.RLock()
		defer task.mu.RUnlock()
		collTask := task.collTask[collRef]

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

	CollTasks() map[collref.Name]RestoreCollTaskView
}

var _ RestoreTaskView = (*RestoreTask)(nil)

type RestoreTask struct {
	mu sync.RWMutex

	id string

	stateCode    backuppb.RestoreTaskStateCode
	errorMessage string

	startTime time.Time
	endTime   time.Time

	collTask map[collref.Name]*restoreCollectionTask
}

func newRestoreTask(id string) *RestoreTask {
	return &RestoreTask{
		id:        id,
		stateCode: backuppb.RestoreTaskStateCode_INITIAL,
		startTime: time.Now(),
		collTask:  make(map[collref.Name]*restoreCollectionTask),
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
	if t.totalSize() == 0 {
		return 1
	}

	progress := int32(float64(restoredSize) / float64(t.totalSize()) * 100)
	// don't return zero,
	if progress == 0 {
		return 1
	}

	return progress
}

func (t *RestoreTask) totalSize() int64 {
	size := int64(0)
	for _, task := range t.collTask {
		size += task.TotalSize()
	}

	return size
}

func (t *RestoreTask) TotalSize() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.totalSize()
}

func (t *RestoreTask) CollTasks() map[collref.Name]RestoreCollTaskView {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// copy and return a new map, to avoid concurrent modification
	tasks := make(map[collref.Name]RestoreCollTaskView, len(t.collTask))
	for collRef, task := range t.collTask {
		tasks[collRef] = task
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

	target collref.Name

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

func newRestoreCollectionTask(collRef collref.Name, totalSize int64) *restoreCollectionTask {
	return &restoreCollectionTask{
		stateCode: backuppb.RestoreTaskStateCode_INITIAL,
		target:    collRef,
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
