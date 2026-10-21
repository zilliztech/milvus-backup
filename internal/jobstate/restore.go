package jobstate

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/internal/collref"
)

// RestoreState is the lifecycle of one restore job, and of one collection as
// derived from its halves. It is jobstate's own enum rather than anything from
// the wire format: transports convert at the edge, so this package stays free
// of pb types.
type RestoreState uint32

const (
	RestoreStateInitial RestoreState = iota
	RestoreStateExecuting
	RestoreStateSuccess
	RestoreStateFail
)

// RestoreDDLState is the lifecycle of one collection's schema half. Only the
// binlog plans run DDL of their own; the snapshot path hands the schema to
// Milvus, so a snapshot collection has no DDL half at all.
type RestoreDDLState uint32

const (
	RestoreDDLPending RestoreDDLState = iota
	RestoreDDLExecuting
	RestoreDDLSuccess
	RestoreDDLFail
)

// RestoreJobState is the lifecycle of one server-side job half: an import job
// on the binlog path, the snapshot restore job on the snapshot path, or the
// data copy on a secondary restore. A job is minted when the plan is built —
// its size is known then — and binds the server-side job id at submit.
type RestoreJobState uint32

const (
	RestoreJobPending RestoreJobState = iota
	RestoreJobRunning
	RestoreJobCompleted
	RestoreJobFailed
)

// RestoreStatus is a point-in-time snapshot of one restore job, copied out
// under the lock and handed over by value. Readers get dead data: no shared
// locks with the writers, no interface to mock in tests.
type RestoreStatus struct {
	ID           string
	State        RestoreState
	ErrorMessage string

	StartTime time.Time
	EndTime   time.Time

	// Colls holds one entry per collection the job covers, registered when the
	// plan is built. The map is this snapshot's own copy.
	Colls map[collref.Name]RestoreCollStatus
}

// Terminal reports whether the job has settled and the status will not change
// anymore. Transports polling for an outcome wait on this.
func (s RestoreStatus) Terminal() bool {
	return s.State == RestoreStateSuccess || s.State == RestoreStateFail
}

// RestoreCollStatus is the snapshot of one collection's slice of a restore
// job: the halves that ran, with nothing aggregated over them. A nil DDL half
// means the plan runs no DDL for the collection — the snapshot path; Jobs
// holds the import jobs, the single snapshot restore job, or the secondary
// copy job, whichever the plan runs. The collection's state and percent are
// renderings over these halves, computed by the transport that answers.
type RestoreCollStatus struct {
	StartTime time.Time
	TotalSize int64

	DDL  *RestoreDDLStatus
	Jobs []RestoreJobStatus
}

// RestoreDDLStatus is the snapshot of the schema half.
type RestoreDDLStatus struct {
	State        RestoreDDLState
	ErrorMessage string

	EndTime time.Time
}

// RestoreJobStatus is the snapshot of one server-side job half.
type RestoreJobStatus struct {
	ID           string
	State        RestoreJobState
	ErrorMessage string

	Size     int64
	Progress int32

	EndTime time.Time
}

type restoreTask struct {
	mu sync.RWMutex

	id string

	state        RestoreState
	errorMessage string

	startTime time.Time
	endTime   time.Time

	collTask map[collref.Name]*restoreCollTask
}

func newRestoreTask(id string) *restoreTask {
	return &restoreTask{
		id:        id,
		state:     RestoreStateInitial,
		startTime: time.Now(),
		collTask:  make(map[collref.Name]*restoreCollTask),
	}
}

func (t *restoreTask) snapshot() RestoreStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	colls := make(map[collref.Name]RestoreCollStatus, len(t.collTask))
	for collRef, coll := range t.collTask {
		colls[collRef] = coll.snapshot()
	}

	return RestoreStatus{
		ID:           t.id,
		State:        t.state,
		ErrorMessage: t.errorMessage,
		StartTime:    t.startTime,
		EndTime:      t.endTime,
		Colls:        colls,
	}
}

// restoreCollTask is one collection's node in the job. Which halves exist is
// the plan's shape: the binlog plans register a DDL half and one job per
// import, the snapshot plan registers its single restore job, a meta_only
// collection is its DDL half alone. All halves are guarded by the node's own
// lock; trackers write through it and never touch the task's lock.
type restoreCollTask struct {
	mu sync.RWMutex

	target    collref.Name
	totalSize int64
	startTime time.Time

	ddl  *restoreCollDDL
	jobs []*restoreJob
}

func (t *restoreCollTask) snapshot() RestoreCollStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	status := RestoreCollStatus{
		StartTime: t.startTime,
		TotalSize: t.totalSize,
	}
	if t.ddl != nil {
		status.DDL = &RestoreDDLStatus{
			State:        t.ddl.state,
			ErrorMessage: t.ddl.errorMessage,
			EndTime:      t.ddl.endTime,
		}
	}
	for _, job := range t.jobs {
		status.Jobs = append(status.Jobs, RestoreJobStatus{
			ID:           job.id,
			State:        job.state,
			ErrorMessage: job.errorMessage,
			Size:         job.size,
			Progress:     job.progress,
			EndTime:      job.endTime,
		})
	}

	return status
}

type restoreCollDDL struct {
	state        RestoreDDLState
	errorMessage string

	endTime time.Time
}

type restoreJob struct {
	id           string
	state        RestoreJobState
	errorMessage string

	size     int64
	progress int32

	endTime time.Time
}

// RestoreTracker is the write side of one restore job, minted by AddRestoreTask
// and held by the job for its whole run. It holds the task pointer directly,
// so a write can never land on an unknown id: the store only hands out a
// tracker for a task it just registered.
type RestoreTracker struct {
	task *restoreTask
}

func (t *RestoreTracker) setState(state RestoreState) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = state
}

func (t *RestoreTracker) SetExecuting() { t.setState(RestoreStateExecuting) }

// SetFail settles the job as failed. Only the job's own goroutine may call it:
// the terminal state is the settlement of the job, so it is written by whoever
// observes the task answer, in one place.
func (t *RestoreTracker) SetFail(err error) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = RestoreStateFail
	t.task.errorMessage = err.Error()
	t.task.endTime = time.Now()
}

// SetSuccess settles the job as successful, under the same single-writer rule
// as SetFail.
func (t *RestoreTracker) SetSuccess() {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = RestoreStateSuccess
	t.task.endTime = time.Now()
}

// NewCollTracker registers one collection of the job and hands its write
// handle. Registration happens when the plan is built: the collections the
// restore will touch are declared before anything runs.
func (t *RestoreTracker) NewCollTracker(collRef collref.Name, totalSize int64) *RestoreCollTracker {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.collTask[collRef] = &restoreCollTask{
		target:    collRef,
		totalSize: totalSize,
		startTime: time.Now(),
	}

	return &RestoreCollTracker{coll: t.task.collTask[collRef]}
}

// Snapshot reads the tracked job's current state. The tracker is the write
// handle, but the job that holds it is also the one answering Wait, so it
// reads through here instead of round-tripping the store map.
func (t *RestoreTracker) Snapshot() RestoreStatus {
	return t.task.snapshot()
}

// RestoreCollTracker is the write handle for one collection's node, minted by
// NewCollTracker. It holds the node pointer directly: a write needs no
// collection key, and a write for a collection the job does not cover cannot
// be expressed at all.
type RestoreCollTracker struct {
	coll *restoreCollTask
}

// DDL hands the schema-half write handle for the collection, registering the
// half on first call: the binlog plans call it at plan build, the snapshot
// plan never does.
func (c *RestoreCollTracker) DDL() *RestoreCollDDLTracker {
	c.coll.mu.Lock()
	defer c.coll.mu.Unlock()

	if c.coll.ddl == nil {
		c.coll.ddl = &restoreCollDDL{state: RestoreDDLPending}
	}

	return &RestoreCollDDLTracker{coll: c.coll}
}

// NewJob mints one server-side job half of the collection: an import job of
// the binlog plans, the restore job of the snapshot plan, or the copy job of
// a secondary restore. Minting happens when the plan is built — the job's
// size is known then; the server-side id is bound at submit.
func (c *RestoreCollTracker) NewJob(size int64) *RestoreJobTracker {
	c.coll.mu.Lock()
	defer c.coll.mu.Unlock()

	job := &restoreJob{state: RestoreJobPending, size: size}
	c.coll.jobs = append(c.coll.jobs, job)

	return &RestoreJobTracker{coll: c.coll, job: job}
}

// RestoreCollDDLTracker is the write handle for one collection's schema half.
type RestoreCollDDLTracker struct {
	coll *restoreCollTask
}

func (t *RestoreCollDDLTracker) setState(state RestoreDDLState) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.state = state
}

func (t *RestoreCollDDLTracker) SetExecuting() { t.setState(RestoreDDLExecuting) }

func (t *RestoreCollDDLTracker) SetSuccess() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.state = RestoreDDLSuccess
	t.coll.ddl.endTime = time.Now()
}

func (t *RestoreCollDDLTracker) SetFail(err error) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.state = RestoreDDLFail
	t.coll.ddl.errorMessage = err.Error()
	t.coll.ddl.endTime = time.Now()
}

// RestoreJobTracker is the write handle for one server-side job half.
type RestoreJobTracker struct {
	coll *restoreCollTask
	job  *restoreJob
}

// Bind records the server-side job id at submit and marks the job running.
// The id is informational — progress and settlement address the half directly.
func (t *RestoreJobTracker) Bind(id string) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.job.id = id
	t.job.state = RestoreJobRunning
}

// SetRunning marks the job in flight without a server-side id: the secondary
// restore's copy half moves the bytes itself, so there is no id to bind.
func (t *RestoreJobTracker) SetRunning() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.job.state = RestoreJobRunning
}

// SetProgress records the job's reported percent.
func (t *RestoreJobTracker) SetProgress(progress int32) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.job.progress = progress
}

func (t *RestoreJobTracker) SetCompleted() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.job.state = RestoreJobCompleted
	t.job.progress = 100
	t.job.endTime = time.Now()
}

func (t *RestoreJobTracker) SetFailed(err error) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.job.state = RestoreJobFailed
	t.job.errorMessage = err.Error()
	t.job.endTime = time.Now()
}
