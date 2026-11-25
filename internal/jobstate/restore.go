package jobstate

import (
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
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
	RestoreStateFailedCleanupPending
	RestoreStateFailedCleaned
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
	EnableResume bool
	ID           string
	State        RestoreState
	ErrorMessage string

	StartTime time.Time
	EndTime   time.Time

	// Colls holds one entry per collection the job covers, registered when the
	// plan is built. The map is this snapshot's own copy.
	Colls map[collref.Name]RestoreCollStatus
}

// Clone gives the caller its own collection progress and job slices.
func (s RestoreStatus) Clone() RestoreStatus {
	s.Colls = maps.Clone(s.Colls)
	for name, coll := range s.Colls {
		if coll.DDL != nil {
			ddl := *coll.DDL
			coll.DDL = &ddl
		}
		coll.Jobs = slices.Clone(coll.Jobs)
		s.Colls[name] = coll
	}
	return s
}

// Terminal reports whether execution has settled. Failed jobs may resume.
func (s RestoreStatus) Terminal() bool {
	return s.State == RestoreStateSuccess || s.Failed()
}

func (s RestoreStatus) Failed() bool {
	return s.State == RestoreStateFail || s.State == RestoreStateFailedCleanupPending || s.State == RestoreStateFailedCleaned
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
	persistence       restorePersistence
	enableResume      bool // Fixed at registration.
	plan              *restoreplan.Plan
	secondaryProgress *SecondaryProgress
	cleaning          bool

	mu sync.RWMutex

	id string

	state        RestoreState
	errorMessage string

	startTime time.Time
	endTime   time.Time

	collTask map[collref.Name]*restoreCollTask
}

func newRestoreTask(id string, enableResume bool) *restoreTask {
	return &restoreTask{
		id:           id,
		enableResume: enableResume,
		state:        RestoreStateInitial,
		startTime:    time.Now(),
		collTask:     make(map[collref.Name]*restoreCollTask),
	}
}

func (t *restoreTask) snapshot() RestoreStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.snapshotLocked()
}

func (t *restoreTask) snapshotLocked() RestoreStatus {
	colls := make(map[collref.Name]RestoreCollStatus, len(t.collTask))
	for collRef, coll := range t.collTask {
		colls[collRef] = coll.snapshot()
	}

	return RestoreStatus{
		EnableResume: t.enableResume,
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
	step                 int
	collectionID         int64
	createTargetPrepared bool // The target was confirmed absent or successfully dropped.
	state                RestoreDDLState
	errorMessage         string

	endTime time.Time
}

type restoreJob struct {
	id           string
	state        RestoreJobState
	errorMessage string

	size     int64
	progress int32

	endTime time.Time

	twoPCProgress import2PCProgress

	binlogImport    *binlogImportState
	snapshotRestore *snapshotRestoreState
}

type import2PCProgress struct {
	commitRequested  bool
	primaryCompleted bool
	completedMirrors map[string]bool
}

type binlogImportState struct {
	batch      *restoreplan.BinlogImportBatch
	stagingDir string
}

type snapshotRestoreState struct {
	targetPrepared bool
	dataRestored   bool
}

// RestoreTracker is the write side of one restore job, minted by AddRestoreTask
// and held by the job for its whole run. It holds the task pointer directly,
// so a write can never land on an unknown id: the store only hands out a
// tracker for a task it just registered.
type RestoreTracker struct {
	task *restoreTask
}

// ResumeEnabled reports the task's immutable Resume setting.
func (t *RestoreTracker) ResumeEnabled() bool { return t.task.enableResume }

func (t *RestoreTracker) setState(state RestoreState) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()

	t.task.state = state
}

func (t *RestoreTracker) SetExecuting() { t.setState(RestoreStateExecuting) }

// Finish records the outcome and captures this execution's response before
// another caller can resume or clean up the job.
func (t *RestoreTracker) Finish(err error) RestoreStatus {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()
	state := RestoreStateSuccess
	if err != nil {
		state = RestoreStateFail
	}
	t.task.finishLocked(state, err)
	return t.task.snapshotLocked()
}

func (t *restoreTask) finishLocked(state RestoreState, err error) {
	t.state = state
	t.endTime = time.Now()
	switch state {
	case RestoreStateFail:
		if err != nil {
			t.errorMessage = err.Error()
		} else {
			t.errorMessage = "restore failed without an error reason"
		}
		if t.plan == nil {
			t.releasePlanLocked()
		}
	case RestoreStateSuccess:
		t.errorMessage = ""
		t.releasePlanLocked()
	}
	t.persistTerminalLocked()
}

func (t *restoreTask) releasePlanLocked() {
	t.plan = nil
	t.secondaryProgress = nil
	for _, coll := range t.collTask {
		coll.mu.Lock()
		for _, job := range coll.jobs {
			if job.binlogImport != nil {
				job.binlogImport.batch = nil
			}
			job.twoPCProgress.completedMirrors = nil
		}
		coll.mu.Unlock()
	}
}

func (t *RestoreTracker) settle(state RestoreState, err error) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()
	t.task.finishLocked(state, err)
}

func (t *RestoreTracker) SetFail(err error) { t.settle(RestoreStateFail, err) }
func (t *RestoreTracker) SetSuccess()       { t.settle(RestoreStateSuccess, nil) }

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

	return &RestoreCollTracker{task: t.task, coll: t.task.collTask[collRef]}
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
	task *restoreTask
	coll *restoreCollTask
}

func (c *RestoreCollTracker) ResumeEnabled() bool { return c.task.enableResume }

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

// NewJob registers a collection job that only needs common progress tracking.
func (c *RestoreCollTracker) NewJob(size int64) *RestoreJobTracker {
	return c.newJob(&restoreJob{size: size})
}

func (c *RestoreCollTracker) NewSnapshotRestoreJob(size int64) *RestoreJobTracker {
	return c.newJob(&restoreJob{size: size, snapshotRestore: &snapshotRestoreState{}})
}

// BinlogImport pairs a job tracker with its stored binlog batch.
type BinlogImport struct {
	Tracker *RestoreJobTracker
	Batch   restoreplan.BinlogImportBatch
}

// NewBinlogImport registers the batch together with its progress.
func (c *RestoreCollTracker) NewBinlogImport(batch restoreplan.BinlogImportBatch) BinlogImport {
	var size int64
	for _, dir := range batch.Dirs {
		size += dir.Size
	}
	tracker := c.newJob(&restoreJob{size: size, binlogImport: &binlogImportState{batch: &batch}})
	return BinlogImport{Tracker: tracker, Batch: batch}
}

func (c *RestoreCollTracker) newJob(job *restoreJob) *RestoreJobTracker {
	c.coll.mu.Lock()
	defer c.coll.mu.Unlock()

	job.state = RestoreJobPending
	c.coll.jobs = append(c.coll.jobs, job)

	return &RestoreJobTracker{coll: c.coll, job: job}
}

// RestoreCollDDLTracker is the write handle for one collection's schema half.
type RestoreCollDDLTracker struct {
	coll *restoreCollTask
}

func (t *RestoreCollDDLTracker) State() RestoreDDLState {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.coll.ddl.state
}

func (t *RestoreCollDDLTracker) setState(state RestoreDDLState) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.state = state
	t.coll.ddl.errorMessage = ""
	t.coll.ddl.endTime = time.Time{}
}

func (t *RestoreCollDDLTracker) SetExecuting() { t.setState(RestoreDDLExecuting) }

func (t *RestoreCollDDLTracker) SetSuccess() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	t.coll.ddl.errorMessage = ""
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

// Current returns the submission to adopt and whether all local work is done.
func (t *RestoreJobTracker) Current() (string, bool) {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.job.id, t.job.state == RestoreJobCompleted
}

// Bind records the server-side job id at submit and marks the job running.
// The id is informational — progress and settlement address the half directly.
func (t *RestoreJobTracker) Bind(id string) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()

	if t.job.id != id {
		t.job.progress = 0
		t.job.twoPCProgress = import2PCProgress{}
	}
	t.job.id = id
	t.job.errorMessage = ""
	t.job.endTime = time.Time{}
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

	t.job.errorMessage = ""
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

// TrackerForCollection returns the tracker for a collection registered when the plan was built.
func (t *RestoreTracker) TrackerForCollection(target collref.Name) *RestoreCollTracker {
	t.task.mu.RLock()
	defer t.task.mu.RUnlock()
	return &RestoreCollTracker{task: t.task, coll: t.task.collTask[target]}
}

// TrackersForBinlogImports returns the trackers and saved batches for binlog imports.
func (c *RestoreCollTracker) TrackersForBinlogImports() []BinlogImport {
	c.coll.mu.RLock()
	defer c.coll.mu.RUnlock()
	var imports []BinlogImport
	for _, job := range c.coll.jobs {
		if job.binlogImport != nil && job.binlogImport.batch != nil {
			imports = append(imports, BinlogImport{Tracker: &RestoreJobTracker{coll: c.coll, job: job}, Batch: *job.binlogImport.batch})
		}
	}
	return imports
}

// TrackerForSingleJob returns the collection's single job for a snapshot or standalone secondary restore.
func (c *RestoreCollTracker) TrackerForSingleJob() *RestoreJobTracker {
	c.coll.mu.RLock()
	defer c.coll.mu.RUnlock()
	return &RestoreJobTracker{coll: c.coll, job: c.coll.jobs[0]}
}

func (t *RestoreCollDDLTracker) DDLProgress() (nextStep int, collectionID int64) {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.coll.ddl.step, t.coll.ddl.collectionID
}

func (t *RestoreCollDDLTracker) SetDDLProgress(nextStep int, collectionID int64) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	t.coll.ddl.step, t.coll.ddl.collectionID = nextStep, collectionID
}

func (t *RestoreCollDDLTracker) CreateTargetPrepared() bool {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.coll.ddl.createTargetPrepared
}

func (t *RestoreCollDDLTracker) MarkCreateTargetPrepared() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	t.coll.ddl.createTargetPrepared = true
}

func (t *RestoreJobTracker) SnapshotTargetPrepared() bool {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.job.snapshotRestore.targetPrepared
}

func (t *RestoreJobTracker) MarkSnapshotTargetPrepared() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	t.job.snapshotRestore.targetPrepared = true
}

func (t *RestoreJobTracker) SnapshotDataRestored() bool {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.job.snapshotRestore.dataRestored
}

func (t *RestoreJobTracker) MarkSnapshotDataRestored() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	t.job.snapshotRestore.targetPrepared, t.job.snapshotRestore.dataRestored = true, true
}

// MarkCommitRequested records that a manual commit may have reached Milvus.
func (t *RestoreJobTracker) MarkCommitRequested() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	t.job.twoPCProgress.commitRequested = true
}

func (t *RestoreJobTracker) CommitRequested() bool {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.job.twoPCProgress.commitRequested
}

func (t *RestoreJobTracker) MarkPrimaryCompleted() {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	t.job.twoPCProgress.primaryCompleted = true
	t.job.twoPCProgress.commitRequested = true
}

func (t *RestoreJobTracker) PrimaryCompleted() bool {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.job.twoPCProgress.primaryCompleted
}

func (t *RestoreJobTracker) MarkMirrorCompleted(endpoint string) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	job := t.job
	if job.twoPCProgress.completedMirrors == nil {
		job.twoPCProgress.completedMirrors = make(map[string]bool)
	}
	job.twoPCProgress.commitRequested = true
	job.twoPCProgress.completedMirrors[endpoint] = true
}

func (t *RestoreJobTracker) MirrorCompleted(endpoint string) bool {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.job.twoPCProgress.completedMirrors[endpoint]
}

func (t *RestoreJobTracker) StagingDir() string {
	t.coll.mu.RLock()
	defer t.coll.mu.RUnlock()
	return t.job.binlogImport.stagingDir
}

func (t *RestoreJobTracker) SetStagingDir(dir string) {
	t.coll.mu.Lock()
	defer t.coll.mu.Unlock()
	t.job.binlogImport.stagingDir = dir
}
