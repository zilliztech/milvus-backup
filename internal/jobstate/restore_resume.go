package jobstate

import (
	"errors"
	"fmt"
	"time"

	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

var ErrRestoreConflict = errors.New("restore state conflict")

func (t *RestoreTracker) Start() bool {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()
	if t.task.state != RestoreStateInitial {
		return false
	}
	t.task.state = RestoreStateExecuting
	return true
}

// SavePlan publishes the complete collection plan before the first DDL or import.
func (t *RestoreTracker) SavePlan(plan *restoreplan.Plan) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()
	t.task.plan = plan
}

func (s *Store) restoreTracker(id string) (*RestoreTracker, error) {
	return s.loadRestore(id, false)
}

// StartResume takes exclusive ownership of a failed job and its immutable inputs.
func (s *Store) StartResume(id string) (*RestoreTracker, *restoreplan.Plan, error) {
	tr, err := s.restoreTracker(id)
	if err != nil {
		return nil, nil, err
	}
	t := tr.task
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enableResume || t.state != RestoreStateFail || t.plan == nil {
		return nil, nil, fmt.Errorf("%w: resume requires a failed resumable restore with a prepared plan", ErrRestoreConflict)
	}
	if err := t.saveLocked(markerStain); err != nil {
		return nil, nil, err
	}
	t.state = RestoreStateExecuting
	t.endTime = time.Time{}
	t.errorMessage = ""
	return tr, t.plan, nil
}

func (s *Store) StartCleanup(id string) (*RestoreTracker, *restoreplan.Plan, error) {
	tr, err := s.loadRestore(id, true)
	if err != nil {
		return nil, nil, err
	}
	t := tr.task
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == RestoreStateFailedCleaned || (t.enableResume && t.state == RestoreStateSuccess) {
		_ = t.deleteLocked()
		return nil, nil, nil
	}
	if !t.enableResume || (t.state != RestoreStateFail && t.state != RestoreStateFailedCleanupPending) || t.cleaning || t.plan == nil {
		return nil, nil, fmt.Errorf("%w: cleanup requires an idle failed resumable restore with a prepared plan", ErrRestoreConflict)
	}
	t.cleaning = true
	t.state = RestoreStateFailedCleanupPending
	if err := t.saveLocked(markerStain); err != nil {
		t.cleaning = false
		return nil, nil, err
	}
	return tr, t.plan, nil
}

func (t *RestoreTracker) FinishCleanup(err error) {
	task := t.task
	task.mu.Lock()
	defer task.mu.Unlock()
	task.cleaning = false
	if err == nil {
		task.state = RestoreStateFailedCleaned
		task.releasePlanLocked()
		_ = task.deleteLocked()
	} else {
		_ = task.saveLocked(markerStain)
	}
}
