// Package breakpoint provides a local-disk, JSON-backed resume ledger for the
// restore flow. It records which backup segments have been confirmed imported
// (so a re-run skips them) and which import jobs were issued but not yet
// confirmed (so a re-run can reconcile them against Milvus and avoid importing
// the same data twice).
//
// The ledger is a single JSON file on the local filesystem. Writes are
// infrequent (one per import-job state transition, and jobs take seconds to
// minutes), so a mutex-guarded full rewrite is more than fast enough. Every
// write goes to a temp file and is renamed into place, so a crash never leaves
// a half-written ledger.
package breakpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// InflightJob records an import job that was issued but whose terminal state
// has not yet been persisted. On resume these are reconciled against Milvus via
// GetImportState before any segment is re-issued.
type InflightJob struct {
	NS   string  `json:"ns"`   // target namespace, "db.collection"
	Segs []int64 `json:"segs"` // backup segment IDs covered by this job
}

// state is the on-disk shape of the ledger.
type state struct {
	RestoreID  string                 `json:"restoreId"`
	BackupName string                 `json:"backupName"`
	Completed  map[string][]int64     `json:"completed"` // ns -> confirmed-imported segment IDs
	Inflight   map[string]InflightJob `json:"inflight"`  // jobID -> issued-but-unconfirmed
}

// Tracker is a concurrency-safe handle over the ledger file.
type Tracker struct {
	path string

	mu sync.Mutex
	st state
	// completedIdx mirrors st.Completed as sets for O(1) membership tests.
	completedIdx map[string]map[int64]struct{}
}

// Open loads the ledger at path. If the file does not exist a fresh ledger is
// created in memory (and written on the first mutation) seeded with restoreID
// and backupName. If the file exists, restoreID/backupName from the file win so
// a resumed run keeps a stable identity (the restore ID drives temp-dir naming).
func Open(path, restoreID, backupName string) (*Tracker, error) {
	t := &Tracker{
		path: path,
		st: state{
			RestoreID:  restoreID,
			BackupName: backupName,
			Completed:  make(map[string][]int64),
			Inflight:   make(map[string]InflightJob),
		},
		completedIdx: make(map[string]map[int64]struct{}),
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return t, nil
		}
		return nil, fmt.Errorf("breakpoint: read ledger %q: %w", path, err)
	}

	if err := json.Unmarshal(data, &t.st); err != nil {
		return nil, fmt.Errorf("breakpoint: parse ledger %q: %w", path, err)
	}
	if t.st.Completed == nil {
		t.st.Completed = make(map[string][]int64)
	}
	if t.st.Inflight == nil {
		t.st.Inflight = make(map[string]InflightJob)
	}
	for ns, segs := range t.st.Completed {
		set := make(map[int64]struct{}, len(segs))
		for _, s := range segs {
			set[s] = struct{}{}
		}
		t.completedIdx[ns] = set
	}
	return t, nil
}

// RestoreID returns the stable restore identity persisted in the ledger.
func (t *Tracker) RestoreID() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.st.RestoreID
}

// IsCompleted reports whether the given backup segment of ns is already
// confirmed imported.
func (t *Tracker) IsCompleted(ns string, segID int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.completedIdx[ns]
	if !ok {
		return false
	}
	_, ok = set[segID]
	return ok
}

// MarkInflight records an issued import job before its terminal state is known.
// Persisted immediately so a crash right after issuing is recoverable.
func (t *Tracker) MarkInflight(jobID, ns string, segs []int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.st.Inflight[jobID] = InflightJob{NS: ns, Segs: append([]int64(nil), segs...)}
	return t.save()
}

// Complete promotes an inflight job's segments to the completed set and removes
// the inflight record. Safe to call with a jobID that is no longer inflight
// (e.g. recovered via a different code path) as long as ns/segs are supplied by
// the caller; here we rely on the inflight record, falling back to a no-op.
func (t *Tracker) Complete(jobID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	infl, ok := t.st.Inflight[jobID]
	if !ok {
		return nil
	}
	t.addCompletedLocked(infl.NS, infl.Segs)
	delete(t.st.Inflight, jobID)
	return t.save()
}

// Fail drops an inflight job's record without completing it; its segments stay
// uncompleted and will be re-batched on the next run.
func (t *Tracker) Fail(jobID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.st.Inflight[jobID]; !ok {
		return nil
	}
	delete(t.st.Inflight, jobID)
	return t.save()
}

// Inflight returns a snapshot of currently-inflight jobs for reconciliation.
func (t *Tracker) Inflight() map[string]InflightJob {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]InflightJob, len(t.st.Inflight))
	for k, v := range t.st.Inflight {
		out[k] = v
	}
	return out
}

func (t *Tracker) addCompletedLocked(ns string, segs []int64) {
	set, ok := t.completedIdx[ns]
	if !ok {
		set = make(map[int64]struct{})
		t.completedIdx[ns] = set
	}
	for _, s := range segs {
		if _, dup := set[s]; dup {
			continue
		}
		set[s] = struct{}{}
		t.st.Completed[ns] = append(t.st.Completed[ns], s)
	}
}

// save writes the ledger atomically: temp file in the same dir + rename.
// Caller must hold t.mu.
func (t *Tracker) save() error {
	data, err := json.MarshalIndent(&t.st, "", "  ")
	if err != nil {
		return fmt.Errorf("breakpoint: marshal ledger: %w", err)
	}
	dir := filepath.Dir(t.path)
	tmp, err := os.CreateTemp(dir, ".restore-bp-*.tmp")
	if err != nil {
		return fmt.Errorf("breakpoint: create temp ledger: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("breakpoint: write temp ledger: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("breakpoint: sync temp ledger: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("breakpoint: close temp ledger: %w", err)
	}
	if err := os.Rename(tmpName, t.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("breakpoint: rename ledger into place: %w", err)
	}
	return nil
}
