// Package breakpoint provides a resume ledger for the restore flow. It records
// which backup segments have been confirmed imported (so a re-run skips them)
// and which import jobs were issued but not yet confirmed (so a re-run can
// reconcile them against Milvus and avoid importing the same data twice).
//
// The ledger is persisted through a pluggable Store:
//
//   - fileStore   — a single local JSON file (atomic temp+rename). Used by the
//     CLI, which runs on a stable host.
//   - objectStore — a single object in object storage. Used by the HTTP server,
//     where the restore runs inside a (possibly ephemeral, possibly replicated)
//     pod and a local file would not survive a restart or a different replica.
//
// Writes are infrequent (one per import-job state transition, and jobs take
// seconds to minutes), so a mutex-guarded full rewrite of the ledger is more
// than fast enough for either backend.
package breakpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/zilliztech/milvus-backup/internal/storage"
)

// DeriveID maps a user-supplied breakpoint label (a local path for the CLI, an
// arbitrary string for the REST API) to a stable restore identity. The same
// label always yields the same id, so temp-dir naming and the ledger location
// line up across runs.
func DeriveID(label string) string {
	sum := sha256.Sum256([]byte(label))
	return "restore_bp_" + hex.EncodeToString(sum[:8])
}

// Store persists the ledger bytes.
type Store interface {
	// Load returns the persisted ledger bytes, or (nil, nil) if none exists yet.
	Load() ([]byte, error)
	// Save persists the ledger bytes atomically (the backend either fully
	// writes the new content or leaves the old content intact).
	Save(data []byte) error
	// Location returns a human-readable location for logging.
	Location() string
}

// ── fileStore: local JSON file (CLI) ──

type fileStore struct{ path string }

// NewFileStore stores the ledger as a local JSON file at path.
func NewFileStore(path string) Store { return &fileStore{path: path} }

func (f *fileStore) Location() string { return f.path }

func (f *fileStore) Load() ([]byte, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (f *fileStore) Save(data []byte) error {
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".restore-bp-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp ledger: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp ledger: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync temp ledger: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp ledger: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename ledger into place: %w", err)
	}
	return nil
}

// ── objectStore: a single object in object storage (HTTP server) ──

type objectStore struct {
	ctx context.Context
	cli storage.Client
	key string
}

// NewObjectStore stores the ledger as a single object at key. An object-storage
// PUT is atomic, so no temp+rename dance is needed.
func NewObjectStore(ctx context.Context, cli storage.Client, key string) Store {
	return &objectStore{ctx: ctx, cli: cli, key: key}
}

func (o *objectStore) Location() string { return o.key }

func (o *objectStore) Load() ([]byte, error) {
	exist, err := storage.Exist(o.ctx, o.cli, o.key)
	if err != nil {
		return nil, fmt.Errorf("check ledger exist: %w", err)
	}
	if !exist {
		return nil, nil
	}
	obj, err := o.cli.GetObject(o.ctx, o.key)
	if err != nil {
		return nil, fmt.Errorf("get ledger object: %w", err)
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

func (o *objectStore) Save(data []byte) error {
	in := storage.UploadObjectInput{Key: o.key, Body: bytes.NewReader(data), Size: int64(len(data))}
	if err := o.cli.UploadObject(o.ctx, in); err != nil {
		return fmt.Errorf("upload ledger object: %w", err)
	}
	return nil
}

// ── ledger ──

// InflightJob records an import job that was issued but whose terminal state
// has not yet been persisted. On resume these are reconciled against Milvus via
// GetImportState before any segment is re-issued.
type InflightJob struct {
	NS   string  `json:"ns"`   // target namespace, "db.collection"
	Segs []int64 `json:"segs"` // backup segment IDs covered by this job
}

type state struct {
	RestoreID  string                 `json:"restoreId"`
	BackupName string                 `json:"backupName"`
	Completed  map[string][]int64     `json:"completed"` // ns -> confirmed-imported segment IDs
	Inflight   map[string]InflightJob `json:"inflight"`  // jobID -> issued-but-unconfirmed
}

// Tracker is a concurrency-safe handle over the ledger.
type Tracker struct {
	store Store

	mu sync.Mutex
	st state
	// completedIdx mirrors st.Completed as sets for O(1) membership tests.
	completedIdx map[string]map[int64]struct{}
}

// Open loads the ledger from store. If the store has no content yet, a fresh
// ledger is created in memory (written on the first mutation) seeded with
// restoreID and backupName. If content exists, restoreID/backupName from it win
// so a resumed run keeps a stable identity.
func Open(store Store, restoreID, backupName string) (*Tracker, error) {
	t := &Tracker{
		store: store,
		st: state{
			RestoreID:  restoreID,
			BackupName: backupName,
			Completed:  make(map[string][]int64),
			Inflight:   make(map[string]InflightJob),
		},
		completedIdx: make(map[string]map[int64]struct{}),
	}

	data, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("breakpoint: load ledger from %s: %w", store.Location(), err)
	}
	if len(data) == 0 {
		return t, nil
	}

	if err := json.Unmarshal(data, &t.st); err != nil {
		return nil, fmt.Errorf("breakpoint: parse ledger from %s: %w", store.Location(), err)
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

// OpenFile opens a ledger backed by a local JSON file (CLI).
func OpenFile(path, restoreID, backupName string) (*Tracker, error) {
	return Open(NewFileStore(path), restoreID, backupName)
}

// OpenObject opens a ledger backed by a single object in object storage (server).
func OpenObject(ctx context.Context, cli storage.Client, key, restoreID, backupName string) (*Tracker, error) {
	return Open(NewObjectStore(ctx, cli, key), restoreID, backupName)
}

// Location returns where the ledger is persisted (for logging).
func (t *Tracker) Location() string { return t.store.Location() }

// RestoreID returns the stable restore identity persisted in the ledger.
func (t *Tracker) RestoreID() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.st.RestoreID
}

// CompletedCount returns how many segments of ns are confirmed imported.
func (t *Tracker) CompletedCount(ns string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.completedIdx[ns])
}

// InflightCount returns how many in-flight (issued, unconfirmed) jobs target ns.
func (t *Tracker) InflightCount(ns string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, j := range t.st.Inflight {
		if j.NS == ns {
			n++
		}
	}
	return n
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
// the inflight record. A no-op if the jobID is not currently inflight.
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

// save marshals and persists the ledger. Caller must hold t.mu.
func (t *Tracker) save() error {
	data, err := json.MarshalIndent(&t.st, "", "  ")
	if err != nil {
		return fmt.Errorf("breakpoint: marshal ledger: %w", err)
	}
	if err := t.store.Save(data); err != nil {
		return fmt.Errorf("breakpoint: save ledger to %s: %w", t.store.Location(), err)
	}
	return nil
}
