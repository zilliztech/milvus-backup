package jobstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

var ErrPersistence = errors.New("restore persistence failed")

// restorePersistence limits a task to saving and deleting its persisted states.
// Save invokes encode synchronously, while the caller holds the task mutex,
// and only when persistence is enabled.
type restorePersistence interface {
	saveRestoreState(id string, encode func(persistenceLocation) ([]byte, error)) error
	deleteRestoreState(id string) error
}

var _ restorePersistence = (*Store)(nil)

// restoreStateKey preserves the existing restore persisted state layout. Other task
// types can choose their own namespace without changing the backend.
func restoreStateKey(taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return hex.EncodeToString(sum[:]) + ".json"
}

// loadRestore uses memory first. Cold loads use the startup configuration
// and are serialized with registration, never with execution. Cleanup loads
// resource records as Pending before publishing them to concurrent callers.
func (s *Store) loadRestore(id string, cleanup bool) (*RestoreTracker, error) {
	s.mu.RLock()
	task := s.restoreTask[id]
	s.mu.RUnlock()
	if task != nil {
		return &RestoreTracker{task: task}, nil
	}
	s.restoreLoadMu.Lock()
	defer s.restoreLoadMu.Unlock()
	s.mu.RLock()
	task = s.restoreTask[id]
	s.mu.RUnlock()
	if task != nil {
		return &RestoreTracker{task: task}, nil
	}
	backend, location, err := s.persistenceStore()
	logger := restorePersistenceLogger(id, "load", location)
	if err != nil {
		logger.Warn("restore persistence operation failed", zap.Error(err))
		return nil, fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	if backend == nil {
		return nil, ErrTaskNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	b, err := backend.Load(ctx, restoreStateKey(id))
	if err != nil {
		logger.Warn("restore persistence operation failed", zap.Error(err))
		if errors.Is(err, ErrPersistenceKeyNotFound) {
			return nil, ErrTaskNotFound
		}
		return nil, fmt.Errorf("%w: load: %w", ErrPersistence, err)
	}
	task, err = decodeRestore(id, b, cleanup)
	if err != nil {
		logger.Warn("restore persistence operation failed", zap.Error(err))
		if errors.Is(err, ErrRestoreConflict) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	// Loaded tasks use the same system backend as newly registered tasks.
	task.persistence = s
	s.mu.Lock()
	s.restoreTask[id] = task
	s.mu.Unlock()
	logger.Info("restore persistence operation succeeded")
	return &RestoreTracker{task: task}, nil
}

// All persistence runs under the task mutex and only after workers quiesce.
// Bounded synchronous retries cannot outlive this attempt and overwrite the next.
func restorePersistenceOperation(op func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = op(ctx)
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	return nil
}

func restorePersistenceLogger(id, operation string, location persistenceLocation) *zap.Logger {
	return log.With(
		zap.String("operation", operation),
		zap.String("task_id", id),
		zap.String("backend", location.Type),
		zap.String("key", restoreStateKey(id)),
	)
}

func (s *Store) saveRestoreState(id string, encode func(persistenceLocation) ([]byte, error)) error {
	backend, location, err := s.persistenceStore()
	logger := restorePersistenceLogger(id, "save", location)
	if err != nil {
		logger.Warn("restore persistence operation failed", zap.Error(err))
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	if backend == nil {
		return nil
	}
	var data []byte
	err = restorePersistenceOperation(func(ctx context.Context) error {
		if data == nil {
			var err error
			data, err = encode(location)
			if err != nil {
				return err
			}
		}
		return backend.Save(ctx, restoreStateKey(id), data)
	})
	if err != nil {
		logger.Warn("restore persistence operation failed", zap.Error(err))
		return err
	}
	logger.Info("restore persistence operation succeeded")
	return nil
}

func (s *Store) deleteRestoreState(id string) error {
	backend, location, err := s.persistenceStore()
	logger := restorePersistenceLogger(id, "delete", location)
	if err != nil {
		logger.Warn("restore persistence operation failed", zap.Error(err))
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	if backend == nil {
		return nil
	}
	err = restorePersistenceOperation(func(ctx context.Context) error {
		return backend.Delete(ctx, restoreStateKey(id))
	})
	if err != nil {
		logger.Warn("restore persistence operation failed", zap.Error(err))
		return err
	}
	logger.Info("restore persistence operation succeeded")
	return nil
}

func (t *restoreTask) saveLocked(marker string) error {
	if !t.enableResume || t.persistence == nil {
		return nil
	}
	return t.persistence.saveRestoreState(t.id, func(location persistenceLocation) ([]byte, error) {
		return t.encodeLocked(marker, location)
	})
}

func (t *restoreTask) deleteLocked() error {
	if !t.enableResume || t.persistence == nil {
		return nil
	}
	return t.persistence.deleteRestoreState(t.id)
}

func (t *restoreTask) persistTerminalLocked() {
	if t.state == RestoreStateSuccess {
		_ = t.deleteLocked()
	} else if t.state == RestoreStateFail && t.plan != nil {
		_ = t.saveLocked(markerAvailable)
	}
}
