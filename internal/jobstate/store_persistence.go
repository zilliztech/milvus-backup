package jobstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// PersistenceBackend stores opaque values. The owning Store defines task keys,
// serialization formats, and when an operation is required or best effort.
type PersistenceBackend interface {
	Check(context.Context) error
	Load(context.Context, string) ([]byte, error)
	Save(context.Context, string, []byte) error
	Delete(context.Context, string) error
}

var ErrPersistenceKeyNotFound = errors.New("persistence key not found")

// storePersistence holds the system persistence state, guarded by Store.mu.
type storePersistence struct {
	config    cfg.PersistenceConfig
	backend   PersistenceBackend
	checkOnce *sync.Once
}

type persistenceLocation struct {
	Type      string `json:"type"`
	Directory string `json:"directory,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
}

// Keys are relative object paths; task IDs must be encoded by their task layer.
func validatePersistenceKey(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !fs.ValidPath(key) || key == "." || strings.Contains(key, `\`) {
		return fmt.Errorf("persistence: invalid key %q", key)
	}
	return nil
}

// A unique probe uses the actual Save/Load/Delete operations without touching
// task records. Cleanup has its own bounded context even if the probe times out.
func checkPersistenceBackend(ctx context.Context, backend PersistenceBackend) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := ".persistence-check-" + uuid.NewString()
	value := []byte(uuid.NewString())
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if cleanupErr := backend.Delete(cleanupCtx, key); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("check persistence delete: %w", cleanupErr))
		}
	}()
	if err := backend.Save(ctx, key, value); err != nil {
		return fmt.Errorf("check persistence save: %w", err)
	}
	got, err := backend.Load(ctx, key)
	if err != nil {
		return fmt.Errorf("check persistence load: %w", err)
	}
	if !bytes.Equal(got, value) {
		return errors.New("check persistence: read-back content mismatch")
	}
	return nil
}

// ConfigurePersistence is startup-only. Identical configuration preserves the
// shared client and startup-check guard (CLI server startup enters twice).
func (s *Store) ConfigurePersistence(c cfg.PersistenceConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persistence.checkOnce != nil && reflect.DeepEqual(s.persistence.config, c) {
		return
	}
	s.persistence = storePersistence{
		config:    c,
		checkOnce: new(sync.Once),
	}
}

// InitializePersistence performs a bounded, best-effort startup check once per
// configuration. Failure is logged; future operations can retry the backend.
func (s *Store) InitializePersistence(ctx context.Context, c cfg.PersistenceConfig) {
	s.ConfigurePersistence(c)
	s.mu.RLock()
	once := s.persistence.checkOnce
	s.mu.RUnlock()
	once.Do(func() {
		checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := s.CheckPersistence(checkCtx); err != nil {
			log.Warn("persistence startup check failed; continuing startup", zap.String("backend", c.Storage.Provider.Val), zap.Error(err))
		}
	})
}

func (s *Store) CheckPersistence(ctx context.Context) error {
	backend, _, err := s.persistenceStore()
	if err != nil {
		return err
	}
	if backend == nil {
		return nil
	}
	return backend.Check(ctx)
}

// Opening a backend constructs a client; storage I/O runs outside the Store lock.
func (s *Store) persistenceStore() (PersistenceBackend, persistenceLocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	location := persistenceLocationFromConfig(s.persistence.config)
	if !s.persistence.config.Enabled.Val {
		return nil, location, nil
	}
	if s.persistence.backend == nil {
		backend, err := openPersistenceBackend(s.persistence.config)
		if err != nil {
			return nil, location, err
		}
		s.persistence.backend = backend
	}
	return s.persistence.backend, location, nil
}

// persistenceLocationFromConfig records the namespace selected at startup.
func persistenceLocationFromConfig(c cfg.PersistenceConfig) persistenceLocation {
	if c.Storage.Provider.Val == cfg.ProviderLocal {
		return persistenceLocation{Type: cfg.ProviderLocal, Directory: c.Storage.RootPath.Val}
	}
	return persistenceLocation{Type: c.Storage.Provider.Val, Bucket: c.Storage.BucketName.Val, Prefix: c.Storage.RootPath.Val}
}

// openPersistenceBackend returns no backend when persistence is disabled. No filesystem or
// network access is performed in that case, including for cold lookups.
func openPersistenceBackend(c cfg.PersistenceConfig) (PersistenceBackend, error) {
	if !c.Enabled.Val {
		return nil, nil
	}
	backend, err := openStorageBackend(c)
	if err != nil {
		return nil, err
	}
	if !c.Encryption.Enabled.Val {
		return backend, nil
	}
	provider, err := newKeyProvider(c.Encryption)
	if err != nil {
		return nil, err
	}
	return encryptedBackend{PersistenceBackend: backend, keys: provider}, nil
}

func openStorageBackend(c cfg.PersistenceConfig) (PersistenceBackend, error) {
	location := persistenceLocationFromConfig(c)
	if location.Type == cfg.ProviderLocal {
		if location.Directory == "" {
			return nil, fmt.Errorf("persistence: local directory is required")
		}
		return localPersistenceBackend{Directory: location.Directory}, nil
	}
	if location.Bucket == "" {
		return nil, fmt.Errorf("persistence: storage bucketName is required")
	}
	return &objectPersistenceBackend{config: storage.PersistenceStorageConfig(&c.Storage), prefix: location.Prefix}, nil
}
