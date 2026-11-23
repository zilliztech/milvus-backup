package jobstate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

type localPersistenceBackend struct{ Directory string }

func (s localPersistenceBackend) Load(ctx context.Context, key string) ([]byte, error) {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(s.Directory, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrPersistenceKeyNotFound
	}
	return b, err
}
func (s localPersistenceBackend) Save(ctx context.Context, key string, b []byte) error {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(s.Directory, key)), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(filepath.Join(s.Directory, key)), ".persistence-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(s.Directory, key)); err != nil {
		return err
	}
	return syncPersistenceDir(filepath.Dir(filepath.Join(s.Directory, key)))
}
func (s localPersistenceBackend) Delete(ctx context.Context, key string) error {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return err
	}
	err := os.Remove(filepath.Join(s.Directory, key))
	if errors.Is(err, os.ErrNotExist) {
		err = syncPersistenceDir(filepath.Dir(filepath.Join(s.Directory, key)))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err != nil {
		return err
	}
	return syncPersistenceDir(filepath.Dir(filepath.Join(s.Directory, key)))
}
func syncPersistenceDir(directory string) error {
	d, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s localPersistenceBackend) Check(ctx context.Context) error {
	return checkPersistenceBackend(ctx, s)
}
