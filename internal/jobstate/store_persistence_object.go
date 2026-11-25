package jobstate

import (
	"bytes"
	"context"
	"io"
	"path"
	"sync"

	"github.com/zilliztech/milvus-backup/internal/storage"
)

type objectPersistenceBackend struct {
	config storage.Config
	prefix string
	mu     sync.Mutex
	client storage.Client
}

// Construct on first use so region discovery uses the operation's context outside the Store lock.
func (s *objectPersistenceBackend) clientForKey(ctx context.Context, key string) (storage.Client, error) {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		cli, err := storage.NewClient(ctx, s.config)
		if err != nil {
			return nil, err
		}
		s.client = cli
	}
	return s.client, nil
}

func (s *objectPersistenceBackend) Load(ctx context.Context, key string) ([]byte, error) {
	cli, err := s.clientForKey(ctx, key)
	if err != nil {
		return nil, err
	}
	obj, err := cli.GetObject(ctx, path.Join(s.prefix, key))
	if err != nil {
		if storage.IsObjectNotFound(err) {
			return nil, ErrPersistenceKeyNotFound
		}
		return nil, err
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

func (s *objectPersistenceBackend) Save(ctx context.Context, key string, b []byte) error {
	cli, err := s.clientForKey(ctx, key)
	if err != nil {
		return err
	}
	return cli.UploadObject(ctx, storage.UploadObjectInput{Key: path.Join(s.prefix, key), Body: bytes.NewReader(b), Size: int64(len(b))})
}

func (s *objectPersistenceBackend) Delete(ctx context.Context, key string) error {
	cli, err := s.clientForKey(ctx, key)
	if err != nil {
		return err
	}
	err = cli.DeleteObject(ctx, path.Join(s.prefix, key))
	if storage.IsObjectNotFound(err) {
		return nil
	}
	return err
}

func (s *objectPersistenceBackend) Check(ctx context.Context) error {
	return checkPersistenceBackend(ctx, s)
}
