package snapshotbundle

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/zilliztech/milvus-backup/internal/storage"
)

// NewStorageStore adapts a storage client to the bundle reader and writer, so
// bundles can be read and written wherever a storage.Client reaches.
func NewStorageStore(cli storage.Client) Store {
	return storageStore{cli: cli}
}

type storageStore struct {
	cli storage.Client
}

func (b storageStore) Read(ctx context.Context, key string) ([]byte, error) {
	obj, err := b.cli.GetObject(ctx, key)
	if err != nil {
		// A HEAD that also fails means the object is not there — or at least
		// not readable, which the staged-commit logic treats the same way: the
		// caller either writes the object or reports an error, and the
		// read-back after a write still verifies the published bytes.
		if _, headErr := b.cli.HeadObject(ctx, key); headErr != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrObjectNotFound, key, err)
		}
		return nil, fmt.Errorf("snapshotbundle: read object %s: %w", key, err)
	}
	defer obj.Body.Close()
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("snapshotbundle: read object %s: %w", key, err)
	}
	return data, nil
}

func (b storageStore) Write(ctx context.Context, key string, data []byte) error {
	err := b.cli.UploadObject(ctx, storage.UploadObjectInput{
		Key:  key,
		Body: bytes.NewReader(data),
		Size: int64(len(data)),
	})
	if err != nil {
		return fmt.Errorf("snapshotbundle: write object %s: %w", key, err)
	}
	return nil
}

func (b storageStore) Remove(ctx context.Context, key string) error {
	if err := b.cli.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("snapshotbundle: delete object %s: %w", key, err)
	}
	return nil
}
