package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var _ Client = (*LocalClient)(nil)

type LocalClient struct {
	config Config
}

func newLocalClient(config Config) *LocalClient {
	return &LocalClient{config: config}
}

func (l *LocalClient) Config() Config {
	return l.config
}

func (l *LocalClient) CopyObject(_ context.Context, i CopyObjectInput) error {
	_, ok := i.SrcCli.(*LocalClient)
	if !ok {
		return fmt.Errorf("storage: local copy object only support local client")
	}

	srcDir := filepath.Dir(i.SrcKey)
	srcDirInfo, err := os.Stat(srcDir)
	if err != nil {
		return fmt.Errorf("storage: local copy object get src parent dir info %w", err)
	}
	destDir := filepath.Dir(i.DestKey)
	err = os.MkdirAll(destDir, srcDirInfo.Mode())
	if err != nil {
		return fmt.Errorf("storage: local copy object create dest parent dir %w", err)
	}

	src, err := os.Open(i.SrcKey)
	if err != nil {
		return fmt.Errorf("storage: local copy object open src file %w", err)
	}
	defer src.Close()
	dest, err := os.Create(i.DestKey)
	if err != nil {
		return fmt.Errorf("storage: local copy object create dest file %w", err)
	}
	defer dest.Close()
	// os package does not support copy file, use io.Copy instead
	if _, err = io.Copy(dest, src); err != nil {
		return fmt.Errorf("storage: local copy object copy file %w", err)
	}

	srcStat, err := os.Stat(i.SrcKey)
	if err != nil {
		return fmt.Errorf("storage: local copy object get src file stat %w", err)
	}

	if err = os.Chmod(i.DestKey, srcStat.Mode()); err != nil {
		return fmt.Errorf("storage: local copy object chmod dest file %w", err)
	}

	return nil
}

func (l *LocalClient) HeadObject(_ context.Context, key string) (ObjectAttr, error) {
	info, err := os.Stat(key)
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: local head object %w", err)
	}

	return ObjectAttr{Key: key, Length: info.Size()}, nil
}

func (l *LocalClient) GetObject(_ context.Context, key string) (*Object, error) {
	info, err := os.Stat(key)
	if err != nil {
		return nil, fmt.Errorf("storage: local get object %w", err)
	}

	f, err := os.Open(key)
	if err != nil {
		return nil, fmt.Errorf("storage: local get object %w", err)
	}

	return &Object{Length: info.Size(), Body: f}, nil
}

func (l *LocalClient) UploadObject(_ context.Context, i UploadObjectInput) error {
	dir := filepath.Dir(i.Key)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("storage: local upload object %w", err)
	}

	f, err := os.Create(i.Key)
	if err != nil {
		return fmt.Errorf("storage: local upload object %w", err)
	}
	defer f.Close()

	if _, err = io.Copy(f, i.Body); err != nil {
		return fmt.Errorf("storage: local upload object %w", err)
	}

	return nil
}

type localIterator struct {
	entries []ObjectAttr
	index   int
}

func (l *localIterator) HasNext() bool {
	return l.index < len(l.entries)
}

func (l *localIterator) Next() (ObjectAttr, error) {
	if !l.HasNext() {
		return ObjectAttr{}, fmt.Errorf("storage: local list prefix no more entries")
	}
	entry := l.entries[l.index]
	l.index++
	return entry, nil
}

func (l *LocalClient) ListPrefix(_ context.Context, prefix string, recursive bool) (ObjectIterator, error) {
	// check if prefix is a file
	info, err := os.Stat(prefix)
	if err != nil {
		if os.IsNotExist(err) {
			return &localIterator{}, nil
		}
		return nil, fmt.Errorf("storage: local list prefix stat %w", err)
	}

	// if prefix is a file, return it directly
	if !info.IsDir() {
		return &localIterator{entries: []ObjectAttr{{Key: prefix, Length: info.Size()}}}, nil
	}

	var entries []ObjectAttr
	if recursive {
		entries, err = l.listRecursive(prefix)
	} else {
		entries, err = l.listNonRecursive(prefix)
	}
	if err != nil {
		return nil, err
	}
	return &localIterator{entries: entries}, nil
}

func (l *LocalClient) listRecursive(prefix string) ([]ObjectAttr, error) {
	var entries []ObjectAttr
	err := filepath.Walk(prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		entries = append(entries, ObjectAttr{Key: path, Length: info.Size()})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: local list prefix recursive %w", err)
	}
	return entries, nil
}

func (l *LocalClient) listNonRecursive(prefix string) ([]ObjectAttr, error) {
	infos, err := os.ReadDir(prefix)
	if err != nil {
		return nil, fmt.Errorf("storage: local list prefix non recursive %w", err)
	}

	entries := make([]ObjectAttr, 0, len(infos))
	for _, info := range infos {
		var size int64
		if !info.IsDir() {
			stat, err := os.Stat(filepath.Join(prefix, info.Name()))
			if err != nil {
				return nil, fmt.Errorf("storage: local list prefix %w", err)
			}
			size = stat.Size()
		}
		entries = append(entries, ObjectAttr{Key: filepath.Join(prefix, info.Name()), Length: size})
	}
	return entries, nil
}

func (l *LocalClient) DeleteObject(_ context.Context, key string) error {
	if err := os.Remove(key); err != nil {
		return fmt.Errorf("storage: local delete prefix %w", err)
	}
	return nil
}

func (l *LocalClient) BucketExist(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (l *LocalClient) CreateBucket(_ context.Context) error {
	return nil
}
