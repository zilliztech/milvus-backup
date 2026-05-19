package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var _ Client = (*LocalClient)(nil)

type LocalClient struct {
	config  Config
	baseDir string // filepath.Join(localPath, bucket)
}

func newLocalClient(config Config) *LocalClient {
	return &LocalClient{
		config:  config,
		baseDir: filepath.Join(config.LocalPath, config.Bucket),
	}
}

func (l *LocalClient) absPath(key string) string {
	return filepath.Join(l.baseDir, key)
}

func (l *LocalClient) relPath(abs string) string {
	prefix := l.baseDir + string(filepath.Separator)
	return strings.TrimPrefix(abs, prefix)
}

func (l *LocalClient) Config() Config {
	return l.config
}

func (l *LocalClient) CopyObject(_ context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(*LocalClient)
	if !ok {
		return fmt.Errorf("storage: local copy object only support local client")
	}

	srcAbs := srcCli.absPath(i.SrcAttr.Key)
	destAbs := l.absPath(i.DestKey)

	srcDir := filepath.Dir(srcAbs)
	srcDirInfo, err := os.Stat(srcDir)
	if err != nil {
		return fmt.Errorf("storage: local copy object get src parent dir info %w", err)
	}
	destDir := filepath.Dir(destAbs)
	err = os.MkdirAll(destDir, srcDirInfo.Mode())
	if err != nil {
		return fmt.Errorf("storage: local copy object create dest parent dir %w", err)
	}

	src, err := os.Open(srcAbs)
	if err != nil {
		return fmt.Errorf("storage: local copy object open src file %w", err)
	}
	defer src.Close()
	dest, err := os.Create(destAbs)
	if err != nil {
		return fmt.Errorf("storage: local copy object create dest file %w", err)
	}
	defer dest.Close()
	// os package does not support copy file, use io.Copy instead
	if _, err = io.Copy(dest, src); err != nil {
		return fmt.Errorf("storage: local copy object copy file %w", err)
	}

	srcStat, err := os.Stat(srcAbs)
	if err != nil {
		return fmt.Errorf("storage: local copy object get src file stat %w", err)
	}

	if err = os.Chmod(destAbs, srcStat.Mode()); err != nil {
		return fmt.Errorf("storage: local copy object chmod dest file %w", err)
	}

	return nil
}

func (l *LocalClient) HeadObject(_ context.Context, key string) (ObjectAttr, error) {
	abs := l.absPath(key)
	info, err := os.Stat(abs)
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: local head object %w", err)
	}

	return ObjectAttr{Key: key, Length: info.Size()}, nil
}

func (l *LocalClient) GetObject(_ context.Context, key string) (*Object, error) {
	abs := l.absPath(key)
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("storage: local get object %w", err)
	}

	f, err := os.Open(abs)
	if err != nil {
		return nil, fmt.Errorf("storage: local get object %w", err)
	}

	return &Object{Length: info.Size(), Body: f}, nil
}

func (l *LocalClient) UploadObject(_ context.Context, i UploadObjectInput) error {
	abs := l.absPath(i.Key)
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("storage: local upload object %w", err)
	}

	f, err := os.Create(abs)
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
	abs := l.absPath(prefix)
	// check if prefix is a file
	info, err := os.Stat(abs)
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
		entries, err = l.listRecursive(abs)
	} else {
		entries, err = l.listNonRecursive(abs)
	}
	if err != nil {
		return nil, err
	}
	return &localIterator{entries: entries}, nil
}

func (l *LocalClient) listRecursive(absPrefix string) ([]ObjectAttr, error) {
	var entries []ObjectAttr
	err := filepath.Walk(absPrefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		entries = append(entries, ObjectAttr{Key: l.relPath(path), Length: info.Size()})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: local list prefix recursive %w", err)
	}
	return entries, nil
}

func (l *LocalClient) listNonRecursive(absPrefix string) ([]ObjectAttr, error) {
	infos, err := os.ReadDir(absPrefix)
	if err != nil {
		return nil, fmt.Errorf("storage: local list prefix non recursive %w", err)
	}

	entries := make([]ObjectAttr, 0, len(infos))
	for _, info := range infos {
		var size int64
		absPath := filepath.Join(absPrefix, info.Name())
		if !info.IsDir() {
			stat, err := os.Stat(absPath)
			if err != nil {
				return nil, fmt.Errorf("storage: local list prefix %w", err)
			}
			size = stat.Size()
		}
		entries = append(entries, ObjectAttr{Key: l.relPath(absPath), Length: size})
	}
	return entries, nil
}

func (l *LocalClient) DeleteObject(_ context.Context, key string) error {
	abs := l.absPath(key)
	if err := os.Remove(abs); err != nil {
		return fmt.Errorf("storage: local delete prefix %w", err)
	}
	return nil
}

func (l *LocalClient) BucketExist(_ context.Context, _ string) (bool, error) {
	_, err := os.Stat(l.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("storage: local bucket exist %w", err)
	}
	return true, nil
}

func (l *LocalClient) CreateBucket(_ context.Context) error {
	if err := os.MkdirAll(l.baseDir, os.ModePerm); err != nil {
		return fmt.Errorf("storage: local create bucket %w", err)
	}
	return nil
}
