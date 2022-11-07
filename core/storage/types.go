package storage

import (
	"context"
	"golang.org/x/exp/mmap"
	"io"
	"time"
)

type FileReader interface {
	io.Reader
	io.Closer
}

// ChunkManager is to manager chunks.
// Include Read, Write, Remove chunks.
type ChunkManager interface {
	// RootPath returns current root path.
	RootPath() string
	// Path returns path of @filePath.
	Path(ctx context.Context, filePath string) (string, error)
	// Size returns path of @filePath.
	Size(ctx context.Context, filePath string) (int64, error)
	// Write writes @content to @filePath.
	Write(ctx context.Context, filePath string, content []byte) error
	// MultiWrite writes multi @content to @filePath.
	MultiWrite(ctx context.Context, contents map[string][]byte) error
	// Exist returns true if @filePath exists.
	Exist(ctx context.Context, filePath string) (bool, error)
	// Read reads @filePath and returns content.
	Read(ctx context.Context, filePath string) ([]byte, error)
	// Reader return a reader for @filePath
	Reader(ctx context.Context, filePath string) (FileReader, error)
	// MultiRead reads @filePath and returns content.
	MultiRead(ctx context.Context, filePaths []string) ([][]byte, error)
	ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error)
	// ReadWithPrefix reads files with same @prefix and returns contents.
	ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error)
	Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error)
	// ReadAt reads @filePath by offset @off, content stored in @p, return @n as the number of bytes read.
	// if all bytes are read, @err is io.EOF.
	// return other error if read failed.
	ReadAt(ctx context.Context, filePath string, off int64, length int64) (p []byte, err error)
	// Remove delete @filePath.
	Remove(ctx context.Context, filePath string) error
	// MultiRemove delete @filePaths.
	MultiRemove(ctx context.Context, filePaths []string) error
	// RemoveWithPrefix remove files with same @prefix.
	RemoveWithPrefix(ctx context.Context, prefix string) error
	// Move move files from fromPath into toPath recursively
	Copy(ctx context.Context, fromPath string, toPath string) error
}
