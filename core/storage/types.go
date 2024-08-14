package storage

import (
	"context"
	"io"
)

type FileReader interface {
	io.Reader
	io.Closer
}

// ChunkManager is to manager chunks.
type ChunkManager interface {
	// Write writes @content to @filePath.
	Write(ctx context.Context, bucketName string, filePath string, content []byte) error
	// Exist returns true if @filePath exists.
	Exist(ctx context.Context, bucketName string, filePath string) (bool, error)
	// Read reads @filePath and returns content.
	Read(ctx context.Context, bucketName string, filePath string) ([]byte, error)
	// ListWithPrefix list all objects with same @prefix
	ListWithPrefix(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []int64, error)
	// Remove delete @filePath.
	Remove(ctx context.Context, bucketName string, filePath string) error
	// RemoveWithPrefix remove files with same @prefix.
	RemoveWithPrefix(ctx context.Context, bucketName string, prefix string) error
	// Copy files from fromPath into toPath recursively
	Copy(ctx context.Context, fromBucketName string, toBucketName string, fromPath string, toPath string) error
}
