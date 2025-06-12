package storage

import (
	"context"
	"io"
)

type CopyObjectInput struct {
	SrcCli  Client
	SrcKey  string
	DestKey string
}

type UploadObjectInput struct {
	Key  string
	Body io.Reader

	// The size of the file to be uploaded, if unknown, set to 0 or negative
	// Configuring this parameter can help reduce memory usage.
	Size int64
}

type Object struct {
	Length int64
	Body   io.ReadCloser
}

type ObjectAttr struct {
	Key    string
	Length int64
}

func (o *ObjectAttr) IsEmpty() bool { return o.Length == 0 }

type Config struct {
	Provider string

	Endpoint string
	UseSSL   bool

	IAMEndpoint string
	UseIAM      bool

	AK    string
	SK    string
	Token string

	GcpCredentialJSON string

	Bucket string
}

type ObjectIterator interface {
	HasNext() bool
	Next() (ObjectAttr, error)
}

type Client interface {
	Config() Config

	// CopyObject copy an object from src to dest, call on dest client.
	// The implementation of CopyObject must directly use the copy API provided by the service provider,
	CopyObject(ctx context.Context, i CopyObjectInput) error
	// HeadObject determine if an object exists, and you have permission to access it.
	HeadObject(ctx context.Context, key string) (ObjectAttr, error)
	// GetObject get an object
	GetObject(ctx context.Context, key string) (*Object, error)
	// UploadObject stream upload an object
	UploadObject(ctx context.Context, i UploadObjectInput) error
	// DeleteObject delete an object
	DeleteObject(ctx context.Context, key string) error

	// ListPrefix list all objects with same prefix, and call WalkFunc for each object.
	ListPrefix(ctx context.Context, prefix string, recursive bool) (ObjectIterator, error)

	// BucketExist use a prefix to chack if bucket exist.
	// Using a prefix to confirm whether a bucket exists can avoid requesting the head Bucket permission.
	BucketExist(ctx context.Context, prefix string) (bool, error)
	// CreateBucket create a bucket.
	CreateBucket(ctx context.Context) error
}
