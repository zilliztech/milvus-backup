package storage

import (
	"context"
	"io"
)

type FileReader interface {
	io.Reader
	io.Closer
}

type CopyObjectInput struct {
	SrcCli ChunkManager

	SrcBucket string
	SrcKey    string

	DestBucket string
	DestKey    string
}

type ListObjectPageInput struct {
	// The name of the bucket
	Bucket string
	// Limits the response to keys that begin with the specified prefix
	Prefix string
}

type GetObjectInput struct {
	Bucket string
	Key    string
}

type DeleteObjectsInput struct {
	Bucket string
	Keys   []string
}

type DeletePrefixInput struct {
	Bucket string
	Prefix string
}

type UploadObjectInput struct {
	Bucket string
	Key    string
	Body   io.Reader

	// The size of the file to be uploaded, if unknown, set to 0 or negative
	// Configuring this parameter can help reduce memory usage.
	Size int64
}

type SeekableReadCloser interface {
	io.ReaderAt
	io.Seeker
	io.ReadCloser
}

type Object struct {
	Length int64
	Body   SeekableReadCloser
}

type ObjectAttr struct {
	Key    string
	Length int64
}

func (o *ObjectAttr) IsEmpty() bool { return o.Length == 0 }

type Page struct {
	Contents []ObjectAttr
}

type ListObjectsPaginator interface {
	HasMorePages() bool
	NextPage(ctx context.Context) (*Page, error)
}

// Learn from file.ReadFile
func Read(r io.Reader, size int64) ([]byte, error) {
	data := make([]byte, 0, size)
	for {
		if len(data) >= cap(data) {
			d := append(data[:cap(data)], 0)
			data = d[:len(data)]
		}
		n, err := r.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, err
		}
	}
}
