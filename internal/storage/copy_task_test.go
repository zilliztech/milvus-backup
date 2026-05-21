package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/retry"
)

// errorIterator reports a next item but always fails when read.
type errorIterator struct{}

func (errorIterator) HasNext() bool             { return true }
func (errorIterator) Next() (ObjectAttr, error) { return ObjectAttr{}, assert.AnError }

func TestCopyPrefixTask_Execute(t *testing.T) {
	t.Run("CopiesAllAndRemapsKeys", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()

		objs := []ObjectAttr{
			{Key: "src/a", Length: 1},
			{Key: "src/b/c", Length: 2},
			{Key: "src/dir/", Length: 0}, // directory marker, must be skipped
		}
		src.EXPECT().ListPrefix(mock.Anything, "src/", true).
			Return(&mockObjectIterator{objs: objs}, nil).Once()
		dest.EXPECT().CopyObject(mock.Anything, CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "src/a", Length: 1}, DestKey: "dest/a"}).Return(nil).Once()
		dest.EXPECT().CopyObject(mock.Anything, CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "src/b/c", Length: 2}, DestKey: "dest/b/c"}).Return(nil).Once()

		task := NewCopyPrefixTask(CopyPrefixOpt{Src: src, Dest: dest, SrcPrefix: "src/", DestPrefix: "dest/", Sem: semaphore.NewWeighted(2)})
		assert.NoError(t, task.Execute(context.Background()))
	})

	t.Run("ListError", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()
		src.EXPECT().ListPrefix(mock.Anything, "src/", true).Return(nil, assert.AnError).Once()

		task := NewCopyPrefixTask(CopyPrefixOpt{Src: src, Dest: dest, SrcPrefix: "src/", DestPrefix: "dest/", Sem: semaphore.NewWeighted(2)})
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "walk prefix")
	})

	t.Run("CopyError", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()
		objs := []ObjectAttr{{Key: "src/a", Length: 1}}
		src.EXPECT().ListPrefix(mock.Anything, "src/", true).Return(&mockObjectIterator{objs: objs}, nil).Once()
		dest.EXPECT().CopyObject(mock.Anything, mock.Anything).Return(retry.Unrecoverable(assert.AnError)).Once()

		task := NewCopyPrefixTask(CopyPrefixOpt{Src: src, Dest: dest, SrcPrefix: "src/", DestPrefix: "dest/", Sem: semaphore.NewWeighted(1)})
		assert.Error(t, task.Execute(context.Background()))
	})
}

func TestCopyObjectsTask_Execute(t *testing.T) {
	t.Run("CopiesAllObjects", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()

		attrs := []CopyAttr{
			{Src: ObjectAttr{Key: "a", Length: 1}, DestKey: "x"},
			{Src: ObjectAttr{Key: "b", Length: 2}, DestKey: "y"},
		}
		dest.EXPECT().CopyObject(mock.Anything, CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "a", Length: 1}, DestKey: "x"}).Return(nil).Once()
		dest.EXPECT().CopyObject(mock.Anything, CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "b", Length: 2}, DestKey: "y"}).Return(nil).Once()

		task := NewCopyObjectsTask(CopyObjectsOpt{Src: src, Dest: dest, Attrs: attrs, Sem: semaphore.NewWeighted(2)})
		assert.NoError(t, task.Execute(context.Background()))
	})

	t.Run("CopyByServerUploadsAll", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()

		body := io.NopCloser(bytes.NewReader([]byte("hi")))
		src.EXPECT().GetObject(mock.Anything, "a").Return(&Object{Body: body, Length: 2}, nil).Once()
		dest.EXPECT().UploadObject(mock.Anything, UploadObjectInput{Body: body, Key: "x", Size: 2}).Return(nil).Once()

		attrs := []CopyAttr{{Src: ObjectAttr{Key: "a", Length: 2}, DestKey: "x"}}
		task := NewCopyObjectsTask(CopyObjectsOpt{Src: src, Dest: dest, Attrs: attrs, Sem: semaphore.NewWeighted(1), CopyByServer: true})
		assert.NoError(t, task.Execute(context.Background()))
	})

	t.Run("CopyError", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()

		attrs := []CopyAttr{{Src: ObjectAttr{Key: "a", Length: 1}, DestKey: "x"}}
		dest.EXPECT().CopyObject(mock.Anything, mock.Anything).Return(retry.Unrecoverable(assert.AnError)).Once()

		task := NewCopyObjectsTask(CopyObjectsOpt{Src: src, Dest: dest, Attrs: attrs, Sem: semaphore.NewWeighted(1)})
		assert.Error(t, task.Execute(context.Background()))
	})
}
