package storage

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/retry"
)

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
		src.EXPECT().NewObjectIter(mock.Anything, "src/", true).
			Return(NewMockObjectIterator(objs)).Once()
		dest.EXPECT().CopyObject(mock.Anything, CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "src/a", Length: 1}, DestKey: "dest/a"}).Return(nil).Once()
		dest.EXPECT().CopyObject(mock.Anything, CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "src/b/c", Length: 2}, DestKey: "dest/b/c"}).Return(nil).Once()

		task := NewCopyPrefixTask(CopyPrefixOpt{Src: src, Dest: dest, SrcPrefix: "src/", DestPrefix: "dest/", Sem: semaphore.NewWeighted(2)})
		assert.NoError(t, task.Execute(context.Background()))
	})

	t.Run("IterError", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()
		src.EXPECT().NewObjectIter(mock.Anything, "src/", true).Return(seqFailing(assert.AnError)).Once()

		task := NewCopyPrefixTask(CopyPrefixOpt{Src: src, Dest: dest, SrcPrefix: "src/", DestPrefix: "dest/", Sem: semaphore.NewWeighted(2)})
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "iter object")
	})

	t.Run("CopyError", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()
		objs := []ObjectAttr{{Key: "src/a", Length: 1}}
		src.EXPECT().NewObjectIter(mock.Anything, "src/", true).Return(NewMockObjectIterator(objs)).Once()
		dest.EXPECT().CopyObject(mock.Anything, mock.Anything).Return(retry.Unrecoverable(assert.AnError)).Once()

		task := NewCopyPrefixTask(CopyPrefixOpt{Src: src, Dest: dest, SrcPrefix: "src/", DestPrefix: "dest/", Sem: semaphore.NewWeighted(1)})
		assert.Error(t, task.Execute(context.Background()))
	})

	t.Run("StopsInflightCopiesOnIterError", func(t *testing.T) {
		// Raw mocks, not NewMockClient, so a leaked goroutine holding a mock
		// lock on a never-released copy cannot deadlock the test cleanup;
		// without the errgroup fix the assertion below fails instead.
		src := &MockClient{}
		dest := &MockClient{}
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()

		released := make(chan struct{})
		var once sync.Once
		// A copy that blocks until its context is canceled. The release can
		// only happen through Execute's own deferred cancel, so this test
		// verifies in-flight copies are stopped on early return.
		dest.EXPECT().CopyObject(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, _ CopyObjectInput) error {
				<-ctx.Done()
				once.Do(func() { close(released) })
				return ctx.Err()
			}).Once()

		// The sequence yields one object, then a listing error, so Execute
		// returns before the listing is drained.
		src.EXPECT().NewObjectIter(mock.Anything, "src/", true).
			Return(seqFailingAfter([]ObjectAttr{{Key: "src/a", Length: 1}}, assert.AnError)).Once()

		task := NewCopyPrefixTask(CopyPrefixOpt{Src: src, Dest: dest, SrcPrefix: "src/", DestPrefix: "dest/", Sem: semaphore.NewWeighted(2)})
		err := task.Execute(context.Background())
		assert.Error(t, err)

		select {
		case <-released:
		default:
			assert.Fail(t, "CopyPrefixTask returned without stopping the in-flight copy")
		}
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

	t.Run("StreamingUploadsAll", func(t *testing.T) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(Config{Bucket: "src"}).Maybe()
		dest.EXPECT().Config().Return(Config{Bucket: "dest"}).Maybe()

		body := io.NopCloser(bytes.NewReader([]byte("hi")))
		src.EXPECT().GetObject(mock.Anything, "a").Return(&Object{Body: body, Length: 2}, nil).Once()
		dest.EXPECT().UploadObject(mock.Anything, UploadObjectInput{Body: body, Key: "x", Size: 2}).Return(nil).Once()

		attrs := []CopyAttr{{Src: ObjectAttr{Key: "a", Length: 2}, DestKey: "x"}}
		task := NewCopyObjectsTask(CopyObjectsOpt{Src: src, Dest: dest, Attrs: attrs, Sem: semaphore.NewWeighted(1), Streaming: true})
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
