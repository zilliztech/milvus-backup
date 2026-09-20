package storage

import (
	"bytes"
	"context"
	"io"
	"iter"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// seqCountingYielded wraps a sequence, recording how many objects it yielded
// before the consumer stopped ranging: the count locks that a consumer which
// returns early really stops the listing.
func seqCountingYielded(objs []ObjectAttr, yielded *int) iter.Seq2[ObjectAttr, error] {
	return func(yield func(ObjectAttr, error) bool) {
		for _, obj := range objs {
			*yielded++
			if !yield(obj, nil) {
				return
			}
		}
	}
}

// seqFailing fails the listing on its first iteration.
func seqFailing(err error) iter.Seq2[ObjectAttr, error] {
	return func(yield func(ObjectAttr, error) bool) {
		yield(ObjectAttr{}, err)
	}
}

// seqFailingAfter yields objs, then a listing error, so the consumer returns
// before the listing is drained.
func seqFailingAfter(objs []ObjectAttr, err error) iter.Seq2[ObjectAttr, error] {
	return func(yield func(ObjectAttr, error) bool) {
		for _, obj := range objs {
			if !yield(obj, nil) {
				return
			}
		}
		yield(ObjectAttr{}, err)
	}
}

func TestSize(t *testing.T) {
	cli := NewMockClient(t)

	objs := []ObjectAttr{
		{Key: "a/b/c", Length: 1},
		{Key: "a/b/d", Length: 2},
		{Key: "a/b/e", Length: 3},
		{Key: "a/b/f", Length: 4},
	}

	iter := NewMockObjectIterator(objs)
	cli.EXPECT().
		NewObjectIter(context.Background(), "a/b/", true).
		Return(iter)

	size, err := Size(context.Background(), cli, "a/b/")
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
}

func TestListPrefixFlat(t *testing.T) {
	cli := NewMockClient(t)

	objs := []ObjectAttr{
		{Key: "a/b/c", Length: 1},
		{Key: "a/b/d", Length: 2},
		{Key: "a/b/e", Length: 3},
		{Key: "a/b/f", Length: 4},
	}

	iter := NewMockObjectIterator(objs)
	cli.EXPECT().
		NewObjectIter(context.Background(), "a/b", true).
		Return(iter)

	keys, sizes, err := ListPrefixFlat(context.Background(), cli, "a/b", true)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a/b/c", "a/b/d", "a/b/e", "a/b/f"}, keys)
	assert.Equal(t, []int64{1, 2, 3, 4}, sizes)
}

func TestDeletePrefix(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		cli := NewMockClient(t)

		objs := []ObjectAttr{
			{Key: "a/b/c", Length: 1},
			{Key: "a/b/d", Length: 2},
			{Key: "a/b/e", Length: 3},
			{Key: "a/b/f", Length: 4},
		}

		iter := NewMockObjectIterator(objs)
		cli.EXPECT().
			NewObjectIter(mock.Anything, "a/b", true).
			Return(iter)

		for _, obj := range objs {
			cli.EXPECT().
				DeleteObject(mock.Anything, obj.Key).
				Return(nil)
		}

		err := DeletePrefix(context.Background(), cli, "a/b")
		assert.NoError(t, err)
	})

	t.Run("StopsInflightDeletesOnIterError", func(t *testing.T) {
		// A raw mock, not NewMockClient, so a leaked goroutine holding the mock
		// lock on a never-released delete cannot deadlock the test cleanup;
		// without the errgroup fix the assertion below fails instead.
		cli := &MockClient{}

		released := make(chan struct{})
		var once sync.Once
		// A delete that blocks until its context is canceled. If DeletePrefix
		// fails to stop the errgroup on early return, the deferred Wait blocks
		// forever and the test hangs.
		cli.EXPECT().
			DeleteObject(mock.MatchedBy(func(ctx context.Context) bool {
				<-ctx.Done()
				once.Do(func() { close(released) })
				return true
			}), "a/b/c").
			Return(nil)

		// The sequence yields one object, then a listing error, so DeletePrefix
		// returns before the listing is drained.
		cli.EXPECT().
			NewObjectIter(mock.Anything, "a/b", true).
			Return(seqFailingAfter([]ObjectAttr{{Key: "a/b/c", Length: 1}}, assert.AnError))

		err := DeletePrefix(context.Background(), cli, "a/b")
		assert.Error(t, err)

		// The in-flight delete was canceled and joined before returning.
		select {
		case <-released:
		default:
			assert.Fail(t, "DeletePrefix returned without stopping the in-flight delete")
		}
	})

	t.Run("EmptyPrefix", func(t *testing.T) {
		cli := NewMockClient(t)
		err := DeletePrefix(context.Background(), cli, "")
		assert.Error(t, err)
	})
}

func TestExist(t *testing.T) {
	t.Run("Exist", func(t *testing.T) {
		cli := NewMockClient(t)

		objs := []ObjectAttr{
			{Key: "a/b/c", Length: 1},
			{Key: "a/b/d", Length: 2},
			{Key: "a/b/e", Length: 3},
			{Key: "a/b/f", Length: 4},
		}

		var yielded int
		cli.EXPECT().
			NewObjectIter(mock.Anything, "a/b", false).
			Return(seqCountingYielded(objs, &yielded))

		exist, err := Exist(context.Background(), cli, "a/b")
		assert.NoError(t, err)
		assert.True(t, exist)
		assert.Equal(t, 1, yielded, "Exist must stop the listing after the first object")
	})

	t.Run("NotExist", func(t *testing.T) {
		cli := NewMockClient(t)

		iter := NewMockObjectIterator(nil)
		cli.EXPECT().
			NewObjectIter(mock.Anything, "a/b", false).
			Return(iter)

		exist, err := Exist(context.Background(), cli, "a/b")
		assert.NoError(t, err)
		assert.False(t, exist)
	})
}

func TestCreateBucketIfNotExist(t *testing.T) {
	t.Run("BucketExists", func(t *testing.T) {
		cli := NewMockClient(t)
		cli.EXPECT().BucketExist(mock.Anything, "").Return(true, nil)

		err := CreateBucketIfNotExist(context.Background(), cli, "")
		assert.NoError(t, err)
	})

	t.Run("BucketNotExistThenCreate", func(t *testing.T) {
		cli := NewMockClient(t)
		cli.EXPECT().BucketExist(mock.Anything, "").Return(false, nil)
		cli.EXPECT().CreateBucket(mock.Anything).Return(nil)

		err := CreateBucketIfNotExist(context.Background(), cli, "")
		assert.NoError(t, err)
	})

	t.Run("BucketExistError", func(t *testing.T) {
		cli := NewMockClient(t)
		cli.EXPECT().BucketExist(mock.Anything, "").Return(false, assert.AnError)

		err := CreateBucketIfNotExist(context.Background(), cli, "")
		assert.Error(t, err)
	})
}

func TestRead(t *testing.T) {
	cli := NewMockClient(t)

	cli.EXPECT().
		GetObject(mock.Anything, "a/b").
		Return(&Object{Length: 5, Body: io.NopCloser(bytes.NewReader([]byte("hello")))}, nil)

	data, err := Read(context.Background(), cli, "a/b")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), data)
	assert.Equal(t, 5, len(data))
}

func TestWrite(t *testing.T) {
	cli := NewMockClient(t)

	cli.EXPECT().
		UploadObject(mock.Anything, mock.Anything).
		Return(nil)

	err := Write(context.Background(), cli, "a/b", []byte("hello"))
	assert.NoError(t, err)
}
