package storage

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockObjectIterator struct {
	objs []ObjectAttr
	idx  int

	closed bool
}

func (m *mockObjectIterator) Next(_ context.Context) (ObjectAttr, bool, error) {
	if m.idx >= len(m.objs) {
		return ObjectAttr{}, false, nil
	}
	obj := m.objs[m.idx]
	m.idx++
	return obj, true, nil
}

func (m *mockObjectIterator) Close() error {
	m.closed = true
	return nil
}

// iterWithError yields objs, then fails the next read so the consumer returns
// before the listing is drained.
type iterWithError struct {
	objs   []ObjectAttr
	idx    int
	closed bool
}

func (m *iterWithError) Next(_ context.Context) (ObjectAttr, bool, error) {
	if m.idx < len(m.objs) {
		obj := m.objs[m.idx]
		m.idx++
		return obj, true, nil
	}
	return ObjectAttr{}, false, assert.AnError
}

func (m *iterWithError) Close() error {
	m.closed = true
	return nil
}

func TestSize(t *testing.T) {
	cli := NewMockClient(t)

	objs := []ObjectAttr{
		{Key: "a/b/c", Length: 1},
		{Key: "a/b/d", Length: 2},
		{Key: "a/b/e", Length: 3},
		{Key: "a/b/f", Length: 4},
	}

	iter := &mockObjectIterator{objs: objs}
	cli.EXPECT().
		ListPrefix(context.Background(), "a/b/", true).
		Return(iter, nil)

	size, err := Size(context.Background(), cli, "a/b/")
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
	assert.True(t, iter.closed, "Size must close the iterator")
}

func TestListPrefixFlat(t *testing.T) {
	cli := NewMockClient(t)

	objs := []ObjectAttr{
		{Key: "a/b/c", Length: 1},
		{Key: "a/b/d", Length: 2},
		{Key: "a/b/e", Length: 3},
		{Key: "a/b/f", Length: 4},
	}

	iter := &mockObjectIterator{objs: objs}
	cli.EXPECT().
		ListPrefix(context.Background(), "a/b", true).
		Return(iter, nil)

	keys, sizes, err := ListPrefixFlat(context.Background(), cli, "a/b", true)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a/b/c", "a/b/d", "a/b/e", "a/b/f"}, keys)
	assert.Equal(t, []int64{1, 2, 3, 4}, sizes)
	assert.True(t, iter.closed, "ListPrefixFlat must close the iterator")
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

		iter := &mockObjectIterator{objs: objs}
		cli.EXPECT().
			ListPrefix(mock.Anything, "a/b", true).
			Return(iter, nil)

		for _, obj := range objs {
			cli.EXPECT().
				DeleteObject(mock.Anything, obj.Key).
				Return(nil)
		}

		err := DeletePrefix(context.Background(), cli, "a/b")
		assert.NoError(t, err)
		assert.True(t, iter.closed, "DeletePrefix must close the iterator")
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

		// The iterator yields one object, then errors so DeletePrefix returns
		// before the listing is drained.
		iter := &iterWithError{objs: []ObjectAttr{{Key: "a/b/c", Length: 1}}}
		cli.EXPECT().
			ListPrefix(mock.Anything, "a/b", true).
			Return(iter, nil)

		err := DeletePrefix(context.Background(), cli, "a/b")
		assert.Error(t, err)
		assert.True(t, iter.closed, "DeletePrefix must close the iterator on early return")

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

		iter := &mockObjectIterator{objs: objs}
		cli.EXPECT().
			ListPrefix(mock.Anything, "a/b", false).
			Return(iter, nil)

		exist, err := Exist(context.Background(), cli, "a/b")
		assert.NoError(t, err)
		assert.True(t, exist)
		assert.True(t, iter.closed, "Exist must close the iterator even after a single read")
	})

	t.Run("NotExist", func(t *testing.T) {
		cli := NewMockClient(t)

		iter := &mockObjectIterator{}
		cli.EXPECT().
			ListPrefix(mock.Anything, "a/b", false).
			Return(iter, nil)

		exist, err := Exist(context.Background(), cli, "a/b")
		assert.NoError(t, err)
		assert.False(t, exist)
		assert.True(t, iter.closed, "Exist must close the iterator")
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
