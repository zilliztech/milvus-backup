package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockObjectIterator struct {
	objs []ObjectAttr
	idx  int
}

func (m *mockObjectIterator) HasNext() bool { return m.idx < len(m.objs) }

func (m *mockObjectIterator) Next() (ObjectAttr, error) {
	if !m.HasNext() {
		return ObjectAttr{}, nil
	}
	obj := m.objs[m.idx]
	m.idx++
	return obj, nil
}

func TestSize(t *testing.T) {
	cli := NewMockClient(t)

	objs := []ObjectAttr{
		{Key: "a/b/c", Length: 1},
		{Key: "a/b/d", Length: 2},
		{Key: "a/b/e", Length: 3},
		{Key: "a/b/f", Length: 4},
	}

	cli.EXPECT().
		ListPrefix(context.Background(), "a/b/", true).
		Return(&mockObjectIterator{objs: objs}, nil)

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

	cli.EXPECT().
		ListPrefix(context.Background(), "a/b", true).
		Return(&mockObjectIterator{objs: objs}, nil)

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

		cli.EXPECT().
			ListPrefix(mock.Anything, "a/b", true).
			Return(&mockObjectIterator{objs: objs}, nil)

		for _, obj := range objs {
			cli.EXPECT().
				DeleteObject(mock.Anything, obj.Key).
				Return(nil)
		}

		err := DeletePrefix(context.Background(), cli, "a/b")
		assert.NoError(t, err)
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

		cli.EXPECT().
			ListPrefix(mock.Anything, "a/b", false).
			Return(&mockObjectIterator{objs: objs}, nil)

		exist, err := Exist(context.Background(), cli, "a/b")
		assert.NoError(t, err)
		assert.True(t, exist)
	})

	t.Run("NotExist", func(t *testing.T) {
		cli := NewMockClient(t)

		cli.EXPECT().
			ListPrefix(mock.Anything, "a/b", false).
			Return(&mockObjectIterator{}, nil)

		exist, err := Exist(context.Background(), cli, "a/b")
		assert.NoError(t, err)
		assert.False(t, exist)
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
