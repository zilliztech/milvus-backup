package storage

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestTrackReader_Read(t *testing.T) {
	t.Run("ReadGTZero", func(t *testing.T) {
		tr := newTrackReader(bytes.NewReader([]byte("hello")), func(size int64, cost time.Duration) {
			assert.Equal(t, int64(5), size)
			assert.True(t, cost > 0)
		})

		buf := make([]byte, 10)
		n, err := tr.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 5, n)
	})

	t.Run("ReadZero", func(t *testing.T) {
		tr := newTrackReader(bytes.NewReader([]byte("hello")), func(size int64, cost time.Duration) {
			assert.Fail(t, "should not be called")
		})

		buf := make([]byte, 0)
		n, err := tr.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 0, n)
	})
}

func TestRemoteCopier_Copy(t *testing.T) {
	dest := NewMockClient(t)
	src := NewMockClient(t)
	rp := &remoteCopier{src: src, dest: dest, logger: zap.NewNop()}

	in := CopyObjectInput{SrcCli: src, SrcKey: "a/b", DestKey: "c/d"}
	dest.EXPECT().CopyObject(mock.Anything, in).Return(nil).Once()

	attr := CopyAttr{Src: ObjectAttr{Key: "a/b"}, DestKey: "c/d"}
	err := rp.copy(context.Background(), attr)
	assert.NoError(t, err)
}

func TestServerCopier_Copy(t *testing.T) {
	dest := NewMockClient(t)
	src := NewMockClient(t)
	sp := &serverCopier{src: src, dest: dest, logger: zap.NewNop()}

	body := io.NopCloser(bytes.NewReader([]byte("hello")))
	obj := &Object{Body: body, Length: 5}
	src.EXPECT().GetObject(mock.Anything, "a/b").Return(obj, nil).Once()

	i := UploadObjectInput{Body: body, Key: "c/d", Size: 5}
	dest.EXPECT().UploadObject(mock.Anything, i).Return(nil).Once()
	attr := CopyAttr{Src: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
	err := sp.copy(context.Background(), attr)
	assert.NoError(t, err)
}
