package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestRemoteCopier_copy(t *testing.T) {
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
