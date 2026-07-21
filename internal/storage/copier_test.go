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

	"github.com/zilliztech/milvus-backup/internal/cfg"
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

func TestDirectCopier_Copy(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		dest := NewMockClient(t)
		src := NewMockClient(t)
		rp := &directCopier{src: src, dest: dest, logger: zap.NewNop()}

		in := CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		dest.EXPECT().CopyObject(mock.Anything, in).Return(nil).Once()

		attr := CopyAttr{Src: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		err := rp.copy(context.Background(), attr)
		assert.NoError(t, err)
	})

	t.Run("CopyObjectError", func(t *testing.T) {
		dest := NewMockClient(t)
		src := NewMockClient(t)
		rp := &directCopier{src: src, dest: dest, logger: zap.NewNop()}

		in := CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		dest.EXPECT().CopyObject(mock.Anything, in).Return(assert.AnError).Once()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		attr := CopyAttr{Src: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		err := rp.copy(ctx, attr)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "copy object")
	})

	t.Run("RetryThenSuccess", func(t *testing.T) {
		dest := NewMockClient(t)
		src := NewMockClient(t)
		rp := &directCopier{src: src, dest: dest, logger: zap.NewNop()}

		in := CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		dest.EXPECT().CopyObject(mock.Anything, in).Return(assert.AnError).Once()
		dest.EXPECT().CopyObject(mock.Anything, in).Return(nil).Once()

		attr := CopyAttr{Src: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		err := rp.copy(context.Background(), attr)
		assert.NoError(t, err)
	})

	t.Run("SuccessWithTraceFn", func(t *testing.T) {
		dest := NewMockClient(t)
		src := NewMockClient(t)

		var traced bool
		rp := &directCopier{src: src, dest: dest, logger: zap.NewNop(), opt: copierOpt{
			traceFn: func(size int64, cost time.Duration) {
				traced = true
				assert.Equal(t, int64(5), size)
			},
		}}

		in := CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		dest.EXPECT().CopyObject(mock.Anything, in).Return(nil).Once()

		attr := CopyAttr{Src: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		err := rp.copy(context.Background(), attr)
		assert.NoError(t, err)
		assert.True(t, traced)
	})

	t.Run("ErrorNoTraceFn", func(t *testing.T) {
		dest := NewMockClient(t)
		src := NewMockClient(t)

		var traced bool
		rp := &directCopier{src: src, dest: dest, logger: zap.NewNop(), opt: copierOpt{
			traceFn: func(size int64, cost time.Duration) {
				traced = true
			},
		}}

		in := CopyObjectInput{SrcCli: src, SrcAttr: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		dest.EXPECT().CopyObject(mock.Anything, in).Return(assert.AnError).Once()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		attr := CopyAttr{Src: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		err := rp.copy(ctx, attr)
		assert.Error(t, err)
		assert.False(t, traced)
	})
}

func TestStreamingCopier_Copy(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		dest := NewMockClient(t)
		src := NewMockClient(t)
		sp := &streamingCopier{src: src, dest: dest, logger: zap.NewNop()}

		body := io.NopCloser(bytes.NewReader([]byte("hello")))
		obj := &Object{Body: body, Length: 5}
		src.EXPECT().GetObject(mock.Anything, "a/b").Return(obj, nil).Once()

		i := UploadObjectInput{Body: body, Key: "c/d", Size: 5}
		dest.EXPECT().UploadObject(mock.Anything, i).Return(nil).Once()

		attr := CopyAttr{Src: ObjectAttr{Key: "a/b", Length: 5}, DestKey: "c/d"}
		err := sp.copy(context.Background(), attr)
		assert.NoError(t, err)
	})
}

func TestCopyThroughProcess(t *testing.T) {
	newClients := func(srcCfg, destCfg Config) (Client, Client) {
		src := NewMockClient(t)
		dest := NewMockClient(t)
		src.EXPECT().Config().Return(srcCfg).Maybe()
		dest.EXPECT().Config().Return(destCfg).Maybe()
		return src, dest
	}

	t.Run("explicit_modes", func(t *testing.T) {
		src, dest := newClients(Config{}, Config{})
		assert.False(t, CopyThroughProcess(cfg.TransferModeDirect, src, dest))
		assert.True(t, CopyThroughProcess(cfg.TransferModeStreaming, src, dest))
	})

	t.Run("auto_direct_for_same_backend", func(t *testing.T) {
		src, dest := newClients(
			Config{Provider: cfg.Minio, Endpoint: "minio:9000"},
			Config{Provider: cfg.Minio, Endpoint: "minio:9000"},
		)
		assert.False(t, CopyThroughProcess(cfg.TransferModeAuto, src, dest))
	})

	t.Run("auto_streams_for_different_endpoint", func(t *testing.T) {
		src, dest := newClients(
			Config{Provider: cfg.Minio, Endpoint: "minio-a:9000"},
			Config{Provider: cfg.Minio, Endpoint: "minio-b:9000"},
		)
		assert.True(t, CopyThroughProcess(cfg.TransferModeAuto, src, dest))
	})

	t.Run("auto_streams_for_different_provider", func(t *testing.T) {
		src, dest := newClients(
			Config{Provider: cfg.Minio, Endpoint: "storage:9000"},
			Config{Provider: cfg.S3, Endpoint: "storage:9000"},
		)
		assert.True(t, CopyThroughProcess(cfg.TransferModeAuto, src, dest))
	})

	t.Run("auto_streams_for_different_azure_account", func(t *testing.T) {
		src, dest := newClients(
			Config{Provider: cfg.CloudProviderAzure, Endpoint: "core.windows.net:443", UseSSL: true, Credential: Credential{AzureAccountName: "account-a"}},
			Config{Provider: cfg.CloudProviderAzure, Endpoint: "core.windows.net:443", UseSSL: true, Credential: Credential{AzureAccountName: "account-b"}},
		)
		assert.True(t, CopyThroughProcess(cfg.TransferModeAuto, src, dest))
	})
}
