package storage

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/retry"
)

type CopyAttr struct {
	Src     ObjectAttr
	DestKey string
}

type copierOpt struct {
	traceFn TraceFn
}

// TraceFn reports copied bytes. The size is all a consumer needs: progress
// state accumulates sizes, and renderers derive speed from their own ticks.
type TraceFn func(size int64)

type trackReader struct {
	inner io.Reader

	traceFn TraceFn
}

func newTrackReader(inner io.Reader, traceFn TraceFn) *trackReader {
	return &trackReader{inner: inner, traceFn: traceFn}
}

func (t *trackReader) Read(p []byte) (int, error) {
	n, err := t.inner.Read(p)
	if n > 0 {
		t.traceFn(int64(n))
	}
	return n, err
}

type copier interface {
	copy(ctx context.Context, copyAttr CopyAttr) error
}

// directCopier copies data with the destination storage's native COPY API,
// so the object never passes through this process.
type directCopier struct {
	src  Client
	dest Client

	opt copierOpt

	logger *zap.Logger
}

func (dc *directCopier) copy(ctx context.Context, copyAttr CopyAttr) error {
	dc.logger.Debug("copy object", zap.String("src", copyAttr.Src.Key), zap.String("dest", copyAttr.DestKey))

	err := retry.Do(ctx, func() error {
		i := CopyObjectInput{SrcCli: dc.src, SrcAttr: copyAttr.Src, DestKey: copyAttr.DestKey}

		if err := dc.dest.CopyObject(ctx, i); err != nil {
			return fmt.Errorf("storage: direct copier copy object: %w", err)
		}

		return nil
	})

	if err == nil && dc.opt.traceFn != nil {
		dc.opt.traceFn(copyAttr.Src.Length)
	}

	return err
}

// streamingCopier downloads from src and uploads to dest, so the object is
// streamed through this process.
type streamingCopier struct {
	src  Client
	dest Client

	opt copierOpt

	logger *zap.Logger
}

func (sc *streamingCopier) copy(ctx context.Context, copyAttr CopyAttr) error {
	sc.logger.Debug("copy object", zap.String("src_key", copyAttr.Src.Key), zap.String("dest_key", copyAttr.DestKey))

	return retry.Do(ctx, func() error {
		obj, err := sc.src.GetObject(ctx, copyAttr.Src.Key)
		if err != nil {
			return fmt.Errorf("storage: streaming copier get object: %w", err)
		}
		defer obj.Body.Close()

		body := io.Reader(obj.Body)
		if sc.opt.traceFn != nil {
			body = newTrackReader(obj.Body, sc.opt.traceFn)
		}

		i := UploadObjectInput{Body: body, Key: copyAttr.DestKey, Size: copyAttr.Src.Length}
		if err := sc.dest.UploadObject(ctx, i); err != nil {
			return fmt.Errorf("storage: streaming copier upload object: %w", err)
		}

		return nil
	})
}

func newCopier(src, dest Client, streaming bool, opt copierOpt) copier {
	logger := log.L().With(
		zap.String("src", src.Config().Bucket),
		zap.String("dest", dest.Config().Bucket),
	)

	if streaming {
		return &streamingCopier{src: src, dest: dest, opt: opt, logger: logger.With(zap.String("copier", "streaming"))}
	}

	return &directCopier{src: src, dest: dest, opt: opt, logger: logger.With(zap.String("copier", "direct"))}
}
