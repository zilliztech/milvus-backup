package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/cfg"
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

type TraceFn func(size int64, cost time.Duration)

type trackReader struct {
	inner io.Reader

	lastRead time.Time
	traceFn  TraceFn
}

func newTrackReader(inner io.Reader, traceFn TraceFn) *trackReader {
	return &trackReader{inner: inner, traceFn: traceFn, lastRead: time.Now()}
}

func (t *trackReader) Read(p []byte) (int, error) {
	n, err := t.inner.Read(p)
	if n > 0 {
		t.traceFn(int64(n), time.Since(t.lastRead))
		t.lastRead = time.Now()
	}
	return n, err
}

type copier interface {
	copy(ctx context.Context, copyAttr CopyAttr) error
}

// directCopier copies data with the destination storage's native COPY API.
type directCopier struct {
	src  Client
	dest Client

	opt copierOpt

	logger *zap.Logger
}

func (rp *directCopier) copy(ctx context.Context, copyAttr CopyAttr) error {
	rp.logger.Debug("copy object", zap.String("src", copyAttr.Src.Key), zap.String("dest", copyAttr.DestKey))

	start := time.Now()
	err := retry.Do(ctx, func() error {
		i := CopyObjectInput{SrcCli: rp.src, SrcAttr: copyAttr.Src, DestKey: copyAttr.DestKey}

		if err := rp.dest.CopyObject(ctx, i); err != nil {
			return fmt.Errorf("storage: direct copier copy object %w", err)
		}

		return nil
	})

	if err == nil && rp.opt.traceFn != nil {
		rp.opt.traceFn(copyAttr.Src.Length, time.Since(start))
	}

	return err
}

// streamingCopier downloads from the source and uploads to the destination
// through the milvus-backup process.
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
			return fmt.Errorf("storage: streaming copier get object %w", err)
		}
		defer obj.Body.Close()

		body := io.Reader(obj.Body)
		if sc.opt.traceFn != nil {
			body = newTrackReader(obj.Body, sc.opt.traceFn)
		}

		i := UploadObjectInput{Body: body, Key: copyAttr.DestKey, Size: copyAttr.Src.Length}
		if err := sc.dest.UploadObject(ctx, i); err != nil {
			return fmt.Errorf("storage: streaming copier upload object %w", err)
		}

		return nil
	})
}

func SameBackend(left, right Config) bool {
	return left.Provider == right.Provider &&
		left.Endpoint == right.Endpoint &&
		left.UseSSL == right.UseSSL &&
		left.Region == right.Region &&
		left.Credential.AzureAccountName == right.Credential.AzureAccountName
}

func CopyThroughProcess(mode string, src, dest Client) bool {
	switch mode {
	case cfg.TransferModeDirect:
		return false
	case cfg.TransferModeStreaming:
		return true
	default:
		return !SameBackend(src.Config(), dest.Config())
	}
}

func newCopier(src, dest Client, copyThroughProcess bool, opt copierOpt) copier {
	logger := log.L().With(
		zap.String("src", src.Config().Bucket),
		zap.String("dest", dest.Config().Bucket),
	)

	if copyThroughProcess {
		return &streamingCopier{src: src, dest: dest, opt: opt, logger: logger.With(zap.String("copier", "streaming"))}
	}

	return &directCopier{src: src, dest: dest, opt: opt, logger: logger.With(zap.String("copier", "direct"))}
}
