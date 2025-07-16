package storage

import (
	"context"
	"fmt"
	"io"
	"time"

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

// remoteCopier copy data from src to dest by calling dest.CopyObject
type remoteCopier struct {
	src  Client
	dest Client

	logger *zap.Logger
}

func (rp *remoteCopier) copy(ctx context.Context, copyAttr CopyAttr) error {
	rp.logger.Debug("copy object", zap.String("src", copyAttr.Src.Key), zap.String("dest", copyAttr.DestKey))
	i := CopyObjectInput{SrcCli: rp.src, SrcKey: copyAttr.Src.Key, DestKey: copyAttr.DestKey}
	if err := rp.dest.CopyObject(ctx, i); err != nil {
		return fmt.Errorf("storage: remote copier copy object %w", err)
	}

	return nil
}

// serverCopier copy data from src to dest by backup server
type serverCopier struct {
	src  Client
	dest Client

	opt copierOpt

	logger *zap.Logger
}

func (sc *serverCopier) copy(ctx context.Context, copyAttr CopyAttr) error {
	sc.logger.Debug("copy object", zap.String("src_key", copyAttr.Src.Key), zap.String("dest_key", copyAttr.DestKey))

	return retry.Do(ctx, func() error {
		obj, err := sc.src.GetObject(ctx, copyAttr.Src.Key)
		if err != nil {
			return fmt.Errorf("storage: server copier get object %w", err)
		}
		defer obj.Body.Close()

		body := io.Reader(obj.Body)
		if sc.opt.traceFn != nil {
			body = newTrackReader(obj.Body, sc.opt.traceFn)
		}

		i := UploadObjectInput{Body: body, Key: copyAttr.DestKey, Size: copyAttr.Src.Length}
		if err := sc.dest.UploadObject(ctx, i); err != nil {
			return fmt.Errorf("storage: copier upload object %w", err)
		}

		return nil
	})
}

func newCopier(src, dest Client, copyByServer bool, opt copierOpt) copier {
	logger := log.L().With(
		zap.String("src", src.Config().Bucket),
		zap.String("dest", dest.Config().Bucket),
	)
	if copyByServer {
		return &serverCopier{src: src, dest: dest, opt: opt, logger: logger.With(zap.String("copier", "server"))}
	}
	return &remoteCopier{src: src, dest: dest, logger: logger.With(zap.String("copier", "remote"))}
}
