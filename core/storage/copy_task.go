package storage

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

type OnSuccessFn func(copyAttr CopyAttr)

type CopyAttr struct {
	Src     ObjectAttr
	DestKey string
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

	logger *zap.Logger
}

func (sc *serverCopier) copy(ctx context.Context, copyAttr CopyAttr) error {
	sc.logger.Debug("copy object", zap.String("src_key", copyAttr.Src.Key), zap.String("dest_key", copyAttr.DestKey))
	obj, err := sc.src.GetObject(ctx, copyAttr.Src.Key)
	if err != nil {
		return fmt.Errorf("storage: server copier get object %w", err)
	}
	defer obj.Body.Close()

	i := UploadObjectInput{Body: obj.Body, Key: copyAttr.DestKey, Size: copyAttr.Src.Length}
	if err := sc.dest.UploadObject(ctx, i); err != nil {
		return fmt.Errorf("storage: copier upload object %w", err)
	}

	return nil
}

func newCopier(src, dest Client, copyByServer bool) copier {
	logger := log.L().With(
		zap.String("src", src.Config().Bucket),
		zap.String("dest", dest.Config().Bucket),
	)
	if copyByServer {
		return &serverCopier{src: src, dest: dest, logger: logger.With(zap.String("copier", "server"))}
	}
	return &remoteCopier{src: src, dest: dest, logger: logger.With(zap.String("copier", "remote"))}
}

type CopyPrefixOpt struct {
	Src  Client
	Dest Client

	SrcPrefix  string
	DestPrefix string

	Sem *semaphore.Weighted

	OnSuccess OnSuccessFn

	CopyByServer bool
}

type CopyPrefixTask struct {
	opt CopyPrefixOpt

	copier copier

	logger *zap.Logger
}

func NewCopyPrefixTask(opt CopyPrefixOpt) *CopyPrefixTask {
	return &CopyPrefixTask{
		opt: opt,

		copier: newCopier(opt.Src, opt.Dest, opt.CopyByServer),

		logger: log.L().With(zap.String("src", opt.SrcPrefix), zap.String("dest", opt.DestPrefix)),
	}
}

func (c *CopyPrefixTask) copy(ctx context.Context, src ObjectAttr) error {
	destKey := strings.Replace(src.Key, c.opt.SrcPrefix, c.opt.DestPrefix, 1)
	attr := CopyAttr{Src: src, DestKey: destKey}

	if err := retry.Do(ctx, func() error { return c.copier.copy(ctx, attr) }); err != nil {
		return fmt.Errorf("storage: copy prefix %w", err)
	}

	if c.opt.OnSuccess != nil {
		c.opt.OnSuccess(attr)
	}

	return nil
}

func (c *CopyPrefixTask) Execute(ctx context.Context) error {
	c.logger.Info("start copy prefix")
	iter, err := c.opt.Src.ListPrefix(ctx, c.opt.SrcPrefix, true)
	if err != nil {
		return fmt.Errorf("storage: copy prefix walk prefix %w", err)
	}

	g, subCtx := errgroup.WithContext(ctx)
	for iter.HasNext() {
		attr, err := iter.Next()
		if err != nil {
			return fmt.Errorf("storage: copy prefix iter object %w", err)
		}
		if attr.IsEmpty() && strings.HasSuffix(attr.Key, "/") {
			continue
		}

		if err := c.opt.Sem.Acquire(subCtx, 1); err != nil {
			return fmt.Errorf("storage: copy prefix acquire semaphore %w", err)
		}
		g.Go(func() error {
			defer c.opt.Sem.Release(1)

			if err := c.copy(subCtx, attr); err != nil {
				return fmt.Errorf("storage: copy prefix %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("storage: copy prefix %w", err)
	}

	return nil
}

type CopyObjectsOpt struct {
	Src  Client
	Dest Client

	Attrs []CopyAttr

	Sem *semaphore.Weighted

	CopyByServer bool
}

type CopyObjectsTask struct {
	opt CopyObjectsOpt

	copier copier
}

func NewCopyObjectsTask(opt CopyObjectsOpt) *CopyObjectsTask {
	return &CopyObjectsTask{
		opt: opt,

		copier: newCopier(opt.Src, opt.Dest, opt.CopyByServer),
	}
}

func (c *CopyObjectsTask) Prepare(_ context.Context) error {
	return nil
}

func (c *CopyObjectsTask) Execute(ctx context.Context) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, attr := range c.opt.Attrs {
		if err := c.opt.Sem.Acquire(subCtx, 1); err != nil {
			return fmt.Errorf("storage: copy objects acquire semaphore %w", err)
		}
		g.Go(func() error {
			defer c.opt.Sem.Release(1)

			if err := c.copier.copy(subCtx, attr); err != nil {
				return fmt.Errorf("storage: copy objects %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("storage: copy objects %w", err)
	}

	return nil
}
