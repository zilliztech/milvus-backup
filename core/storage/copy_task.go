package storage

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/log"
)

type CopyPrefixOpt struct {
	Src  Client
	Dest Client

	SrcPrefix  string
	DestPrefix string

	Sem *semaphore.Weighted

	TraceFn TraceFn

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

		copier: newCopier(opt.Src, opt.Dest, opt.CopyByServer, copierOpt{traceFn: opt.TraceFn}),

		logger: log.L().With(zap.String("src", opt.SrcPrefix), zap.String("dest", opt.DestPrefix)),
	}
}

func (c *CopyPrefixTask) copy(ctx context.Context, src ObjectAttr) error {
	destKey := strings.Replace(src.Key, c.opt.SrcPrefix, c.opt.DestPrefix, 1)
	attr := CopyAttr{Src: src, DestKey: destKey}

	if err := c.copier.copy(ctx, attr); err != nil {
		return fmt.Errorf("storage: copy prefix %w", err)
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

		copier: newCopier(opt.Src, opt.Dest, opt.CopyByServer, copierOpt{}),
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
