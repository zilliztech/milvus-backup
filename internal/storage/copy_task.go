package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo/mutable"
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

	Streaming bool
}

type CopyPrefixTask struct {
	opt CopyPrefixOpt

	copier copier

	logger *zap.Logger
}

func NewCopyPrefixTask(opt CopyPrefixOpt) *CopyPrefixTask {
	return &CopyPrefixTask{
		opt: opt,

		copier: newCopier(opt.Src, opt.Dest, opt.Streaming, copierOpt{traceFn: opt.TraceFn}),

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
	defer iter.Close()

	// Derive a cancellable context so in-flight copies can be stopped when the
	// loop bails early, then join them through the single Wait below.
	// defer cancel() satisfies vet's lostcancel check; it is a no-op on the
	// happy path where Wait has already joined every copy.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, subCtx := errgroup.WithContext(ctx)

	var loopErr error
	for {
		attr, ok, err := iter.Next(ctx)
		if err != nil {
			loopErr = fmt.Errorf("storage: copy prefix iter object %w", err)
			break
		}
		if !ok {
			break
		}
		if attr.IsEmpty() && strings.HasSuffix(attr.Key, "/") {
			continue
		}

		if err := c.opt.Sem.Acquire(ctx, 1); err != nil {
			loopErr = fmt.Errorf("storage: copy prefix acquire semaphore %w", err)
			break
		}
		g.Go(func() error {
			defer c.opt.Sem.Release(1)

			if err := c.copy(subCtx, attr); err != nil {
				return fmt.Errorf("storage: copy prefix %w", err)
			}

			return nil
		})
	}

	// If the loop bailed early, stop the in-flight copies so Wait below
	// returns promptly instead of running them to completion.
	if loopErr != nil {
		cancel()
	}

	waitErr := g.Wait()
	if loopErr != nil {
		return loopErr
	}
	if waitErr != nil {
		return fmt.Errorf("storage: copy prefix %w", waitErr)
	}

	return nil
}

type CopyObjectsOpt struct {
	Src  Client
	Dest Client

	Attrs []CopyAttr

	Sem *semaphore.Weighted

	TraceFn TraceFn

	Streaming bool
}

type CopyObjectsTask struct {
	opt CopyObjectsOpt

	copier copier
}

func NewCopyObjectsTask(opt CopyObjectsOpt) *CopyObjectsTask {
	return &CopyObjectsTask{
		opt: opt,

		copier: newCopier(opt.Src, opt.Dest, opt.Streaming, copierOpt{traceFn: opt.TraceFn}),
	}
}

func (c *CopyObjectsTask) Execute(ctx context.Context) error {
	// shuffle to avoid hot region
	mutable.Shuffle(c.opt.Attrs)

	// Derive a cancellable context so in-flight copies can be stopped when the
	// loop bails early, then join them through the single Wait below.
	// defer cancel() satisfies vet's lostcancel check; it is a no-op on the
	// happy path where Wait has already joined every copy.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, subCtx := errgroup.WithContext(ctx)

	var loopErr error
	for _, attr := range c.opt.Attrs {
		if err := c.opt.Sem.Acquire(ctx, 1); err != nil {
			loopErr = fmt.Errorf("storage: copy objects acquire semaphore %w", err)
			break
		}
		g.Go(func() error {
			defer c.opt.Sem.Release(1)

			if err := c.copier.copy(subCtx, attr); err != nil {
				return fmt.Errorf("storage: copy objects %w", err)
			}

			return nil
		})
	}

	// If the loop bailed early, stop the in-flight copies so Wait below
	// returns promptly instead of running them to completion.
	if loopErr != nil {
		cancel()
	}

	waitErr := g.Wait()
	if loopErr != nil {
		return loopErr
	}
	if waitErr != nil {
		return fmt.Errorf("storage: copy objects %w", waitErr)
	}

	return nil
}
