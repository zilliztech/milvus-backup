package backup

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

//go:generate stringer -type=Strategy
type Strategy int

const (
	StrategyAuto Strategy = iota
	StrategyMetaOnly
	StrategySkipFlush
	StrategyBulkFlush
	StrategySerialFlush
)

// concurrencyThrottling:
// CollSem / SegSem / CopySem form a three-level semaphore hierarchy (collection -> segment -> copy).
// Always acquire strictly top-down: CollSem -> SegSem -> CopySem.
// Always release strictly bottom-up: CopySem -> SegSem -> CollSem.
// Do NOT skip levels; do NOT acquire an upper level while holding a lower; do NOT reorder.
// Reason: breaking the order can create circular wait (A holds lower waiting for upper; B holds upper waiting for lower) -> deadlock.
type concurrencyThrottling struct {
	CollSem *semaphore.Weighted
	SegSem  *semaphore.Weighted
	CopySem *semaphore.Weighted
}

type collectionTaskArgs struct {
	TaskID string

	MilvusStorage  storage.Client
	MilvusRootPath string
	CrossStorage   bool
	BackupStorage  storage.Client
	BackupDir      string

	Throttling concurrencyThrottling

	MetaBuilder *metaBuilder

	TaskMgr *taskmgr.Mgr

	Grpc    milvus.Grpc
	Restful milvus.Restful

	gcCtrl gcCtrl
}

type collTask func(ctx context.Context) error

func concurrentExecCollTask(ctx context.Context, collSem *semaphore.Weighted, tasks []collTask) error {
	g, subCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		if err := collSem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("backup: acquire collection semaphore %w", err)
		}

		g.Go(func() error {
			defer collSem.Release(1)

			if err := task(subCtx); err != nil {
				return fmt.Errorf("backup: execute task %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("backup: wait task %w", err)
	}

	return nil
}

func newDDLTasks(nss []namespace.NS, args collectionTaskArgs) []collTask {
	ddlTasks := make([]collTask, 0, len(nss))
	for _, ns := range nss {
		task := func(ctx context.Context) error {
			if err := newCollDDLTask(ns, args).Execute(ctx); err != nil {
				args.TaskMgr.UpdateBackupTask(args.TaskID, taskmgr.SetBackupCollFail(ns, err))
				return fmt.Errorf("backup: execute ddl task %w", err)
			}

			return nil
		}
		ddlTasks = append(ddlTasks, task)
	}

	return ddlTasks
}

func newDMLTasks(nss []namespace.NS, args collectionTaskArgs) []collTask {
	dmlTasks := make([]collTask, 0, len(nss))
	for _, ns := range nss {
		task := func(ctx context.Context) error {
			if err := newCollDMLTask(ns, args).Execute(ctx); err != nil {
				args.TaskMgr.UpdateBackupTask(args.TaskID, taskmgr.SetBackupCollFail(ns, err))
				return fmt.Errorf("backup: execute dml task %w", err)
			}

			args.TaskMgr.UpdateBackupTask(args.TaskID, taskmgr.SetBackupCollSuccess(ns))

			return nil
		}
		dmlTasks = append(dmlTasks, task)
	}

	return dmlTasks
}

type metaOnlyStrategy struct {
	nss []namespace.NS

	args collectionTaskArgs
}

func newMetaOnlyStrategy(nss []namespace.NS, args collectionTaskArgs) *metaOnlyStrategy {
	return &metaOnlyStrategy{nss: nss, args: args}
}

func (m *metaOnlyStrategy) Execute(ctx context.Context) error {
	ddlTasks := newDDLTasks(m.nss, m.args)
	if err := concurrentExecCollTask(ctx, m.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: concurrent execute ddl task %w", err)
	}

	return nil
}

type skipFlushStrategy struct {
	nss []namespace.NS

	args collectionTaskArgs

	logger *zap.Logger
}

func newSkipFlushStrategy(nss []namespace.NS, args collectionTaskArgs) *skipFlushStrategy {
	return &skipFlushStrategy{
		nss:    nss,
		args:   args,
		logger: log.With(zap.String("task_id", args.TaskID)),
	}
}

func (sf *skipFlushStrategy) Execute(ctx context.Context) error {
	sf.logger.Info("use skip flush strategy")

	// backup DDL
	ddlTasks := newDDLTasks(sf.nss, sf.args)
	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: execute ddl task %w", err)
	}

	// backup DML
	dmlTasks := newDMLTasks(sf.nss, sf.args)
	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, dmlTasks); err != nil {
		return fmt.Errorf("backup: execute dml task %w", err)
	}

	sf.args.TaskMgr.UpdateBackupTask(sf.args.TaskID, taskmgr.SetBackupSuccess())

	return nil
}

type serialFlushStrategy struct {
	nss []namespace.NS

	args collectionTaskArgs

	logger *zap.Logger
}

func newSerialFlushStrategy(nss []namespace.NS, args collectionTaskArgs) *serialFlushStrategy {
	return &serialFlushStrategy{
		nss:    nss,
		args:   args,
		logger: log.With(zap.String("task_id", args.TaskID)),
	}
}

func (sf *serialFlushStrategy) flushAndBackupPOS(ctx context.Context, ns namespace.NS) error {
	sf.logger.Info("start flush collection")
	start := time.Now()
	resp, err := sf.args.Grpc.Flush(ctx, ns.DBName(), ns.CollName())
	if err != nil {
		return fmt.Errorf("backup: flush collection %w", err)
	}
	sf.logger.Info("flush collection done", zap.Any("resp", resp), zap.Duration("cost", time.Since(start)))

	channelCP := make(map[string]string, len(resp.GetChannelCps()))
	var maxChannelTS uint64
	for vch, checkpoint := range resp.GetChannelCps() {
		cp, err := pbconv.Base64MsgPosition(checkpoint)
		if err != nil {
			return fmt.Errorf("backup: encode msg position %w", err)
		}
		channelCP[vch] = cp

		maxChannelTS = max(maxChannelTS, checkpoint.GetTimestamp())
	}

	sf.args.MetaBuilder.addPOS(ns, channelCP, maxChannelTS, uint64(resp.GetCollSealTimes()[ns.CollName()]))
	return nil
}

func (sf *serialFlushStrategy) executeDDLTask(ctx context.Context) error {
	ddlTasks := newDDLTasks(sf.nss, sf.args)

	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: concurrent execute ddl task %w", err)
	}

	return nil
}

func (sf *serialFlushStrategy) executeDMLTask(ctx context.Context) error {
	dmlTasks := make([]collTask, 0, len(sf.nss))
	for _, ns := range sf.nss {
		task := func(ctx context.Context) error {
			if err := sf.flushAndBackupPOS(ctx, ns); err != nil {
				sf.args.TaskMgr.UpdateBackupTask(sf.args.TaskID, taskmgr.SetBackupCollFail(ns, err))
				return fmt.Errorf("backup: flush and backup pos %w", err)
			}

			if err := newCollDMLTask(ns, sf.args).Execute(ctx); err != nil {
				sf.args.TaskMgr.UpdateBackupTask(sf.args.TaskID, taskmgr.SetBackupCollFail(ns, err))
				return fmt.Errorf("backup: execute dml task %w", err)
			}

			sf.args.TaskMgr.UpdateBackupTask(sf.args.TaskID, taskmgr.SetBackupCollSuccess(ns))
			return nil
		}

		dmlTasks = append(dmlTasks, task)
	}

	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, dmlTasks); err != nil {
		return fmt.Errorf("backup: concurrent execute dml task %w", err)
	}

	return nil
}

func (sf *serialFlushStrategy) Execute(ctx context.Context) error {
	if err := sf.executeDDLTask(ctx); err != nil {
		return fmt.Errorf("backup: execute ddl task %w", err)
	}

	if err := sf.executeDMLTask(ctx); err != nil {
		return fmt.Errorf("backup: execute dml task %w", err)
	}

	return nil
}

type bulkFlushStrategy struct {
	nss []namespace.NS

	args collectionTaskArgs

	logger *zap.Logger
}

func newBulkFlushStrategy(nss []namespace.NS, args collectionTaskArgs) *bulkFlushStrategy {
	return &bulkFlushStrategy{nss: nss, args: args, logger: log.With(zap.String("task_id", args.TaskID))}
}

func (bf *bulkFlushStrategy) Execute(ctx context.Context) error {
	ddlTasks := newDDLTasks(bf.nss, bf.args)
	if err := concurrentExecCollTask(ctx, bf.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: execute ddl task %w", err)
	}

	if err := bf.flushAllAndBackupTS(ctx); err != nil {
		return fmt.Errorf("backup: flush all and backup ts %w", err)
	}

	dmlTasks := newDMLTasks(bf.nss, bf.args)
	if err := concurrentExecCollTask(ctx, bf.args.Throttling.CollSem, dmlTasks); err != nil {
		return fmt.Errorf("backup: execute dml task %w", err)
	}

	return nil
}

func (bf *bulkFlushStrategy) flushAllAndBackupTS(ctx context.Context) error {
	bf.logger.Info("start flush all")

	start := time.Now()
	resp, err := bf.args.Grpc.FlushAll(ctx)
	if err != nil {
		return fmt.Errorf("backup: flush all %w", err)
	}
	bf.logger.Info("flush all done", zap.Any("resp", resp), zap.Duration("cost", time.Since(start)))

	bf.args.MetaBuilder.setFlushAllTS(resp.GetFlushAllTss())

	return nil
}
