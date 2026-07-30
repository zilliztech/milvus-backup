package backup

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

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
	StrategySnapshot
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

type collTaskArgs struct {
	TaskID string

	MilvusStorage  storage.Client
	MilvusRootPath string
	Streaming      bool
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

func newDDLTasks(nss []namespace.NS, args collTaskArgs) []collTask {
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

func newDMLTasks(nss []namespace.NS, args collTaskArgs) []collTask {
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

// snapshotStrategy backs up through Milvus snapshots: milvus-backup orchestrates and
// Milvus moves the bytes. It needs a 3.0 server, and produces backups in a format only
// a milvus-backup that understands it can restore.
type snapshotStrategy struct {
	nss []namespace.NS

	target     snapshotTarget
	backupName string

	args collTaskArgs

	logger *zap.Logger
}

func newSnapshotStrategy(nss []namespace.NS, backupName string, target snapshotTarget, args collTaskArgs) *snapshotStrategy {
	return &snapshotStrategy{
		nss:        nss,
		target:     target,
		backupName: backupName,
		args:       args,
		logger:     log.With(zap.String("task_id", args.TaskID)),
	}
}

// snapshotName is what the collection is frozen under while it is exported. Backup
// names allow a leading digit and snapshot names do not, hence the prefix; the backup
// name itself is already restricted to letters, digits and underscores.
func (ss *snapshotStrategy) snapshotName() string { return "mbk_" + ss.backupName }

func (ss *snapshotStrategy) Execute(ctx context.Context) error {
	ddlTasks := newDDLTasks(ss.nss, ss.args)
	if err := concurrentExecCollTask(ctx, ss.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: execute ddl task %w", err)
	}

	if err := ss.flushAll(ctx); err != nil {
		return fmt.Errorf("backup: flush all %w", err)
	}

	// Each collection creates its snapshot immediately before submitting the export
	// that pins it, so there is no window where a snapshot sits unpinned waiting its
	// turn. The collection semaphore bounds how many exports are in flight, which
	// matters because a job starts its timeout when DataCoord accepts it, not when it
	// starts copying — anything queued beyond what
	// dataCoord.snapshot.exportMaxConcurrentJobs can run burns that budget waiting.
	snapshotTasks := make([]collTask, 0, len(ss.nss))
	for _, ns := range ss.nss {
		task := func(ctx context.Context) error {
			if err := newCollSnapshotTask(ns, ss.snapshotName(), ss.target, ss.args).Execute(ctx); err != nil {
				ss.args.TaskMgr.UpdateBackupTask(ss.args.TaskID, taskmgr.SetBackupCollFail(ns, err))
				return fmt.Errorf("backup: execute snapshot task %w", err)
			}

			ss.args.TaskMgr.UpdateBackupTask(ss.args.TaskID, taskmgr.SetBackupCollSuccess(ns))
			return nil
		}
		snapshotTasks = append(snapshotTasks, task)
	}

	if err := concurrentExecCollTask(ctx, ss.args.Throttling.CollSem, snapshotTasks); err != nil {
		return fmt.Errorf("backup: concurrent execute snapshot task %w", err)
	}

	return nil
}

// flushAll seals what is still growing. A snapshot admits only the segments below its
// channel seek positions, so without this it would hold whatever the last automatic
// flush left behind rather than the collections as they are now.
//
// One call covers every collection. A collection whose snapshot is taken well after this
// is still bounded by its own channel checkpoint at creation time, so writes since the
// flush are in it only as far as automatic flushing has carried them. How far behind the
// last collection runs is up to dataCoord.snapshot.exportMaxConcurrentJobs — refreshable,
// with no upper bound, and 1 until an operator raises it.
//
// FlushAll needs no feature check here: it arrived in 2.6.11 and this strategy already
// requires 3.0.
func (ss *snapshotStrategy) flushAll(ctx context.Context) error {
	return flushAllAndRecord(ctx, ss.args, ss.logger)
}

type metaOnlyStrategy struct {
	nss []namespace.NS

	args collTaskArgs
}

func newMetaOnlyStrategy(nss []namespace.NS, args collTaskArgs) *metaOnlyStrategy {
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

	args collTaskArgs

	logger *zap.Logger
}

func newSkipFlushStrategy(nss []namespace.NS, args collTaskArgs) *skipFlushStrategy {
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

	return nil
}

type serialFlushStrategy struct {
	nss []namespace.NS

	args collTaskArgs

	logger *zap.Logger
}

func newSerialFlushStrategy(nss []namespace.NS, args collTaskArgs) *serialFlushStrategy {
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

	if err := sf.args.MetaBuilder.addPOS(ns, channelCP, maxChannelTS, uint64(resp.GetCollSealTimes()[ns.CollName()])); err != nil {
		return fmt.Errorf("backup: add POS meta: %w", err)
	}
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

	args collTaskArgs

	logger *zap.Logger
}

func newBulkFlushStrategy(nss []namespace.NS, args collTaskArgs) *bulkFlushStrategy {
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
	return flushAllAndRecord(ctx, bf.args, bf.logger)
}

// flushAllAndRecord seals every collection in the cluster and records what the response
// says about it: the control and physical channels, and the flush messages per channel.
func flushAllAndRecord(ctx context.Context, args collTaskArgs, logger *zap.Logger) error {
	logger.Info("start flush all")

	start := time.Now()
	resp, err := args.Grpc.FlushAll(ctx)
	if err != nil {
		return fmt.Errorf("backup: flush all %w", err)
	}
	logger.Info("flush all done", zap.Any("resp", resp), zap.Duration("cost", time.Since(start)))

	pchs := resp.GetClusterInfo().GetPchannels()
	cch := resp.GetClusterInfo().GetCchannel()

	flushAllMsg := make(map[string]string, len(resp.GetFlushAllMsgs()))
	for pch, msg := range resp.GetFlushAllMsgs() {
		byts, err := proto.Marshal(msg)
		if err != nil {
			return fmt.Errorf("backup: marshal flush all msg %w", err)
		}

		flushAllMsg[pch] = base64.StdEncoding.EncodeToString(byts)
	}

	args.MetaBuilder.setClusterInfoAndTSS(cch, pchs, flushAllMsg)
	return nil
}
