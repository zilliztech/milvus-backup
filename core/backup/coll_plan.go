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

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// Strategy decides when the backup's point-in-time boundary is established: when,
// if at all, data is flushed before it is captured. What carries the captured
// data is the format, a separate axis the task resolves at start.
//
//go:generate stringer -type=Strategy
type Strategy int

const (
	StrategyAuto Strategy = iota
	StrategyMetaOnly
	StrategySkipFlush
	StrategyBulkFlush
	StrategySerialFlush
)

// Format is what artifact carries the backup's data: binlog files copied by
// milvus-backup, or a bundle Milvus exports from a snapshot. Both default to auto
// and resolve against the server at task start.
//
//go:generate stringer -type=Format
type Format int

const (
	FormatAuto Format = iota
	FormatBinlog
	FormatSnapshot
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

// dataTaskFactory builds the per-collection data task for the resolved format.
// The plans call it instead of constructing a DML task themselves, so one flush
// orchestration runs whichever artifact the format calls for.
type dataTaskFactory func(collRef collref.Name, args collTaskArgs) tasklet.Tasklet

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

func newDDLTasks(collRefs []collref.Name, args collTaskArgs) []collTask {
	ddlTasks := make([]collTask, 0, len(collRefs))
	for _, collRef := range collRefs {
		task := func(ctx context.Context) error {
			if err := newCollDDLTask(collRef, args).Execute(ctx); err != nil {
				args.TaskMgr.UpdateBackupTask(args.TaskID, taskmgr.SetBackupCollFail(collRef, err))
				return fmt.Errorf("backup: execute ddl task %w", err)
			}

			return nil
		}
		ddlTasks = append(ddlTasks, task)
	}

	return ddlTasks
}

func newDMLTasks(collRefs []collref.Name, args collTaskArgs, newData dataTaskFactory) []collTask {
	dmlTasks := make([]collTask, 0, len(collRefs))
	for _, collRef := range collRefs {
		task := func(ctx context.Context) error {
			if err := newData(collRef, args).Execute(ctx); err != nil {
				args.TaskMgr.UpdateBackupTask(args.TaskID, taskmgr.SetBackupCollFail(collRef, err))
				return fmt.Errorf("backup: execute data task %w", err)
			}

			args.TaskMgr.UpdateBackupTask(args.TaskID, taskmgr.SetBackupCollSuccess(collRef))

			return nil
		}
		dmlTasks = append(dmlTasks, task)
	}

	return dmlTasks
}

type metaOnlyPlan struct {
	collRefs []collref.Name

	args collTaskArgs
}

func newMetaOnlyPlan(collRefs []collref.Name, args collTaskArgs) *metaOnlyPlan {
	return &metaOnlyPlan{collRefs: collRefs, args: args}
}

func (m *metaOnlyPlan) Execute(ctx context.Context) error {
	ddlTasks := newDDLTasks(m.collRefs, m.args)
	if err := concurrentExecCollTask(ctx, m.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: concurrent execute ddl task %w", err)
	}

	return nil
}

type skipFlushPlan struct {
	collRefs []collref.Name

	args collTaskArgs

	newData dataTaskFactory

	logger *zap.Logger
}

func newSkipFlushPlan(collRefs []collref.Name, args collTaskArgs, newData dataTaskFactory) *skipFlushPlan {
	return &skipFlushPlan{
		collRefs: collRefs,
		args:     args,
		newData:  newData,
		logger:   log.With(zap.String("task_id", args.TaskID)),
	}
}

func (sf *skipFlushPlan) Execute(ctx context.Context) error {
	sf.logger.Info("use skip flush plan")

	// backup DDL
	ddlTasks := newDDLTasks(sf.collRefs, sf.args)
	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: execute ddl task %w", err)
	}

	// backup DML
	dmlTasks := newDMLTasks(sf.collRefs, sf.args, sf.newData)
	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, dmlTasks); err != nil {
		return fmt.Errorf("backup: execute dml task %w", err)
	}

	return nil
}

type serialFlushPlan struct {
	collRefs []collref.Name

	args collTaskArgs

	newData dataTaskFactory

	logger *zap.Logger
}

func newSerialFlushPlan(collRefs []collref.Name, args collTaskArgs, newData dataTaskFactory) *serialFlushPlan {
	return &serialFlushPlan{
		collRefs: collRefs,
		args:     args,
		newData:  newData,
		logger:   log.With(zap.String("task_id", args.TaskID)),
	}
}

func (sf *serialFlushPlan) flushAndBackupPOS(ctx context.Context, collRef collref.Name) error {
	sf.logger.Info("start flush collection")
	start := time.Now()
	resp, err := sf.args.Grpc.Flush(ctx, collRef.DBName(), collRef.CollName())
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

	if err := sf.args.MetaBuilder.addPOS(collRef, channelCP, maxChannelTS, uint64(resp.GetCollSealTimes()[collRef.CollName()])); err != nil {
		return fmt.Errorf("backup: add POS meta: %w", err)
	}
	return nil
}

func (sf *serialFlushPlan) executeDDLTask(ctx context.Context) error {
	ddlTasks := newDDLTasks(sf.collRefs, sf.args)

	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: concurrent execute ddl task %w", err)
	}

	return nil
}

func (sf *serialFlushPlan) executeDMLTask(ctx context.Context) error {
	dmlTasks := make([]collTask, 0, len(sf.collRefs))
	for _, collRef := range sf.collRefs {
		task := func(ctx context.Context) error {
			if err := sf.flushAndBackupPOS(ctx, collRef); err != nil {
				sf.args.TaskMgr.UpdateBackupTask(sf.args.TaskID, taskmgr.SetBackupCollFail(collRef, err))
				return fmt.Errorf("backup: flush and backup pos %w", err)
			}

			if err := sf.newData(collRef, sf.args).Execute(ctx); err != nil {
				sf.args.TaskMgr.UpdateBackupTask(sf.args.TaskID, taskmgr.SetBackupCollFail(collRef, err))
				return fmt.Errorf("backup: execute data task %w", err)
			}

			sf.args.TaskMgr.UpdateBackupTask(sf.args.TaskID, taskmgr.SetBackupCollSuccess(collRef))
			return nil
		}

		dmlTasks = append(dmlTasks, task)
	}

	if err := concurrentExecCollTask(ctx, sf.args.Throttling.CollSem, dmlTasks); err != nil {
		return fmt.Errorf("backup: concurrent execute dml task %w", err)
	}

	return nil
}

func (sf *serialFlushPlan) Execute(ctx context.Context) error {
	if err := sf.executeDDLTask(ctx); err != nil {
		return fmt.Errorf("backup: execute ddl task %w", err)
	}

	if err := sf.executeDMLTask(ctx); err != nil {
		return fmt.Errorf("backup: execute dml task %w", err)
	}

	return nil
}

type bulkFlushPlan struct {
	collRefs []collref.Name

	args collTaskArgs

	newData dataTaskFactory

	logger *zap.Logger
}

func newBulkFlushPlan(collRefs []collref.Name, args collTaskArgs, newData dataTaskFactory) *bulkFlushPlan {
	return &bulkFlushPlan{collRefs: collRefs, args: args, newData: newData, logger: log.With(zap.String("task_id", args.TaskID))}
}

func (bf *bulkFlushPlan) Execute(ctx context.Context) error {
	ddlTasks := newDDLTasks(bf.collRefs, bf.args)
	if err := concurrentExecCollTask(ctx, bf.args.Throttling.CollSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: execute ddl task %w", err)
	}

	if err := bf.flushAllAndBackupTS(ctx); err != nil {
		return fmt.Errorf("backup: flush all and backup ts %w", err)
	}

	dmlTasks := newDMLTasks(bf.collRefs, bf.args, bf.newData)
	if err := concurrentExecCollTask(ctx, bf.args.Throttling.CollSem, dmlTasks); err != nil {
		return fmt.Errorf("backup: execute dml task %w", err)
	}

	return nil
}

func (bf *bulkFlushPlan) flushAllAndBackupTS(ctx context.Context) error {
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
