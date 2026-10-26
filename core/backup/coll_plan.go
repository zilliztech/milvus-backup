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
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
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
// DMLSem / SegSem / CopySem form a three-level semaphore hierarchy (collection data -> segment -> copy).
// Always acquire strictly top-down: DMLSem -> SegSem -> CopySem.
// Always release strictly bottom-up: CopySem -> SegSem -> DMLSem.
// Do NOT skip levels; do NOT acquire an upper level while holding a lower; do NOT reorder.
// Reason: breaking the order can create circular wait (A holds lower waiting for upper; B holds upper waiting for lower) -> deadlock.
// DDLSem stands outside the hierarchy: a DDL task is a handful of RPCs that
// settles in seconds while a DML task copies the whole collection, so the two
// kinds never share a budget — and a DDL task holds no lower semaphore.
type concurrencyThrottling struct {
	DDLSem  *semaphore.Weighted
	DMLSem  *semaphore.Weighted
	SegSem  *semaphore.Weighted
	CopySem *semaphore.Weighted
}

type collTaskArgs struct {
	TaskID string

	tracker *jobstate.BackupTracker

	MilvusStorage  storage.Client
	MilvusRootPath string
	Streaming      bool
	BackupStorage  storage.Client
	BackupDir      string

	Throttling concurrencyThrottling

	MetaBuilder *metaBuilder

	Grpc    milvus.Grpc
	Restful milvus.Restful

	gcCtrl gcCtrl
}

// dataTaskFactory builds the per-collection data task for the resolved format.
// The plans call it instead of constructing a DML task themselves, so one flush
// orchestration runs whichever artifact the format calls for.
type dataTaskFactory func(collRef collref.Name, args collTaskArgs) tasklet.Tasklet

func newDDLTasks(collRefs []collref.Name, args collTaskArgs) []tasklet.Tasklet {
	ddlTasks := make([]tasklet.Tasklet, 0, len(collRefs))
	for _, collRef := range collRefs {
		ddlTasks = append(ddlTasks, newCollDDLTask(collRef, args))
	}

	return ddlTasks
}

func newDMLTasks(collRefs []collref.Name, args collTaskArgs, newData dataTaskFactory) []tasklet.Tasklet {
	dmlTasks := make([]tasklet.Tasklet, 0, len(collRefs))
	for _, collRef := range collRefs {
		dmlTasks = append(dmlTasks, newData(collRef, args))
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
	if err := tasklet.RunConcurrent(ctx, m.args.Throttling.DDLSem, ddlTasks); err != nil {
		return fmt.Errorf("backup: execute ddl tasks %w", err)
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

	// DDL and DML share no data dependency, so the two kinds race each other,
	// each bounded by its own semaphore: a collection's schema lands whenever
	// its turn comes, and the meta builder holds any data-side write that
	// beats it.
	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return tasklet.RunConcurrent(subCtx, sf.args.Throttling.DDLSem, newDDLTasks(sf.collRefs, sf.args))
	})
	g.Go(func() error {
		return tasklet.RunConcurrent(subCtx, sf.args.Throttling.DMLSem, newDMLTasks(sf.collRefs, sf.args, sf.newData))
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("backup: execute collection tasks %w", err)
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

func (sf *serialFlushPlan) Execute(ctx context.Context) error {
	dmlTasks := make([]tasklet.Tasklet, 0, len(sf.collRefs))
	for _, collRef := range sf.collRefs {
		dmlTasks = append(dmlTasks, newSerialDMLTask(collRef, sf.newData(collRef, sf.args), sf.args))
	}

	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return tasklet.RunConcurrent(subCtx, sf.args.Throttling.DDLSem, newDDLTasks(sf.collRefs, sf.args))
	})
	g.Go(func() error {
		return tasklet.RunConcurrent(subCtx, sf.args.Throttling.DMLSem, dmlTasks)
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("backup: execute collection tasks %w", err)
	}

	return nil
}

// serialDMLTask is the serial plan's per-collection data chain: flush the
// collection, record its POS, then run the data task. The flush is what the
// data task's segment listing must see, so the two stay chained; everything
// else in the job — the collection's own DDL included — races it.
type serialDMLTask struct {
	collRef collref.Name

	grpc        milvus.Grpc
	metaBuilder *metaBuilder
	tracker     *jobstate.BackupCollDMLTracker

	data tasklet.Tasklet

	logger *zap.Logger
}

func newSerialDMLTask(collRef collref.Name, data tasklet.Tasklet, args collTaskArgs) *serialDMLTask {
	return &serialDMLTask{
		collRef:     collRef,
		grpc:        args.Grpc,
		metaBuilder: args.MetaBuilder,
		tracker:     args.tracker.NewCollDMLTracker(collRef),
		data:        data,
		logger:      log.With(zap.String("task_id", args.TaskID), zap.String("coll", collRef.String())),
	}
}

func (t *serialDMLTask) Execute(ctx context.Context) error {
	// The flush is the visible long pole of this plan; the data task flips
	// the half to prepare itself once it starts.
	t.tracker.SetFlushing()

	if err := t.flushAndBackupPOS(ctx); err != nil {
		t.tracker.SetFail(err)
		return err
	}

	return t.data.Execute(ctx)
}

func (t *serialDMLTask) flushAndBackupPOS(ctx context.Context) error {
	t.logger.Info("start flush collection")
	start := time.Now()
	resp, err := t.grpc.Flush(ctx, t.collRef.DBName(), t.collRef.CollName())
	if err != nil {
		return fmt.Errorf("backup: flush collection %w", err)
	}
	t.logger.Info("flush collection done", zap.Any("resp", resp), zap.Duration("cost", time.Since(start)))

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

	t.metaBuilder.addPOS(t.collRef, channelCP, maxChannelTS, uint64(resp.GetCollSealTimes()[t.collRef.CollName()]))
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
	// The dependency graph has exactly one edge: the data tasks enumerate
	// segments from the boundary the flush seals, so they chain after it. The
	// DDL tasks read nothing the flush produces and produce nothing the data
	// tasks read — the meta builder's pendings absorb their reordering — so
	// they race both, bounded by their own semaphore. The flush itself is one
	// cluster-wide RPC that moves no bytes; it takes no semaphore.
	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return tasklet.RunConcurrent(subCtx, bf.args.Throttling.DDLSem, newDDLTasks(bf.collRefs, bf.args))
	})
	g.Go(func() error {
		flush := &flushAllTask{collRefs: bf.collRefs, args: bf.args, logger: bf.logger}
		if err := flush.Execute(subCtx); err != nil {
			bf.markUnstartedDMLFailed(err)
			return err
		}
		if err := tasklet.RunConcurrent(subCtx, bf.args.Throttling.DMLSem, newDMLTasks(bf.collRefs, bf.args, bf.newData)); err != nil {
			bf.markUnstartedDMLFailed(err)
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("backup: execute collection tasks %w", err)
	}

	return nil
}

// markUnstartedDMLFailed settles every data half still in flushing as failed. The
// flush flips all halves before its RPC runs, so a failed data branch can leave
// halves with no owner left to settle them: the flush task has returned, and a
// data task that never launched never ran. A half that got past flushing was
// settled by its own data task before its goroutine joined, so only flushing
// ones remain, and only those are touched.
func (bf *bulkFlushPlan) markUnstartedDMLFailed(err error) {
	for collRef, coll := range bf.args.tracker.Snapshot().Colls {
		if coll.DML == nil || coll.DML.State != jobstate.BackupCollDMLStateFlushing {
			continue
		}

		bf.args.tracker.NewCollDMLTracker(collRef).SetFail(err)
	}
}

// flushAllTask is the bulk plan's cluster-wide flush, the head of the data
// branch: it marks every collection flushing, then seals the whole cluster in
// one call. The data tasks chained after it mint their handles on these same
// halves and flip them past flushing as they start.
type flushAllTask struct {
	collRefs []collref.Name

	args   collTaskArgs
	logger *zap.Logger
}

func (t *flushAllTask) Execute(ctx context.Context) error {
	for _, collRef := range t.collRefs {
		t.args.tracker.NewCollDMLTracker(collRef).SetFlushing()
	}

	return flushAllAndRecord(ctx, t.args, t.logger)
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
