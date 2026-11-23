package restore

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/secondary"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// savedPlan records the resolved strategy and collection configuration. Resume
// does not enumerate storage, run the strategy selector, or mint new job slots.
func (t *Task) savedPlan(plan tasklet.Tasklet) *restoreplan.Plan {
	config := t.args.Params
	opt := *t.args.Option
	opt.Secondaries = slices.Clone(opt.Secondaries)
	opt.SkipParams.CollectionProperties = slices.Clone(opt.SkipParams.CollectionProperties)
	opt.SkipParams.FieldTypeParams = slices.Clone(opt.SkipParams.FieldTypeParams)
	opt.SkipParams.FieldIndexParams = slices.Clone(opt.SkipParams.FieldIndexParams)
	opt.SkipParams.IndexParams = slices.Clone(opt.SkipParams.IndexParams)
	// Each collection already carries its resolved key.
	opt.EZKMapping = nil
	p := &restoreplan.Plan{
		Milvus:         config.Milvus,
		Backup:         config.Backup,
		Restore:        config.Restore,
		Transfer:       config.Transfer,
		BackupDir:      t.args.BackupDir,
		Option:         opt,
		SnapshotSource: t.snapshotSource,
		Streaming:      t.streaming,
	}
	switch plan := plan.(type) {
	case *restoreSnapPlan:
		p.Strategy = StrategyRestoreSnap
	case *importSnapPlan:
		p.Strategy = StrategyImportSnap
	case *binlogPlan:
		p.Strategy = plan.strategy
	}
	for _, target := range t.collTargets {
		coll := restoreplan.Collection{
			Target:   target.target,
			Metadata: collectionMetadata(target.collBackup),
			HasData:  backupHasData(target.collBackup),
			EZK:      t.mappedEZK(target.dbBackup),
			Override: target.collOverride,
		}
		p.Collections = append(p.Collections, coll)
	}
	return p
}

// Import batches already contain the resolved binlog paths; DDL and snapshot
// postprocessing only need the collection metadata and partition descriptions.
func collectionMetadata(source *backuppb.CollectionBackupInfo) *backuppb.CollectionBackupInfo {
	if source == nil {
		return nil
	}
	metadata := *source
	metadata.L0Segments = nil
	metadata.PartitionBackups = make([]*backuppb.PartitionBackupInfo, len(source.PartitionBackups))
	for i, part := range source.PartitionBackups {
		if part != nil {
			desc := *part
			desc.SegmentBackups = nil
			metadata.PartitionBackups[i] = &desc
		}
	}
	return protoadapt.MessageV1Of(proto.Clone(protoadapt.MessageV2Of(&metadata))).(*backuppb.CollectionBackupInfo)
}

// Resume starts a fresh executor for the stored job and returns after admission.
func Resume(ctx context.Context, store *jobstate.Store, id string) error {
	tracker, p, err := store.StartResume(id)
	if err != nil {
		return err
	}
	go func() {
		if p.Secondary != nil {
			secondary.Resume(ctx, id, tracker, p)
			return
		}

		config := &cfg.Config{
			Milvus:   p.Milvus,
			Backup:   p.Backup,
			Transfer: p.Transfer,
			Restore:  p.Restore,
		}
		task := &Task{
			args: TaskArgs{
				TaskID:    id,
				Params:    config,
				BackupDir: p.BackupDir,
				Option:    &p.Option,
			},
			tracker:        tracker,
			snapshotSource: p.SnapshotSource,
			streaming:      p.Streaming,
			logger:         log.With(zap.String("task_id", id)),
			copySem:        semaphore.NewWeighted(int64(config.Transfer.Concurrency.Val)),
			bulkInsertSem:  semaphore.NewWeighted(int64(config.Restore.Concurrency.ImportJobs.Val)),
		}
		_, _ = task.run(ctx, p)
	}()
	return nil
}

func (t *Task) rebuild(ctx context.Context, p *restoreplan.Plan) error {
	limit := p.Restore.Concurrency.Collections.Val
	if p.Option.MetaOnly {
		plan := &metaOnlyPlan{limit: limit, logger: t.logger}
		for _, coll := range p.Collections {
			tracker := t.tracker.TrackerForCollection(coll.Target)
			ddl := newCollDDLTask(t.args.TaskID, &p.Option, coll.Override, coll.Metadata, coll.Target, t.grpc, tracker.DDL())
			plan.colls = append(plan.colls, ddl)
		}
		t.plan = plan
		return nil
	}
	binlog := &binlogPlan{limit: limit, logger: t.logger}
	snap := &restoreSnapPlan{limit: limit, logger: t.logger}
	imp := &importSnapPlan{limit: limit, logger: t.logger}
	switch p.Strategy {
	case StrategyPerSeg, StrategyMergedSeg:
		var err error
		t.args.BackupStorage, err = storage.NewClient(ctx, storage.BackupStorageConfig(t.args.Params))
		if err != nil {
			return err
		}
		t.args.MilvusStorage, err = storage.NewMilvusStorage(ctx, t.args.Params)
		if err != nil {
			return err
		}
		t.plan = binlog
	case StrategyRestoreSnap:
		t.plan = snap
	case StrategyImportSnap:
		t.plan = imp
	default:
		return fmt.Errorf("restore: unsupported saved strategy %v", p.Strategy)
	}
	for _, saved := range p.Collections {
		target := collTarget{target: saved.Target, collBackup: saved.Metadata, dbBackup: &backuppb.DatabaseBackupInfo{Ezk: saved.EZK}, collOverride: saved.Override}
		collTracker := t.tracker.TrackerForCollection(saved.Target)
		switch p.Strategy {
		case StrategyRestoreSnap:
			snap.colls = append(snap.colls, t.newRestoreSnapCollTask(target, collTracker.TrackerForSingleJob()))
		case StrategyImportSnap:
			node, err := t.newImportSnapCollTask(target, collTracker, collTracker.TrackerForSingleJob())
			if err != nil {
				return err
			}
			node.ddl.savedHasData = saved.HasData
			imp.colls = append(imp.colls, node)
		default:
			ddl := newCollDDLTask(t.args.TaskID, t.args.Option, target.collOverride, target.collBackup, target.target, t.grpc, collTracker.DDL())
			ddl.savedHasData = saved.HasData
			node := t.bindBinlog(target, ddl, collTracker, p.Strategy)
			binlog.colls = append(binlog.colls, node)
		}
	}
	return nil
}

func (t *Task) bindBinlog(target collTarget, ddl tasklet.Tasklet, tracker *jobstate.RestoreCollTracker, strategy Strategy) *collBinlogTask {
	args := t.newImportTaskArgs(target, tracker)
	node := &collBinlogTask{ddl: ddl, sem: t.bulkInsertSem, logger: args.logger}
	groups := make(map[string]int)
	for _, job := range tracker.TrackersForBinlogImports() {
		b := job.Batch
		task := args.importTask(strategy, job, t.secondaries)
		if b.PartitionName == "" {
			node.globalL0 = append(node.globalL0, task)
			continue
		}
		index, ok := groups[b.PartitionName]
		if !ok {
			index = len(node.parts)
			groups[b.PartitionName] = index
			node.parts = append(node.parts, partBinlogTasks{})
		}
		part := &node.parts[index]
		if b.L0 {
			part.l0 = append(part.l0, task)
		} else {
			part.leveled = append(part.leveled, task)
		}
	}
	return node
}

func CleanupFailed(ctx context.Context, store *jobstate.Store, id string) (err error) {
	tracker, p, err := store.StartCleanup(id)
	if err != nil || tracker == nil {
		return err
	}
	defer func() { tracker.FinishCleanup(err) }()
	if p.Secondary != nil {
		return secondary.CleanupFailed(ctx, tracker, p)
	}
	var dest storage.Client
	var failures []error
	for _, coll := range p.Collections {
		c := tracker.TrackerForCollection(coll.Target)
		for _, job := range c.TrackersForBinlogImports() {
			dir := job.Tracker.StagingDir()
			if dir == "" {
				continue
			}
			if dest == nil {
				dest, err = storage.NewMilvusStorage(ctx, &cfg.Config{Milvus: p.Milvus, Transfer: p.Transfer})
				if err != nil {
					return err
				}
			}
			if e := storage.DeletePrefix(ctx, dest, destKey(dest, p.Milvus.Storage.RootPath.Val, dir)); e != nil {
				failures = append(failures, e)
				continue
			}
			job.Tracker.SetStagingDir("")
		}
	}
	return errors.Join(failures...)
}
