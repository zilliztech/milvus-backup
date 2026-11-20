package secondary

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func (t *Task) savePlan(ctx context.Context) error {
	build, err := t.newCollDMLTaskFactory()
	if err != nil {
		return err
	}
	wal, err := sourceWALName(t.args.Backup)
	if err != nil {
		return err
	}
	var bounds map[string]uint64
	if t.tracker.Resumable() {
		bounds, err = t.pchTS()
		if err != nil {
			return err
		}
		// A target checkpoint reports its source pchannel. Require a bijection so
		// the exact source-channel pending queue can be validated on Resume.
		sourceChannels := make(map[string]bool, len(t.targetChannels))
		for _, ch := range t.targetChannels {
			sourceChannels[ch] = true
		}
		if len(sourceChannels) != len(t.targetChannels) || len(bounds) != len(t.targetChannels) {
			return fmt.Errorf("secondary: incomplete source/target channel mapping")
		}
		for _, ch := range t.args.Backup.GetPhysicalChannelNames() {
			if ch == "" || !sourceChannels[ch] || bounds[ch] == 0 {
				return fmt.Errorf("secondary: missing source channel mapping or flush boundary for %s", ch)
			}
			delete(sourceChannels, ch)
		}
		if len(sourceChannels) != 0 {
			return fmt.Errorf("secondary: source channels in backup do not match target configuration")
		}
	}
	s := &restoreplan.Secondary{
		SourceWAL:      wal,
		SourceID:       t.args.SourceClusterID,
		TargetID:       t.args.TargetClusterID,
		Backup:         t.args.Backup,
		Layouts:        t.snapLayouts,
		TargetChannels: t.targetChannels,
		Collections:    make(map[int64]*restoreplan.SecondaryCollection),
	}
	names := make(map[collref.Name]bool)
	for _, coll := range t.args.Backup.GetCollectionBackups() {
		name := collref.New(coll.GetDbName(), coll.GetCollectionName())
		if names[name] {
			return fmt.Errorf("secondary: duplicate target collection %s", name)
		}
		names[name] = true
		if _, exists := s.Collections[coll.GetCollectionId()]; exists {
			return fmt.Errorf("secondary: duplicate collection ID %d", coll.GetCollectionId())
		}
		task, err := build(coll)
		if err != nil {
			return err
		}
		saved := &restoreplan.SecondaryCollection{}
		if bin, ok := task.(*collBinlogDMLTask); ok {
			saved, err = bin.plan(ctx)
			if err != nil {
				return err
			}
		}
		s.Collections[coll.GetCollectionId()] = saved
		tr := t.tracker.NewCollTracker(name, coll.GetSize())
		tr.DDL()
		tr.NewJob(coll.GetSize())
	}
	t.plan = s
	t.progress = jobstate.NewSecondaryProgress(s)
	if t.tracker.Resumable() {
		t.progress.StreamCheckpoint = milvus.NewStreamCheckpoint(bounds)
		c := t.args.Params
		t.tracker.SaveSecondaryPlan(&restoreplan.Plan{
			Milvus: c.Milvus, Backup: c.Backup, Restore: c.Restore, Transfer: c.Transfer,
			BackupDir: t.args.BackupDir, SnapshotSource: t.snapSource, Secondary: s,
		}, t.progress)
	}
	return nil
}

// Resume runs after the store has admitted exactly one owner.
func Resume(ctx context.Context, id string, tracker *jobstate.RestoreTracker, p *restoreplan.Plan) {
	config := &cfg.Config{
		Milvus:   p.Milvus,
		Backup:   p.Backup,
		Restore:  p.Restore,
		Transfer: p.Transfer,
	}
	s := p.Secondary
	t := &Task{
		args: TaskArgs{
			TaskID:          id,
			Resumable:       true,
			SourceClusterID: s.SourceID,
			TargetClusterID: s.TargetID,
			Backup:          s.Backup,
			Params:          config,
			BackupDir:       p.BackupDir,
		},
		tracker:     tracker,
		plan:        s,
		progress:    tracker.SecondaryProgress(),
		snapLayouts: s.Layouts,
		snapSource:  p.SnapshotSource,
		logger:      log.With(zap.String("task_id", id)),
	}
	var err error
	if s.Backup.GetFormat() == meta.FormatBinlog {
		t.args.BackupStorage, err = storage.NewClient(ctx, storage.BackupStorageConfig(config))
		if err == nil {
			t.args.MilvusStorage, err = storage.NewMilvusStorage(ctx, config)
		}
	}
	if err == nil {
		err = t.run(ctx)
	}
	tracker.Finish(err)
}

func CleanupFailed(ctx context.Context, tracker *jobstate.RestoreTracker, p *restoreplan.Plan) error {
	var dest storage.Client
	var failures []error
	for _, coll := range tracker.SecondaryProgress().Collections {
		if coll.StagingDir == "" {
			continue
		}
		if dest == nil {
			var err error
			dest, err = storage.NewMilvusStorage(ctx, &cfg.Config{Milvus: p.Milvus, Transfer: p.Transfer})
			if err != nil {
				return err
			}
		}
		task := &collBinlogDMLTask{progress: coll, milvusStorage: dest, milvusRootPath: p.Milvus.Storage.RootPath.Val}
		if err := task.cleanTempDirs(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
